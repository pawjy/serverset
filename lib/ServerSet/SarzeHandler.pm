package ServerSet::SarzeHandler;
use strict;
use warnings;
use AbortController;
use Promise;
use Promised::Flow;
use Sarze;

use ServerSet::DefaultHandler;
push our @ISA, qw(ServerSet::DefaultHandler);

sub new_from_params ($$$) {
  my ($class, $h_name, $params) = @_;
  return bless {handler_name => $h_name, params => $params}, $class;
} # new_from_params

sub start ($$;%) {
  my ($handler, $ss, %args) = @_;
  my $data = {};

  my $params = $handler->{params};
  my $statechange = $handler->onstatechange;

  return Promise->resolve->then (sub {
    die $args{signal}->manakai_error if $args{signal}->aborted;
    return $params->{prepare}->($handler, $ss, \%args, $data); # or throw
  })->then (sub {
    my $sarze_params = $_[0];

    local %ENV = (%ENV, %{$sarze_params->{envs} or {}});
    return Sarze->start (
      %$sarze_params,
      debug => $args{debug},
    )->then (sub {
      my $server = $_[0];

      my $end = $server->completed->then (sub {
        return (($params->{cleanup} or sub {})->($handler, $ss, \%args, $data)); # or throw
      });

      my $acs = [];
      $args{signal}->manakai_onabort (sub {
        $_->abort for @$acs;
        $server->stop;
      });

      return Promise->resolve->then (sub {
        die $args{signal}->manakai_error if $args{signal}->aborted;

        my $ac = AbortController->new;
        push @$acs, $ac;
        $statechange->($handler, 'waiting');
        ($params->{wait} or sub { })->($handler, $ss, \%args, $data, $ac->signal); # or throw
      })->then (sub {
        die $args{signal}->manakai_error if $args{signal}->aborted;

        my $ac = AbortController->new;
        push @$acs, $ac;
        $statechange->($handler, 'initing');
        ($params->{init} or sub { })->($handler, $ss, \%args, $data, $ac->signal); # or throw
      })->then (sub {
        return [$data, $end];
      })->catch (sub {
        my $e = $_[0];
        $_->abort for @$acs;
        $server->stop;
        return $end->finally (sub { die $e });
      });
    });
  });
} # start

1;
