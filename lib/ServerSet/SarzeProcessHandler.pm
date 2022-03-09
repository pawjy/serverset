package ServerSet::SarzeProcessHandler;
use strict;
use warnings;
use AbortController;
use Promise;
use Promised::Flow;
use Promised::Command;
use Web::URL;

use ServerSet::DefaultHandler;
push our @ISA, qw(ServerSet::DefaultHandler);

sub new_from_params ($$) {
  my ($class, $params) = @_;
  return bless {params => $params, logs => ''}, $class;
} # new_from_params

sub start ($$;%) {
  my ($handler, $ss, %args) = @_;
  my $data = {};
  my $params = $handler->{params};
  return Promise->resolve->then (sub {
    die $args{signal}->manakai_error if $args{signal}->aborted;
    return $params->{prepare}->($handler, $ss, \%args, $data); # or throw
  })->then (sub {
    my $prepared = $_[0];
    
    my $sarze = Promised::Command->new
        ([@{$prepared->{command}},
          $prepared->{local_url}->host->to_ascii,
          $prepared->{local_url}->port]);
    $sarze->propagate_signal (1);

    for (keys %{$prepared->{envs} or {}}) {
      $sarze->envs->{$_} = $prepared->{envs}->{$_};
    }
    $args{signal}->manakai_onabort (sub { $sarze->send_signal ('TERM') });
    return $sarze->run->then (sub {
      my $ac = AbortController->new;
      $sarze->wait->finally (sub { $ac->abort });
      return $ss->wait_for_http
          (Web::URL->parse_string ('/robots.txt', $prepared->{local_url}),
           signal => $ac->signal, name => 'wait for app');
    })->then (sub {
      die $args{signal}->manakai_error if $args{signal}->aborted;
      return [$data, $sarze->wait];
    })->catch (sub {
      my $e = $_[0];
      $sarze->send_signal ('TERM');
      return $sarze->wait->catch (sub {
        my $x = shift;
        #warn $x;
      })->finally (sub { die $e });
    });
  });
} # start

1;
