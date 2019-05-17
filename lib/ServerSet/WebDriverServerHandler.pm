package ServerSet::WebDriverServerHandler;
use strict;
use warnings;
use Promised::Docker::WebDriver;

use ServerSet::DefaultHandler;
push our @ISA, qw(ServerSet::DefaultHandler);

sub new_from_params ($$) {
  my ($class, $params) = @_;
  return bless {params => $params, logs => ''}, $class;
} # new_from_params

sub start ($$;%) {
  my ($handler, $self, %args) = @_;
  my $browser = $args{browser_type} // 'chrome';
  my $wd = Promised::Docker::WebDriver->$browser;
  return $wd->start (
    host => $self->local_url ('wd')->host,
    port => $self->local_url ('wd')->port,
  )->then (sub {
    die $args{signal}->manakai_error if $args{signal}->aborted;
    $args{signal}->manakai_onabort (sub {
      return $wd->stop;
    });
    return [{}, $wd->completed];
  })->catch (sub {
    my $e = $_[0];
    return $wd->stop->finally (sub { die $e });
  });
} # start

1;
