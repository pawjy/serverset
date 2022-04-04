package ServerSet::WebDriverServerHandler;
use strict;
use warnings;
our $VERSION = '2.0';
use Promised::Docker::WebDriver;
use Web::URL;

use ServerSet::DefaultHandler;
push our @ISA, qw(ServerSet::DefaultHandler);

sub new_from_params ($$) {
  my ($class, $params) = @_;
  return bless {params => $params, logs => ''}, $class;
} # new_from_params

sub start ($$;%) {
  my ($handler, $ss, %args) = @_;
  my $browser = $args{browser_type} // 'chrome';
  my $wd = Promised::Docker::WebDriver->$browser;
  return $wd->start->then (sub {
    die $args{signal}->manakai_error if $args{signal}->aborted;
    $args{signal}->manakai_onabort (sub {
      return $wd->stop;
    });
    my $url = Web::URL->parse_string ($wd->get_url_prefix);
    $ss->set_actual_url (wd => $url);

    return [{}, $wd->completed];
  })->catch (sub {
    my $e = $_[0];
    return $wd->stop->finally (sub { die $e });
  });
} # start

1;

=head1 LICENSE

Copyright 2018-2022 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
