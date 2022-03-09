package ServerSet::DefaultHandler;
use strict;
use warnings;

sub new_from_params ($$) {
  my ($class, $params) = @_;
  die "No |start|" unless defined $params->{start};
  return bless {params => $params, logs => ''}, $class;
} # new_from_params

sub get_keys ($) {
  my $self = shift;
  return $self->{params}->{keys} || {};
} # get_keys

sub start ($$;%) {
  my $self = shift;
  return $self->{params}->{start}->($self, @_); # or throw
} # start

sub onstatechange ($;$) {
  $_[0]->{onstatechange} = $_[1] if @_ > 1;
  return $_[0]->{onstatechange} ||= sub { }
}

sub logs ($) { return $_[0]->{logs} }

sub heartbeat ($) { }
sub heartbeat_interval ($) { undef }

1;

=head1 LICENSE

Copyright 2018-2022 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
