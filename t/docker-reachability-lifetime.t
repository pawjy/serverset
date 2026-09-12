#!/usr/bin/env perl
use strict;
use FindBin;
use lib glob "$FindBin::Bin/../modules/*/lib";
use lib glob "$FindBin::Bin/../t_deps/modules/*/lib";
use lib "$FindBin::Bin/../lib";
use warnings;
use IO::Socket::INET;
use Path::Tiny;
use POSIX ();
use Promised::Flow;
use ServerSet;
use Test::More;
use Web::URL;

# This inline command runs inside the handler's Linux Docker container.
# Native macOS closes this unread HTTP request with RST instead of EOF.
plan skip_all => 'Docker reachability probe requires Linux TCP close semantics'
    unless $^O eq 'linux';

my $source = path ("$FindBin::Bin/../lib/ServerSet/DockerHandler.pm")->slurp;
my ($command) = $source =~ /command => \['\/app\/perl', '-MAnyEvent::Socket', '-e', q\{([^\n]+)\}, \$port\],/;
die "Missing ServerSet reachability command\n" unless defined $command;

my $reservation = IO::Socket::INET->new (
  LocalAddr => '127.0.0.1', LocalPort => 0, Listen => 1,
) or die $!;
my $port = $reservation->sockport;
close $reservation;
my $pid = fork;
die $! unless defined $pid;
if (!$pid) {
  exec ($^X, '-MAnyEvent::Socket', '-e', $command, $port)
      or POSIX::_exit (127);
}

my $reaped = 0;
local $SIG{ALRM} = sub { die "Probe regression deadline exceeded\n" };
alarm 30;
eval {
  (promised_wait_until {
    my $socket = IO::Socket::INET->new (
      LocalAddr => '127.0.0.1', LocalPort => $port, Listen => 1,
    );
    my $busy = !defined $socket;
    close $socket if $socket;
    return $busy;
  } timeout => 5, interval => 0.01)->to_cv->recv;
  pass 'probe listens without a readiness connection consuming its lifetime';

  # Discard the first response, as when it arrives after the caller gave up
  # that attempt.  The production wait_for_http must still be able to connect.
  my $first = IO::Socket::INET->new (
    PeerAddr => '127.0.0.1', PeerPort => $port, Timeout => 2,
  ) or die $!;
  my $response = do { local $/; <$first> };
  close $first;
  like $response, qr/\AHTTP\/1\.0 200 OK\r\n\r\n\z/,
      'first response is complete but not used as readiness';

  for my $attempt (1..2) {
    my $ready = eval {
      ServerSet->wait_for_http (
        Web::URL->parse_string ("http://127.0.0.1:$port/"),
        timeout => 1,
      )->to_cv->recv;
      1;
    };
    ok $ready, "existing HTTP readiness check accepts a later connection ($attempt)";
  }
  my $finished = waitpid ($pid, POSIX::WNOHANG ());
  is $finished, 0,
      'probe remains running until owner cleanup';
  $reaped = 1 if $finished != 0;
  unless ($reaped) {
    kill 'TERM', $pid;
    waitpid $pid, 0;
    $reaped = 1;
    is $?, 0, 'owner TERM is handled as a normal probe shutdown';
  }
  my $again = IO::Socket::INET->new (
    LocalAddr => '127.0.0.1', LocalPort => $port, Listen => 1, ReuseAddr => 1,
  );
  ok $again, 'owner cleanup releases the listening port';
  close $again if $again;
  1;
} or do {
  my $error = $@;
  kill 'TERM', $pid unless $reaped;
  waitpid $pid, 0 unless $reaped;
  die $error;
};
alarm 0;
done_testing;
