#!/usr/bin/env perl
use strict;
use FindBin;
use lib glob "$FindBin::Bin/../modules/*/lib";
use lib glob "$FindBin::Bin/../t_deps/modules/*/lib";
use lib "$FindBin::Bin/../lib";
use warnings;
use Path::Tiny;
use ServerSet::MySQLServerHandler;
use Test::More;
use Web::Host;

{
  package DumpTestServerSet;
  sub actual_or_local { $_[0]->{mode} . '_' . $_[1] }
}

for my $mode (qw(actual local)) {
  my $ss = bless {mode => $mode}, 'DumpTestServerSet';
  my $data = {$mode . '_dsn_options' => {test => {
    host => Web::Host->parse_string ('127.0.0.1'),
    port => 13306, user => 'test-user', password => 'test-password',
  }}};
  for my $enabled (undef, 0, 1) {
    my $params = defined $enabled ? {dump_single_transaction => $enabled} : {};
    my $handler = bless {params => $params}, 'ServerSet::MySQLServerHandler';
    my $args;
    my $captured = bless {}, 'DumpCommandCaptured';
    {
      no warnings 'redefine';
      local *Promised::Command::new = sub {
        $args = $_[1];
        die $captured;
      };
      eval { $handler->heartbeat ($ss, $data) };
      is $@, $captured, "$mode heartbeat constructs the dump command";
    }
    is_deeply $args, [
      'mysqldump',
      ($enabled ? '--single-transaction' : ()),
      '-A', '-h', '127.0.0.1', '-P', '13306',
      '-u', 'test-user', '--password=test-password',
    ], "$mode dump preserves all databases and credentials; snapshot mode is opt-in";
    is $handler->heartbeat_interval, 600, 'existing dump interval is unchanged';
  }
}

done_testing;
