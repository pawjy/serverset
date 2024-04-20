package ServerSet::Migration;
use strict;
use warnings;
use Digest::SHA qw(sha1_hex);
use Time::HiRes qw(time);
use Promise;
use Promised::Flow;
use Dongry::Database;
use Dongry::Type;

sub run ($$$;%) {
  my ($class, $sqls, $dsn, %args) = @_;
  my $db = Dongry::Database->new (
    sources => {
      master => {dsn => Dongry::Type->serialize ('text', $dsn),
                 writable => 1, anyevent => 1},
    },
  );
  return Promise->resolve->then (sub {
    $sqls =~ s/--.*$//gm;
    my @sql = grep { length } map { s/^\s+//; s/\s+$//; $_ } split /;/, $sqls;
    return unless @sql;

    my $done = {};
    return $db->execute (q{
      create table if not exists `__migration` (
        `sql_sha` binary(40) not null,
        `sql` mediumblob not null,
        `timestamp` double not null,
        primary key (`sql_sha`),
        key (`timestamp`)
      ) default charset=binary engine=innodb
    }, undef, source_name => 'master')->then (sub {
      return $db->select ('__migration', {
        sql_sha => {-in => [map { sha1_hex $_ } @sql]},
      }, fields => ['sql'], source_name => 'master');
    })->then (sub {
      unless ($ENV{SS_MIGRATION_ALL}) {
        $done->{$_->{sql}} = 1 for @{$_[0]->all};
      }
      return promised_for {
        my $sql = shift;
        return $db->transaction->then (sub {
          my $tr = $_[0];
          return Promise->resolve->then (sub {
            return $tr->execute ($sql, undef, source_name => 'master');
          })->then (sub {
            return $tr->insert ('__migration', [{
              sql => $sql,
              sql_sha => sha1_hex ($sql),
              timestamp => time,
            }], source_name => 'master', duplicate => 'replace')->then (sub {
              return $tr->commit;
            ## Note that `create` statements are not rollbacked by failure.
            });
          }, sub {
            my $e = $_[0];
            if ($ENV{SS_MIGRATION_ALL}) {
              warn $e;
              return;
            }
            die $e;
          });
        });
      } [grep { not $done->{$_} } @sql];
    });
  })->then (sub {
    return undef unless $args{dump};
    return $db->execute ('show tables', undef, source_name => 'master')->then (sub {
      return promised_map {
        my $name = $_[0];
        return $db->execute ('show create table :name:id', {name => $name}, source_name => 'master')->then (sub {
          my $w = $_[0]->first;
          return $w->{'Create Table'};
        });
      } [sort { $a cmp $b } map { [values %$_]->[0] } @{$_[0]->all}];
    })->then (sub {
      return join ";\x0A\x0A", @{$_[0]};
    });
  })->finally (sub {
    return $db->disconnect;
  });
} # run

sub run_dumped ($$$;%) {
  my ($class, $sqls, $dsns, %args) = @_;
  my @done;
  my $db;
  my $set_db = sub {
    my $db_name = shift;
    push @done, $db->disconnect if defined $db;
    $db = Dongry::Database->new (
      sources => {
        master => {dsn => Dongry::Type->serialize ('text', $dsns->{$db_name} // die "Database |$db_name| has no dsn"),
                   writable => 1, anyevent => 1},
      },
    );
  }; # $set_db
  $set_db->([keys %$dsns]->[0] // die "Empty |dsns|");
  return Promise->resolve->then (sub {
    $sqls =~ s/--.*$//gm;
    $sqls =~ s/^;+//;
    return promised_until {
      my $sql = '';
      if ($sqls =~ s{^((?:"(?:[^"\\]+|\\.)*"|'(?:[^'\\]|\\.)*'|[^"';])+)}{}s) {
        $sql = $1;
      } else {
        return 'done';
      }
      $sqls =~ s{^;+}{};

      $sql =~ s/^\s+//;
      $sql =~ s/\s+$//;
      return not 'done' if
          $sql eq '' or
          $sql =~ /^LOCK TABLES / or
          $sql =~ /^UNLOCK TABLES/ or
          $sql =~ m{^/\*![0-9]+\s*SET\s+};
      
      if ($sql =~ m{^\s*USE\s*`([^`]+)`\s*$}) {
        my $name = $1;
        $name =~ s/_local$//g;
        $set_db->($name);
        return not 'done';
      }
      
      return $db->execute ($sql, undef, source_name => 'master')->then (sub {
        return not 'done';
      });
    };
  })->finally (sub {
    push @done, $db->disconnect;
  })->finally (sub {
    return Promise->all (\@done);
  });
} # run

1;

=head1 LICENSE

Copyright 2018-2022 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
