package ServerSet::MySQLServerHandler;
use strict;
use warnings;
our $VERSION = '2.0';
use Promise;
use Promised::Flow;
use Promised::File;
use Promised::Command;
use ArrayBuffer;
use DataView;
use Dongry::SQL qw(quote);
use AnyEvent::MySQL::Client;
use Web::Encoding;
use Web::Host;

use ServerSet::Migration;
use ServerSet::DockerHandler;
push our @ISA, qw(ServerSet::DockerHandler);

#my $MySQLImage = 'mysql/mysql-server';
my $MySQLImage = 'mariadb';

sub get_keys ($) {
  my $self = shift;
  return {
    %{$self->SUPER::get_keys},
    mysqld_user => 'key',
    mysqld_password => 'key',
    mysqld_root_password => 'key',
    mysqld_dir_name_id => 'id',
  };
} # get_keys

sub start ($$;%) {
  my $handler = shift;
  my (undef, %s_args) = @_;
  my $params = $handler->{params};

  my $mysql_port = 3306;
  $params->{prepare} = sub {
    my ($handler, $self, $args, $data) = @_;
    my $my_cnf = join "\n", '[mysqld]',
        'user=mysql',
        'default_authentication_plugin=mysql_native_password', # XXX
        #'skip-networking',
        'bind-address=0.0.0.0',
        'port=' . $mysql_port,
        'innodb_lock_wait_timeout=2',
        'max_connections=1000',
        #'sql_mode=', # old default
        #'sql_mode=NO_ENGINE_SUBSTITUTION,STRICT_TRANS_TABLES', # 5.6 default
    ;

    my @dsn = (
      user => $self->key ('mysqld_user'),
      password => $self->key ('mysqld_password'),
      host => $self->local_url ('mysqld')->host,
      port => $self->local_url ('mysqld')->port,
    );
        my @dbname = keys %{$args->{databases}};
        @dbname = ('test') unless @dbname;
        $data->{_dbname_suffix} = $args->{database_name_suffix} // '';
        for my $dbname (@dbname) {
          $data->{local_dsn_options}->{$dbname} = {
            @dsn,
            dbname => $dbname . $data->{_dbname_suffix},
          };
          $data->{docker_dsn_options}->{$dbname} = {
            @dsn,
            host => $handler->dockerhost,
            dbname => $dbname . $data->{_dbname_suffix},
          };
          $data->{local_dsn}->{$dbname}
              = $self->dsn ('mysql', $data->{local_dsn_options}->{$dbname});
          $data->{docker_dsn}->{$dbname}
              = $self->dsn ('mysql', $data->{docker_dsn_options}->{$dbname});
        } # $dbname

    my $data_path;
    return Promise->resolve->then (sub {
      return $self->regenerate_keys (['mysqld_dir_name_id'])
          if $s_args{try_count} > 1;
    })->then (sub {
      $data_path = $self->path ('mysqld-data-' . $self->key ('mysqld_dir_name_id'));
      
      return Promise->all ([
        Promised::File->new_from_path ($data_path)->mkpath,
        $self->write_file ('my.cnf', $my_cnf),
      ]);
    })->then (sub {
      my $net_host = $args->{docker_net_host};
      return {
        image => $MySQLImage,
        volumes => [
          $self->path ('my.cnf')->absolute . ':/etc/my.cnf',
          $data_path->absolute . ':/var/lib/mysql',
        ],
        net_host => $net_host,
        ports => ($net_host ? undef : [
          $self->local_url ('mysqld')->hostport . ':' . $mysql_port,
        ]),
            environment => {
              MYSQL_USER => $self->key ('mysqld_user'),
              MYSQL_PASSWORD => $self->key ('mysqld_password'),
              MYSQL_ROOT_PASSWORD => $self->key ('mysqld_root_password'),
              #MYSQL_ROOT_HOST => $handler->dockerhost->to_ascii,
              MYSQL_ROOT_HOST => '%',
              MYSQL_DATABASE => $dbname[0] . $data->{_dbname_suffix},
              MYSQL_LOG_CONSOLE => 1,
            },
          };
    });
  }; # prepare

  $params->{beforewait} = sub {
    my ($handler, $self, $args, $data, $signal, $docker) = @_;

    return $docker->get_container_ipaddr->then (sub {
      my $ipaddr = Web::Host->parse_string ($_[0]) // die "Failed to get container IP address |$_[0]|";
      
      for my $dbname (keys %{$data->{local_dsn_options} or {}}) {
        $data->{actual_dsn_options}->{$dbname} = {
          %{$data->{local_dsn_options}->{$dbname}},
          host => $ipaddr,
          port => $mysql_port,
        };
        $data->{actual_dsn}->{$dbname} = $self->dsn
            ('mysql', $data->{actual_dsn_options}->{$dbname});
      } # $dbname
    });
  }; # beforewait
  
  $params->{wait} = sub {
    my ($handler, $self, $args, $data, $signal) = @_;
    my $client;
    return Promise->resolve->then (sub {
      require AnyEvent::MySQL::Client::ShowLog if $ENV{SQL_DEBUG};
      my $dsn = $data->{actual_dsn_options}->{[keys %{$args->{databases}}]->[0] // 'test'};
      return promised_wait_until {
        return $handler->check_running->then (sub {
          die "|mysqld| is not running" unless $_[0];
        })->then (sub {
          $client = AnyEvent::MySQL::Client->new;
          return $client->connect (
            hostname => $dsn->{host}->to_ascii,
            port => $dsn->{port},
            username => 'root',
            password => (encode_web_utf8 $self->key ('mysqld_root_password')),
            database => 'mysql',
          )->then (sub {
            return 1;
          })->catch (sub {
            return $client->disconnect->catch (sub { })->then (sub {
              my $log = $handler->logs;
              # 2022-05-16  2:07:20 0 [ERROR] InnoDB: Cannot open datafile './ibdata1'
              # 2022-05-16  2:29:08 0 [ERROR] InnoDB: Unable to lock ./ibdata1 error: 11
              # [ERROR] mysqld: Can't lock aria control file '/var/lib/mysql/aria_log_control' for exclusive use, error: 11. Will retry for 30 seconds
              if ($log =~ m{\[ERROR\] InnoDB: Cannot open datafile './ibdata1'} or
                  $log =~ m{\[ERROR\] InnoDB: Unable to lock ./ibdata1 error: 11} or
                  $log =~ m{\[ERROR\] mysqld: Can't lock aria control file '/var/lib/mysql/aria_log_control' for exclusive use, error: 11. Will retry for 30 seconds}) {
                die bless {message => "MySQL database is broken"},
                    'ServerSet::RestartableError';
              }
              return 0;
            });
          });
        });
      } timeout => 60*4, signal => $signal;
    })->then (sub {
          return $client->query (
            encode_web_utf8 sprintf q{create user '%s'@'%s' identified by '%s'},
                $self->key ('mysqld_user'), '%',
                $self->key ('mysqld_password'),
          );
        })->then (sub {
          return promised_for {
            my $name = encode_web_utf8 (shift . $data->{_dbname_suffix});
            return $client->query ('create database if not exists ' . quote $name)->then (sub {
              return $client->query (
                encode_web_utf8 sprintf q{grant all on %s.* to '%s'@'%s'},
                    quote $name,
                    $self->key ('mysqld_user'), '%',
              );
            });
          } [keys %{$args->{databases} or {}}];
        })->finally (sub {
          return $client->disconnect if defined $client;
    })->then (sub {
      if ($s_args{try_count} > 1) {
        my $path = $self->path ('mysql-dump.sql');
        my $file = Promised::File->new_from_path ($path);
        return $file->read_byte_string->then (sub {
          return ServerSet::Migration->run_dumped
              ($_[0] => $data->{actual_dsn});
        }, sub { });
      }
    })->then (sub {
          return promised_for {
            my $name = shift;
            my $path = $args->{databases}->{$name};
            return Promised::File->new_from_path ($path)->read_byte_string->catch (sub {
              my $e = $_[0];
              die "Failed to open |$path|: $e";
            })->then (sub {
              return ServerSet::Migration->run ($_[0] => $data->{actual_dsn}->{$name}, dump => 1);
            })->then (sub {
              return $self->write_file ("mysqld-$name.sql" => $_[0]);
            });
          } [keys %{$args->{databases} or {}}];
        });
  }; # wait

  return $handler->SUPER::start (@_);
} # start

sub heartbeat_interval ($) { 600 }
sub heartbeat ($$$) {
  my ($handler, $ss, $data) = @_;

  my $dsn = $data->{actual_dsn_options}->{[keys %{$data->{actual_dsn_options}}]->[0]};
  return unless defined $dsn;
  my $cmd = Promised::Command->new ([
    'mysqldump',
    '-A',
    '-h', $dsn->{host}->to_ascii,
    '-P', $dsn->{port},
    '-u', $dsn->{user},
    '--password=' . $dsn->{password},
  ]);

  my $path = $ss->path ('mysql-dump.sql');
  my $file = Promised::File->new_from_path ($path);
  my $ws = $file->write_bytes;
  my $w = $ws->get_writer;

  my @wait;
  my $so_rs = $cmd->get_stdout_stream;
  my $so_r = $so_rs->get_reader ('byob');
  push @wait, promised_until {
    return $so_r->read (DataView->new (ArrayBuffer->new (1024)))->then (sub {
      if ($_[0]->{done}) {
        push @wait, $w->close;
        return 'done';
      }
      return $w->write ($_[0]->{value})->then (sub {
        return not 'done';
      });
    });
  };

  push @wait, $cmd->run->then (sub {
    return $cmd->wait;
  })->then (sub {
    my $return = $_[0];
    die $return unless $return->exit_code == 0;
  });

  return Promise->all (\@wait)->catch (sub {
    my $e = $_[0];
    warn __PACKAGE__ . ": $e";
  });
} # heartbeat

1;

=head1 LICENSE

Copyright 2018-2022 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
