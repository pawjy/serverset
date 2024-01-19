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
use AnyEvent::MySQL::Client;
use Web::Encoding;
use Web::Host;

use ServerSet::DockerHandler;
push our @ISA, qw(ServerSet::DockerHandler);

sub quote ($) {
  my $s = $_[0];
  $s =~ s/`/``/g;
  return q<`> . $s . q<`>;
} # quote

sub get_keys ($) {
  my $self = shift;
  return {
    %{$self->SUPER::get_keys},
    mysqld_user => 'key:,16', # max=16 in MySQL 5.6
    mysqld_password => 'key',
    mysqld_root_password => 'key',
    mysqld_dir_name_id => 'id',
  };
} # get_keys

sub start ($$;%) {
  my $handler = shift;
  my ($ss, %s_args) = @_;
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
        'socket=/sock/mysqld.sock',
        (map { $_ . '=' . $args->{mycnf}->{$_} } grep { defined $args->{mycnf}->{$_} } keys %{$args->{mycnf} or {}}),
        '',
    ;
    
    my @dsn = (
      user => $self->key ('mysqld_user'),
      password => $self->key ('mysqld_password'),
      host => $self->local_url ('mysqld')->host,
      port => $self->local_url ('mysqld')->port,
      mysql_socket => $self->path ('sock/mysqld.sock'),
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
    my $temp_path = $self->path ('logs');
    return Promise->resolve->then (sub {
      return $self->regenerate_keys (['mysqld_dir_name_id'])
          if $s_args{try_count} > 1;
    })->then (sub {
      $data_path = $self->path ('mysqld-data-' . $self->key ('mysqld_dir_name_id'));
      
      return Promise->all ([
        Promised::File->new_from_path ($data_path)->mkpath,
        Promised::File->new_from_path ($temp_path->child ('log.txt'))->write_byte_string ("")->then (sub {
          chmod 0777, $temp_path->child ('log.txt');
        }),
        Promised::File->new_from_path ($self->path ('sock'))->mkpath->then (sub {
          chmod 0777, $self->path ('sock');
        }),
        $self->write_file ('my.cnf', $my_cnf),
      ]);
    })->then (sub {
      my $net_host = $args->{docker_net_host};

      my $ver = $args->{mysql_version} // '';
      my $MySQLImage = 'mariadb';
      if ($ver eq 'mysql') {
        $MySQLImage = 'mysql/mysql-server';
      } elsif ($ver eq 'mysql8') {
        $MySQLImage = 'mysql/mysql-server:8.0';
      } elsif ($ver eq 'mysql5.6') {
        $MySQLImage = 'mysql/mysql-server:5.6';
      } elsif (length $ver) {
        die "Unknown |mysql_version|: |$ver|";
      }
      
      return {
        image => $MySQLImage,
        volumes => [
          $self->path ('my.cnf')->absolute . ':/etc/my.cnf',
          $self->path ('my.cnf')->absolute . ':/etc/mysql/my.cnf',
          $data_path->absolute . ':/var/lib/mysql',
          $temp_path->absolute . ':/logs',
          $self->path ('sock')->absolute . ':/sock',
          (defined $s_args{volume_path} ? $s_args{volume_path} . ':' . $s_args{volume_path} : ()),
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
        command => [
          '--log-error=/logs/log.txt',
          #'--console',
        ],
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
      my $dsn = $data->{$ss->actual_or_local ('dsn_options')}->{[keys %{$args->{databases}}]->[0] // 'test'};
      return promised_wait_until {
        return $handler->check_running->then (sub {
          unless ($_[0]) {
            my $path = $self->path ('logs')->child ('log.txt');
            my $file = Promised::File->new_from_path ($path);
            return $file->is_file->then (sub {
              return unless $_[0];
              return $file->read_byte_string->then (sub {
                my $bytes = $_[0];
                warn "\n====== mysqld log ======\n$bytes\n====== / mysqld log ======\n";
              });
            })->then (sub {
              die "|mysqld| is not running";
            });
          }
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
              #
              # [ERROR] InnoDB: Missing FILE_CHECKPOINT at 42245 between the checkpoint 42245 and the end 45765.
              # [ERROR] InnoDB: Plugin initialization aborted with error Generic error
              # [ERROR] mariadbd: Got error 'Could not get an exclusive lock; file is probably in use by another process' when trying to use aria control file '/var/lib/mysql/aria_log_control'
              if ($log =~ m{\[ERROR\] InnoDB: Cannot open datafile './ibdata1'} or
                  $log =~ m{\[ERROR\] InnoDB: Unable to lock ./ibdata1 error: 11} or
                  $log =~ m{\[ERROR\] mysqld: Can't lock aria control file '/var/lib/mysql/aria_log_control' for exclusive use, error: 11. Will retry for 30 seconds} or
                  $log =~ m{\[ERROR\] InnoDB: Missing FILE_CHECKPOINT at } or
                  $log =~ m{\[ERROR\] mariadbd: Got error 'Could not get an exclusive lock; file is probably in use by another process'}) {
                die bless {message => "MySQL database is broken"},
                    'ServerSet::RestartableError';
              }
              return 0;
            });
          });
        });
      } timeout => 60*4, signal => $signal;
    })->then (sub {
      #return $client->query (
      #  encode_web_utf8 sprintf q{create user '%s'@'%s' identified by '%s'},
      #      $self->key ('mysqld_user'), '%',
      #      $self->key ('mysqld_password'),
      #);
    })->then (sub {
      #die $_[0] unless $_[0]->is_success;
      return promised_for {
        my $name = encode_web_utf8 (shift . $data->{_dbname_suffix});
        return $client->query ('create database if not exists ' . quote $name)->then (sub {
          die $_[0] unless $_[0]->is_success;
          return $client->query (
            encode_web_utf8 sprintf q{grant all on %s.* to '%s'@'%s'},
                quote $name,
                $self->key ('mysqld_user'), '%',
          )->then (sub {
            die $_[0] unless $_[0]->is_success;
          });
        });
      } [keys %{$args->{databases} or {}}];
    })->finally (sub {
      return $client->disconnect if defined $client;
    })->then (sub {
      return if $args->{no_dump};
      if ($s_args{try_count} > 1) {
        my $path = $self->path ('mysql-dump.sql');
        return unless $path->is_file;

        require ServerSet::Migration;
        my $file = Promised::File->new_from_path ($path);
        return $file->read_byte_string->then (sub {
          return ServerSet::Migration->run_dumped
              ($_[0] => $data->{$self->actual_or_local ('dsn')});
        }, sub { });
      }
    })->then (sub {
      return if $args->{no_dump};
      return promised_for {
        my $name = shift;
        my $path = $args->{databases}->{$name};
        return Promise->resolve->then (sub {
          return '' if ref $path;
          return Promised::File->new_from_path ($path)->read_byte_string->catch (sub {
            my $e = $_[0];
            die "Failed to open |$path|: $e";
            return $e;
          });
        })->then (sub {
          require ServerSet::Migration;
          return ServerSet::Migration->run ($_[0] => $data->{$self->actual_or_local ('dsn')}->{$name}, dump => 1);
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

  my $dsn = $data->{$ss->actual_or_local ('dsn_options')}->{[keys %{$data->{$ss->actual_or_local ('dsn_options')}}]->[0]};
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

Copyright 2018-2024 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
