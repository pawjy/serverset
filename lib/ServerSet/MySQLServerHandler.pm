package ServerSet::MySQLServerHandler;
use strict;
use warnings;
our $VERSION = '2.0';
use Promise;
use Promised::Flow;
use Promised::File;
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
  };
} # get_keys

sub start ($$;%) {
  my $handler = shift;
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

    my $data_path = $args->{data_path} // $self->path ('mysqld-data');
    return Promise->all ([
      Promised::File->new_from_path ($data_path)->mkpath,
      $self->write_file ('my.cnf', $my_cnf),
    ])->then (sub {
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
      my $ipaddr = Web::Host->parse_string (shift);
      
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
            return $client->disconnect->catch (sub { })->then (sub { 0 });
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

sub heartbeat_interval ($) { 6 }
sub heartbeat ($) {
  warn "XXX heartbeat";
}

1;
