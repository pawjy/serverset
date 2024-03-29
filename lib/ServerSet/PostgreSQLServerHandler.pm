package ServerSet::PostgreSQLServerHandler;
use strict;
use warnings;
our $VERSION = '2.0';
use Promise;
use Promised::Flow;
use Promised::File;
use Promised::Command;

use ServerSet::DockerHandler;
push our @ISA, qw(ServerSet::DockerHandler);

sub start ($$;%) {
  my $handler = shift;
  my $params = $handler->{params};

  my $pg_port = 5432;
  $params->{prepare} = sub {
    my ($handler, $self, $args, $data) = @_;

    my @dsn = (
      user => 'user',
      password => 'password',
      host => $self->local_url ('postgresql')->host,
      port => $self->local_url ('postgresql')->port,
    );
    my @dbname = keys %{$args->{databases}};
    @dbname = ('test') unless @dbname;
    die "Only one database name must be specified" unless @dbname == 1;
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
      $data->{local_dsn}->{$dbname} = $self->dsn
          ('Pg', $data->{local_dsn_options}->{$dbname});
      $data->{docker_dsn}->{$dbname} = $self->dsn
          ('Pg', $data->{docker_dsn_options}->{$dbname});
    } # $dbname

    my $data_path = $args->{data_path} // $self->path ('pg-data4');
    return Promised::File->new_from_path ($data_path)->mkpath->then (sub {
      return promised_for {
        my $name = shift;
        my $old_path = $args->{databases}->{$name};
        my $new_path = $self->path ('pg-schema')->child ("pg-$name.sql");
        return Promised::File->new_from_path ($old_path)->read_byte_string->then (sub {
          return Promised::File->new_from_path ($new_path)->write_byte_string ($_[0]);
        });
      } [keys %{$args->{databases} or {}}];
    })->then (sub {
      my $init_path = $self->path ('pg-schema')->child ('init.sh');
      return Promised::File->new_from_path ($init_path)->write_byte_string (q{#!/bin/bash
        echo "host all all all trust" > /pgdata/pg_hba.conf
      });
    })->then (sub {
      my $net_host = $args->{docker_net_host};
      return {
        image => 'quay.io/wakaba/postgresql',
        volumes => [
          $data_path->absolute . ':/pgdata',
          $self->path ('pg-schema')->absolute.':/docker-entrypoint-initdb.d',
        ],
        net_host => $net_host,
        ports => ($net_host ? undef : [
          $self->local_url ('postgresql')->hostport.':'.$pg_port,
        ]),
        environment => {
          POSTGRES_USER => 'user',
          POSTGRES_PASSWORD => 'password',
          POSTGRES_DB => $dbname[0] . $data->{_dbname_suffix},
          PGDATA => '/pgdata',
        },
        command => [
          '-c', 'port=' . $pg_port,
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
          port => $pg_port,
        };
        $data->{actual_dsn}->{$dbname} = $self->dsn
            ('Pg', $data->{actual_dsn_options}->{$dbname});
      } # $dbname
    });
  }; # beforewait
  
  $params->{wait} = sub {
    my ($handler, $self, $args, $data, $signal) = @_;
    my $dsn = $data->{$self->actual_or_local ('dsn')}->{[keys %{$args->{databases}}]->[0] // 'test'};
    return promised_wait_until {
      return $handler->check_running->then (sub {
        die "|postgresql| is not running" unless $_[0];
      })->then (sub {
        my $cmd = Promised::Command->new (['perl', '-MDBI', '-e', q{
          use DBI;
          my $dbh = DBI->connect (shift, undef, undef, {RaiseError => 1});
          exit 0;
        }, $dsn]);
        return $cmd->run->then (sub { return $cmd->wait })->then (sub {
          return $_[0]->exit_code == 0;
        });
      });
    } timeout => 60*5, signal => $signal;
  }; # wait
  
  return $handler->SUPER::start (@_);
} # start

1;
