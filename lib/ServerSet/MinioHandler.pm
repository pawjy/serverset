package ServerSet::MinioHandler;
use strict;
use warnings;
our $VERSION = '2.0';
use Promise;
use Promised::Flow;
use Promised::File;
use JSON::PS;
use Web::URL;
use Web::Transport::BasicClient;

use ServerSet::DockerHandler;
push our @ISA, qw(ServerSet::DockerHandler);

sub get_keys ($) {
  my $self = shift;
  return {
    %{$self->SUPER::get_keys},
    storage_bucket => 'key',
  };
} # get_keys

my $Methods = {
  prepare => sub {
    my ($handler, $self, $args, $data) = @_;
    $data->{aws4} = [undef, undef, undef, 's3'];
    my $storage_port = $data->{_storage_port} = 9000; # default
    return Promise->all ([
      Promised::File->new_from_path ($self->path ('minio_config'))->mkpath,
      Promised::File->new_from_path ($self->path ('minio_data'))->mkpath,
    ])->then (sub {
      my $net_host = $args->{docker_net_host};
      return {
        image => 'minio/minio',
        volumes => [
          $self->path ('minio_config')->absolute . ':/config',
          $self->path ('minio_data')->absolute . ':/data',
        ],
        no_tty => 1,
        user => ($args->{no_set_uid} ? undef : "$<:$>"),
        environment => {
          MINIO_BROWSER => 'off',
        },
        command => [
          'server',
          '--address', "0.0.0.0:" . $storage_port,
          '--config-dir', '/config',
          '/data'
        ],
        net_host => $net_host,
        ports => ($net_host ? undef : [
          $self->local_url ('storage')->hostport . ":" . $storage_port,
        ]),
      };
    });
  }, # prepare
  sniff_log => sub {
    my ($handler, $self, $log) = @_;
    die "Disk is full" if $log =~ m{ERROR.*Unable to initialize config system:.*Storage reached its minimum free disk threshold};
  },
  beforewait => sub {
    my ($handler, $ss, $args, $data, $signal, $docker) = @_;

    return $docker->get_container_ipaddr->then (sub {
      my $url = Web::URL->parse_string ('http://' . $_[0] . ':' . $data->{_storage_port});
      $ss->set_actual_url ('storage', $url);
    });
  }, # beforewait
  wait => sub {
    my ($handler, $ss, $args, $data, $signal) = @_;
    return Promise->resolve->then (sub {
      my $config1_path = $ss->path ('minio_config')->child ('config.json');
      my $config2_path = $ss->path ('minio_data')->child ('.minio.sys/config/config.json');
      my $f1 = Promised::File->new_from_path ($config1_path);
      my $f2 = Promised::File->new_from_path ($config2_path);
      return promised_wait_until {
        return $handler->check_running->then (sub {
          die "|storage| is not running" unless $_[0];
          return Promise->all ([$f1->is_file, $f2->is_file]);
        })->then (sub {
          return (($_[0]->[1] ? $f2 : $f1)->read_byte_string);
        })->then (sub {
          my $config = json_bytes2perl $_[0];
          if (defined $config->{region}->{_}) {
            $data->{aws4}->[0] = [grep { $_->{key} eq 'access_key' } @{$config->{credentials}->{_}}]->[0]->{value};
            $data->{aws4}->[1] = [grep { $_->{key} eq 'secret_key' } @{$config->{credentials}->{_}}]->[0]->{value};
            $data->{aws4}->[2] = [grep { $_->{key} eq 'name' } @{$config->{region}->{_}}]->[0]->{value};
          } else { # old
            $data->{aws4}->[0] = $config->{credential}->{accessKey};
            $data->{aws4}->[1] = $config->{credential}->{secretKey};
            $data->{aws4}->[2] = $config->{region};
          }
          return defined $data->{aws4}->[0] &&
                 defined $data->{aws4}->[1] &&
                 defined $data->{aws4}->[2];
        })->catch (sub { return 0 });
      } timeout => 60*4, signal => $signal, name => 'minio config';
    })->then (sub {
        return $ss->wait_for_http
            ($ss->actual_url ('storage'),
             signal => $signal, name => 'wait for storage');
    });
  }, # wait
  init => sub {
    my ($handler, $ss, $args, $data, $signal) = @_;
    my $bucket_domain = $ss->key ('storage_bucket');
    $bucket_domain =~ tr/A-Z_-/a-z/;
    my $public_prefixes = $args->{public_prefixes} || [];
    my $s3_url = Web::URL->parse_string
        ("/$bucket_domain/", $ss->actual_url ('storage'));
    my $client = Web::Transport::BasicClient->new_from_url ($s3_url);
    return $client->request (
      url => $s3_url, method => 'PUT',
      aws4 => $data->{aws4},
    )->then (sub {
      die $_[0] unless $_[0]->status == 200 || $_[0]->status == 409; # 409 if exists
      my $body = perl2json_bytes {
        "Version" => "2012-10-17",
        "Statement" => [map {
          {
            "Action" => ["s3:GetObject"],
            "Effect" => "Allow",
            "Principal" => {"AWS" => ["*"]},
            "Resource" => ["arn:aws:s3:::$bucket_domain$_/*"],
            "Sid" => "",
          }
        } @$public_prefixes],
      };
      return $client->request (
        url => Web::URL->parse_string ('./?policy', $s3_url),
        method => 'PUT', aws4 => $data->{aws4}, body => $body,
      );
    })->then (sub {
      die $_[0] unless $_[0]->is_success;
      return promised_sleep 3;
    })->then (sub {
      $data->{bucket_domain} = $bucket_domain;
      $data->{file_root_client_url} =
      $data->{form_client_url} = Web::URL->parse_string
          ("/$bucket_domain/", $ss->client_url ('storage'));
    })->finally (sub {
      return $client->close;
    });
  }, # init
}; # $Methods

sub start ($$;%) {
  my $handler = shift;
  my $params = $handler->{params};

  $params->{$_} = $Methods->{$_} for keys %$Methods;

  return $handler->SUPER::start (@_);
} # start

1;

=head1 LICENSE

Copyright 2018-2022 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
