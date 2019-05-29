package ServerSet::MinioHandler;
use strict;
use warnings;
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
    return Promise->all ([
      Promised::File->new_from_path ($self->path ('minio_config'))->mkpath,
      Promised::File->new_from_path ($self->path ('minio_data'))->mkpath,
    ])->then (sub {
      my $port = $self->local_url ('storage')->port; # default: 9000
      return {
            image => 'minio/minio',
            volumes => [
              $self->path ('minio_config')->absolute . ':/config',
              $self->path ('minio_data')->absolute . ':/data',
            ],
            user => "$<:$>",
            command => [
              'server',
              '--address', "0.0.0.0:" . $port,
              '--config-dir', '/config',
              '/data'
            ],
            ports => [
              $self->local_url ('storage')->hostport . ":" . $port,
            ],
          };
        });
  }, # prepare
  sniff_log => sub {
    my ($handler, $self, $log) = @_;
    die "Disk is full" if $log =~ m{ERROR.*Unable to initialize config system:.*Storage reached its minimum free disk threshold};
  },
  wait => sub {
    my ($handler, $self, $args, $data, $signal) = @_;
    return Promise->resolve->then (sub {
      my $config1_path = $self->path ('minio_config')->child ('config.json');
      my $config2_path = $self->path ('minio_data')->child ('.minio.sys/config/config.json');
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
          $data->{aws4}->[0] = $config->{credential}->{accessKey};
          $data->{aws4}->[1] = $config->{credential}->{secretKey};
          $data->{aws4}->[2] = $config->{region};
          return defined $data->{aws4}->[0] &&
                 defined $data->{aws4}->[1] &&
                 defined $data->{aws4}->[2];
        })->catch (sub { return 0 });
      } timeout => 60*4, signal => $signal, name => 'minio config';
    })->then (sub {
      return $self->wait_for_http ($self->local_url ('storage'),
          signal => $signal, name => 'wait for storage');
    });
  }, # wait
  init => sub {
    my ($handler, $self, $args, $data, $signal) = @_;
    my $bucket_domain = $self->key ('storage_bucket');
    $bucket_domain =~ tr/A-Z_-/a-z/;
    my $public_prefixes = $args->{public_prefixes} || [];
    my $s3_url = Web::URL->parse_string
        ("/$bucket_domain/", $self->local_url ('storage'));
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
          ("/$bucket_domain/", $self->client_url ('storage'));
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
