package ServerSet::ApploachHandler;
use strict;
use warnings;
our $VERSION = '2.0';
use Promise;
use Promised::Flow;
use Promised::File;

use ServerSet::DockerHandler;
push our @ISA, qw(ServerSet::DockerHandler);

sub get_keys ($) {
  my $self = shift;
  return {
    %{$self->SUPER::get_keys},
    apploach_bearer => 'key',
  };
} # get_keys

my $Methods = {
  prepare => sub {
    my ($handler, $self, $args, $data) = @_;
    my $apploach_port = $data->{_apploach_port} = 8080;
    return Promise->all ([
      $self->read_json (\($args->{config_path})),
      $args->{receive_mysqld_data},
      $args->{receive_storage_data},
    ])->then (sub {
      my ($config, $mysqld_data, $storage_data) = @{$_[0]};

      $config->{s3_aws4} = $storage_data->{aws4};
      #"s3_sts_role_arn"
      $config->{s3_bucket} = $storage_data->{bucket_domain};
      $config->{s3_form_url} = $storage_data->{form_client_url}->stringify;
      $config->{s3_file_url_prefix} = $storage_data->{file_root_client_url}->stringify;

      $data->{local_dsn} = $self->dsn
          ('mysql', $mysqld_data->{local_dsn_options}->{apploach});
      $data->{docker_dsn} = $self->dsn
          ('mysql', $mysqld_data->{docker_dsn_options}->{apploach});
      $data->{actual_dsn} = $self->dsn
          ('mysql', $mysqld_data->{actual_dsn_options}->{apploach});

      my $envs = {};
      $self->set_docker_envs ('proxy' => $envs);
      
      return Promise->all ([
        $self->write_json ('apploach-config.json', {
          %$config,
          bearer => $self->key ('apploach_bearer'),
          dsn => $data->{actual_dsn},
        }),
      ])->then (sub {
        my $net_host = $args->{docker_net_host};
        return {
          image => 'quay.io/wakaba/apploach',
          volumes => [
            $self->path ('apploach-config.json')->absolute . ':/config.json',
          ],
          net_host => $net_host,
          ports => ($net_host ? undef : [
            $self->local_url ('apploach')->hostport.':'.$apploach_port,
          ]),
          environment => {
            %$envs,
            PORT => $apploach_port,
            APP_CONFIG => '/config.json',

            SQL_DEBUG => $args->{debug} || 0,
            WEBUA_DEBUG => $args->{debug} || 0,
            WEBSERVER_DEBUG => $args->{debug} || 0,
          },
        };
      });
    });
  }, # prepare
  beforewait => sub {
    my ($handler, $ss, $args, $data, $signal, $docker) = @_;

    return $docker->get_container_ipaddr->then (sub {
      my $url = Web::URL->parse_string ('http://' . $_[0] . ':' . $data->{_apploach_port});
      $ss->set_actual_url ('apploach', $url);
    });
  }, # beforewait
  wait => sub {
    my ($handler, $ss, $args, $data, $signal) = @_;
    return $ss->wait_for_http ($ss->actual_or_local_url ('apploach'),
        signal => $signal, name => 'wait for apploach');
  }, # wait
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
