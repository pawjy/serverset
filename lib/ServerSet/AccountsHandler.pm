package ServerSet::AccountsHandler;
use strict;
use warnings;
use Promise;
use Promised::Flow;
use Promised::File;

use ServerSet::DockerHandler;
push our @ISA, qw(ServerSet::DockerHandler);

sub get_keys ($) {
  my $self = shift;
  return {
    %{$self->SUPER::get_keys},
    accounts_key => 'key',
  };
} # get_keys

my $Methods = {
  prepare => sub {
    my ($handler, $self, $args, $data) = @_;
    return Promise->all ([
      $self->read_json (\($args->{config_path})),
      $self->read_json (\($args->{servers_path})),
      $args->{receive_mysqld_data},
      $args->{receive_storage_data},
    ])->then (sub {
      my ($config, $servers, $mysqld_data, $storage_data) = @{$_[0]};
      
      $data->{local_dsn} = $self->dsn
          ('mysql', $mysqld_data->{local_dsn_options}->{accounts});
      $data->{docker_dsn} = $self->dsn
          ('mysql', $mysqld_data->{docker_dsn_options}->{accounts});

      $config->{s3_access_key_id} = $storage_data->{aws4}->[0];
      $config->{s3_secret_access_key} = $storage_data->{aws4}->[1];
      #"s3_sts_role_arn"
      $config->{s3_region} = $storage_data->{aws4}->[2];
      $config->{s3_bucket} = $storage_data->{bucket_domain};
      $config->{s3_form_url} = $storage_data->{form_client_url}->stringify;
      $config->{s3_image_url_prefix} = $storage_data->{file_root_client_url}->stringify;
      $config->{s3_key_prefix} = 'accounts';

      my $envs = {};
      $self->set_docker_envs ('proxy' => $envs);
      
      return Promise->all ([
        $self->write_json ('accounts/servers.json', {
          %$servers,
        }),
        $self->write_json ('accounts/config.json', {
          %$config,
          'auth.bearer' => $self->key ('accounts_key'),
          servers_json_file => 'servers.json',
          dsns => {account => $data->{docker_dsn}},
          alt_dsns => {master => {account => $data->{docker_dsn}}},
        }),
      ])->then (sub {
        my $net_host = $args->{docker_net_host};
        my $port = $self->local_url ('accounts')->port; # default: 8080
        return {
          image => 'quay.io/wakaba/accounts',
          volumes => [
            $self->path ('accounts')->absolute . ':/config',
          ],
          net_host => $net_host,
          ports => ($net_host ? undef : [
            $self->local_url ('accounts')->hostport.':'.$port,
          ]),
          environment => {
            %$envs,
            PORT => $port,
            APP_CONFIG => '/config/config.json',
            SQL_DEBUG => $args->{debug} || 0,
            WEBUA_DEBUG => $args->{debug} || 0,
            WEBSERVER_DEBUG => $args->{debug} || 0,
          },
          command => ['/server'],
        };
      });
    });
  }, # prepare
  wait => sub {
    my ($handler, $self, $args, $data, $signal) = @_;
    return $self->wait_for_http ($self->local_url ('accounts'),
        signal => $signal, name => 'wait for accounts');
  }, # wait
}; # $Methods

sub start ($$;%) {
  my $handler = shift;
  my $params = $handler->{params};

  $params->{$_} = $Methods->{$_} for keys %$Methods;

  return $handler->SUPER::start (@_);
} # start

1;
