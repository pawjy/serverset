package ServerSet::ReverseProxyProxyManager;
use strict;
use warnings;
use JSON::PS;
use Web::Host;
use Web::URL;
use Web::Transport::ENVProxyManager;
use Web::Transport::BasicClient;

sub new_from_envs ($$) {
  my ($class, $envs) = @_;
  return bless {
    pm => Web::Transport::ENVProxyManager->new_from_envs ($envs),
    http_proxy => Web::URL->parse_string ($envs->{http_proxy}), # or undef
  }, $class;
} # new_from_envs

sub get_proxies_for_url ($$;%) {
  my ($self, $url, %args) = @_;
  
  if (defined $self->{http_proxy} and $url->host->is_domain) {
    my $resolve_url = Web::URL->parse_string ("http://resolver.ss.test/");
    my $resolve_client = Web::Transport::BasicClient->new_from_url ($resolve_url, {
      proxy_manager => $self->{pm},
    });
    return $resolve_client->request (
      path => [],
      headers => {'x-url' => $url->stringify},
    )->then (sub {
      my $res = $_[0];
      if ($res->status == 200) {
        my $json = json_bytes2perl $res->body_bytes;
        if (defined $json->{proxies}) {
          for (@{$json->{proxies}}) {
            $_->{host} = Web::Host->parse_string ($_->{host})
                if defined $_->{host};
          }
          return $json->{proxies};
        }
      }
      return $self->{pm}->get_proxies_for_url ($url, %args);
    });
  }
  
  return $self->{pm}->get_proxies_for_url ($url, %args);
} # get_proxies_for_url

1;

=head1 LICENSE

Copyright 2018-2019 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
