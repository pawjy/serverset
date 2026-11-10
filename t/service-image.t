use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->child ('t_deps/lib');
use lib glob path (__FILE__)->parent->parent->child ('t_deps/modules/*/lib');
use Test::More;
use Promise;
use Web::URL;
use ServerSet::AccountsHandler;
use ServerSet::ApploachHandler;

for my $service (qw(accounts apploach)) {
  my $class = $service eq 'accounts' ? 'ServerSet::AccountsHandler' : 'ServerSet::ApploachHandler';
  for my $case (qw(omitted undef tag digest)) {
    my $ss = bless {written => {}}, 'ImageTestServerSet';
    my $handler = $class->new_from_params ($service, {});
    my $expected = "quay.io/wakaba/$service";
    my %args;
    $args{image} = undef if $case eq 'undef';
    $args{image} = $expected = "example.test/$service:test-build" if $case eq 'tag';
    $args{image} = $expected = "example.test/$service\@sha256:" . ('a' x 64) if $case eq 'digest';
    $args{docker_net_host} = $case eq 'digest';
    $args{receive_mysqld_data} = Promise->resolve ({map {
      ("${_}_dsn_options" => {$service => {}})
    } qw(local docker actual)});
    $args{receive_storage_data} = Promise->resolve ({
      aws4 => [qw(synthetic-key synthetic-secret synthetic-region)],
      bucket_domain => 'bucket.test',
      form_client_url => Web::URL->parse_string ('http://storage.test/form'),
      file_root_client_url => Web::URL->parse_string ('http://storage.test/files/'),
    });
    my $configured;
    $args{edit_config} = sub { $configured++; $_[1]->{test_setting} = 1 };
    my $definition;
    {
      # Exercise the real subclass start/prepare paths without starting Docker.
      no warnings 'redefine';
      local *ServerSet::DockerHandler::start = sub ($$%) {
        my ($handler, $ss, %args) = @_;
        return $handler->{params}->{prepare}->($handler, $ss, \%args, {});
      };
      $definition = $handler->start ($ss, %args)->to_cv->recv;
    }
    is $definition->{image}, $expected, "$service $case image";
    is $definition->{environment}->{PORT}, 8080, "$service $case preserves port";
    is $definition->{net_host}, $args{docker_net_host}, "$service $case preserves networking";
    if ($case eq 'digest') {
      ok !defined $definition->{ports}, "$service preserves host network port behavior";
    } else {
      is_deeply $definition->{ports}, ['127.0.0.1:12345:8080'], "$service preserves port mapping";
    }
    if ($service eq 'accounts') {
      is_deeply $definition->{command}, ['/server'], 'Accounts command unchanged';
      is $definition->{environment}->{APP_CONFIG}, '/config/config.json', 'Accounts config unchanged';
    } else {
      is $configured, 1, 'Apploach still invokes edit_config once';
      is $ss->{written}->{'apploach-config.json'}->{test_setting}, 1, 'Apploach keeps edited configuration';
    }
  }
}
done_testing;

package ImageTestServerSet;
sub read_json { return Promise->resolve ({}) }
sub write_json { $_[0]->{written}->{$_[1]} = $_[2]; return Promise->resolve }
sub dsn { return 'synthetic-dsn' }
sub key { return 'synthetic-key' }
sub set_docker_envs { $_[2]->{TEST_PROXY} = 'synthetic-proxy' }
sub path { return Path::Tiny::path ('/synthetic', $_[1]) }
sub local_url { return Web::URL->parse_string ('http://127.0.0.1:12345/') }
