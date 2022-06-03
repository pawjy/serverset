use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->child ('t_deps/modules/*/lib');
use AbortController;
use ServerSet;
use Test::X1;
use Test::More;
use Web::Transport::BasicClient;

sub run ($;%) {
  my $class = shift;

  my $defs = {};

  $defs->{storage} = {
    handler => 'ServerSet::MinioHandler',
  };

  $defs->{_} = {
    requires => ['storage'],
    start => sub {
      my ($handler, $ss, %args) = @_;
      my $data = {};
      return $args{receive_storage_data}->then (sub {
        my $storage_data = $_[0];
        
        $data->{local_url} = $ss->local_url ('storage');
        $data->{aws4} = $storage_data->{aws4};
        $data->{bucket} = $ss->key ('storage_bucket');
        
        return [$data, undef];
      });
    },
  };
  
  return ServerSet->run ($defs, sub {
    my ($ss, $args) = @_;
    my $result = {};

    $result->{server_params} = {
      storage => {
        public_prefixes => [
          '/public',
          '/accounts',
        ],
        writable_prefixes => [
          '/accounts',
        ],
      },
      _ => {},
    };
    
    return $result;
  }, @_);
} # run

test {
  my $c = shift;
  
  my $ac = AbortController->new;
  my $done;
  __PACKAGE__->run (signal => $ac->signal)->then (sub {
    my $result = $_[0];
    test {
      ok $done = $result->{done}, 'done promise';
      ok $result->{data}->{local_url}, $result->{data}->{local_url}->stringify;
    } $c;

    my $client = Web::Transport::BasicClient->new_from_url
        ($result->{data}->{local_url});
    return $client->request (
      method => 'PUT',
      path => [$result->{data}->{bucket}, 'accounts', 'abcde'],
      body => 'xyzabc',
      aws4 => $result->{data}->{aws4},
    )->then (sub {
      my $res = $_[0];
      test {
        is $res->status, 200;
      } $c;
      return $client->request (
        method => 'GET',
        path => [$result->{data}->{bucket}, 'accounts', 'abcde'],
      );
    })->then (sub {
      my $res = $_[0];
      test {
        is $res->body_bytes, 'xyzabc';
      } $c;
    })->finally (sub {
      return $client->close;
    });
  })->catch (sub {
    my $e = $_[0];
    test {
      ok 0, 'no error';
      is $e, undef, $e;
    } $c;
  })->finally (sub {
    $ac->abort;
    return $done;
  })->finally (sub {
    done $c;
    undef $c;
  })->to_cv->recv;
} n => 4;

run_tests;

=head1 LICENSE

Copyright 2022 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
