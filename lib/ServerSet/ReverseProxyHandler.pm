package ServerSet::ReverseProxyHandler;
use strict;
use warnings;
use Path::Tiny;
use AnyEvent;
use AnyEvent::Socket;
use AbortController;
use Promise;
use Promised::Flow;
use Promised::File;
use Web::Transport::ProxyServerConnection;
use Web::Transport::PKI::Generator;
use Web::Transport::PKI::Parser;

use ServerSet::DefaultHandler;
push our @ISA, qw(ServerSet::DefaultHandler);

sub new_from_params ($$) {
  my ($class, $params) = @_;
  return bless {params => $params, logs => ''}, $class;
} # new_from_params

sub start ($$;%) {
  my ($handler, $ss, %args) = @_;
  my $data = {};

  my $params = $handler->{params};
  my $statechange = $handler->onstatechange;

  return Promise->resolve->then (sub {
    die $args{signal}->manakai_error if $args{signal}->aborted;
    return $params->{prepare}->($handler, $ss, \%args, $data); # or throw
  })->then (sub {
    my $prepared = $_[0];

    $prepared = {
      %$prepared,
      http_local_url => $ss->local_url ('proxy'),
      https_local_url => $ss->local_url ('sproxy'),
      https_client_url => $ss->client_url ('sproxy'),
      
      ca_cert_path => $ss->path ('ca-cert.pem'),
      ca_key_path => $ss->path ('ca-key.pem'),
    };
    
    my $cv = AE::cv;
    $cv->begin;

    my $map = $ss->{proxy_map};
    my $code = sub {
      my $args = $_[0];
      my $url = $args->{request}->{url};
      my $mapped = $map->{$url->host->to_ascii};
      if (defined $mapped) {
        my $x = $url->stringify;
        $x =~ s/^https:/http:/g;
        $args->{request}->{url} = Web::URL->parse_string ($x);
        push @{$args->{request}->{headers}}, ['x-forwarded-scheme', $url->scheme];
        $args->{client_options}->{server_connection}->{url} = $mapped;
        return $args;
      } else {
                warn "proxy: ERROR: Unknown host in <@{[$url->stringify]}>\n";
                my $body = 'Host not registered: |'.$url->host->to_ascii.'|';
                return {response => {
                  status => 504,
                  status_text => $body,
                  headers => [['content-type', 'text/plain;charset=utf-8']],
                  body => $body,
                }} unless $args{allow_forwarding};
              }
      return $args;
    }; # $code

    my $lpurl = $prepared->{http_local_url};
    my $pserver = tcp_server $lpurl->host->to_ascii, $lpurl->port, sub {
      $cv->begin;
      my $con = Web::Transport::ProxyServerConnection->new_from_aeargs_and_opts
          (\@_, {
            handle_request => $code,
          });
      $con->closed->finally (sub { $cv->end });
    }; # $pserver

    my $gen = Web::Transport::PKI::Generator->new;
    my $ca_cert_path = $prepared->{ca_cert_path};
    my $ca_key_path = $prepared->{ca_key_path};
    my $ca_cert_file = Promised::File->new_from_path ($ca_cert_path);
    my $ca_key_file = Promised::File->new_from_path ($ca_key_path);
    my $host = $prepared->{https_client_url}->host;
    my $get_certs = Promise->all ([
      $ca_cert_file->is_file,
      $ca_key_file->is_file,
    ])->then (sub {
      return Promise->all ([
        $_[0]->[0] ? $ca_cert_file->stat : undef,
        $_[0]->[0] ? $ca_key_file->stat : undef,
      ]);
    })->then (sub {
      my $ca_cert_time = defined $_[0]->[0] ? $_[0]->[0]->mtime : 0;
      my $ca_key_time = defined $_[0]->[1] ? $_[0]->[1]->mtime : 0;
      my $expires = 100*24*60*60;
      if ($ca_cert_time + $expires < time + 24*60*60 or
          $ca_key_time + $expires < time + 24*60*60) {
        return $gen->create_rsa_key->then (sub {
          my $ca_rsa = $_[0];
          my $name = "The " . path (__FILE__)->absolute . " Root CA (" . rand . ")";
          return $gen->create_certificate (
            rsa => $ca_rsa,
            ca_rsa => $ca_rsa,
            subject => {O => $name},
            issuer => {O => $name},
            not_before => time - 60,
            not_after => time + $expires,
            serial_number => 1,
            ca => 1,
            # XXX domain constraints
          )->then (sub {
            my $ca_cert = $_[0];
            return Promise->all ([
              $ca_cert_file->write_byte_string ($ca_cert->to_pem),
              $ca_key_file->write_byte_string ($ca_rsa->to_pem),
            ])->then (sub {
              return [$ca_cert, $ca_rsa];
            });
          });
        });
      } else {
        my $parser = Web::Transport::PKI::Parser->new;
        return Promise->all ([
          $ca_cert_file->read_byte_string,
          $ca_key_file->read_byte_string,
        ])->then (sub {
          my $ca_cert = $parser->parse_pem ($_[0]->[0])->[0];
          my $ca_key = $parser->parse_pem ($_[0]->[1])->[0];
          return [$ca_cert, $ca_key];
        });
      }
    })->then (sub {
      my ($ca_cert, $ca_rsa) = @{$_[0]};
      return Promise->all ([
        $gen->create_rsa_key,
      ])->then (sub {
        my $rsa = $_[0]->[0];
        return $gen->create_certificate (
          rsa => $rsa,
          ca_rsa => $ca_rsa,
          ca_cert => $ca_cert,
          not_before => time - 30,
          not_after => time + 30*24*60*60,
          serial_number => int rand 10000000,
          subject => {CN => $host->to_ascii},
          san_hosts => [$host, map { $_->host } @{$prepared->{client_urls}}],
          ee => 1,
        )->then (sub {
          my $cert = $_[0];
          return [$ca_cert, $ca_rsa, $cert, $rsa];
        });
      });
    });
    
    my $lsurl = $prepared->{https_local_url};
    my $sserver = tcp_server $lsurl->host->to_ascii, $lsurl->port, sub {
      my $args = [@_];
      $get_certs->then (sub {
        my ($ca_cert, $ca_rsa, $ee_cert, $ee_rsa) = @{$_[0]};
        $cv->begin;
        my $con = Web::Transport::ProxyServerConnection->new_from_aeargs_and_opts ($args, {
          tls => {
            ca_cert => $ca_cert->to_pem,
            cert => $ee_cert->to_pem,
            key => $ee_rsa->to_pem,
          },
          handle_request => $code,
        });
        $con->closed->finally (sub { $cv->end });
      });
    }; # $sserver

    $args{signal}->manakai_onabort (sub { $cv->end; undef $pserver; undef $sserver });
    die $args{signal}->manakai_error if $args{signal}->aborted;
    return [$data, Promise->from_cv ($cv)];
  })->catch (sub {
    my $e = $_[0];
    $args{signal}->manakai_onabort->();
    die $e;
  });
} # start

1;
