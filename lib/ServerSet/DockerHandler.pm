package ServerSet::DockerHandler;
use strict;
use warnings;
use Time::HiRes qw(time);
use AbortController;
use Promise;
use Promised::Flow;
use Promised::Command;
use Promised::Command::Docker;
use Web::Host;
use Web::URL;

use ServerSet::DefaultHandler;
push our @ISA, qw(ServerSet::DefaultHandler);

my $dockerhost = Web::Host->parse_string
    (Promised::Command::Docker->dockerhost_host_for_container);
sub dockerhost () { $dockerhost }

sub init ($$$) {
  my ($class, $ss, $logs) = @_;
  return $ss->{_dh_check} ||= $class->check_container_reachability ($ss, $logs)->then (sub {
    $ss->{use_local} = ! $_[0];
    $logs->("actual_or_local: " . ($ss->{use_local} ? 'local' : 'actual'));
  });
} # init

sub new_from_params ($$$) {
  my ($class, $h_name, $params) = @_;
  return bless {handler_name => $h_name, params => $params,
                container_names => {}, logs => ''}, $class;
} # new_from_params

sub start ($$%) {
  my ($handler, $ss, %args) = @_;
  my $data = {};

  my $params = $handler->{params};
  my $statechange = $handler->onstatechange;

  my $stop_dockers = sub { };
  my $cleanup_invoked = 0;
  my $stop = sub {
    return Promise->all ([
      $stop_dockers->(),
      Promise->resolve->then (sub {
        return if $cleanup_invoked++;
        return (($params->{cleanup} or sub {})->($handler, $ss, \%args, $data)); # or throw
      }),
    ]);
  }; # $stop
  my $cancel_by_error = sub { };

  my $error;
  return Promise->resolve->then (sub {
    die $args{signal}->manakai_error if $args{signal}->aborted;
    return $params->{prepare}->($handler, $ss, \%args, $data); # or throw
  })->then (sub {
    my $service = $_[0];
    my $services = {};
    $services->{0} = $service if defined $service;
    return [{}, undef] unless keys %$services;

    my $logs = sub {
      my $v = $_[0];
      return unless defined $v;
      $v =~ s/^/$$: docker: /gm;
      $v .= "\x0A" unless $v =~ /\x0A\z/;
      if ($args{debug}) {
        warn $v;
      } else {
        $handler->{logs} .= $v;
      }
      return Promise->resolve->then (sub {
        ($params->{sniff_log} or sub { })->($handler, $ss, $v); # or throw
      })->catch (sub {
        my $e = $_[0] // 'Error detected';
        $error //= $e;
        $cancel_by_error->();
      });
    }; # $logs
    $args{signal}->manakai_onabort ($stop);

    my $dockers = {};
    $stop_dockers = sub {
      return Promise->all ([
        map { $_->stop } values %$dockers
      ])->finally (sub {
        $dockers = {};
      });
    }; # $stop_dockers

    my $context_name = __PACKAGE__ . '--' . time;
    return Promise->resolve->then (sub {
      return promised_for {
        die $args{signal}->manakai_error if $args{signal}->aborted;
        my $name = shift;
        my $d = $services->{$name};
        $logs->("$d->{image}...\n");
        my $container_name = $context_name . '--' . $name;
        $container_name =~ s/[^A-Za-z0-9_.-]/_/g;
        my $docker = Promised::Command::Docker->new (
          image => $d->{image},
          command => $d->{command},
          no_tty => $d->{no_tty},
          docker_run_options => [
            '--name' => $container_name,
            (map { ('-v', $_) } @{$d->{volumes} or []}),
            (map { ('-p', $_) } @{$d->{ports} or []}),
            (map { ('--user', $_) } grep { defined $_ } ($d->{user})),
            (map { ('-e', $_ . '=' . $d->{environment}->{$_}) } grep { defined $d->{environment}->{$_} } keys %{$d->{environment} or {}}),
            ($d->{net_host} ? ('--net=host') : ()),
            '--restart' => {
              any => 'always',
              'on-failure' => 'on-failure',
              none => 'no',
            }->{$d->{deploy}->{restart_policy}->{condition} || 'any'},
          ],
        );
        $docker->propagate_signal (1);
        $docker->signal_before_destruction ('TERM');
        $docker->logs ($logs);
        $dockers->{$name} = $docker;
        $handler->{container_names}->{$name} = $container_name;
        return $docker->start;
      } [keys %$services];
    })->then (sub {
      my $acs = [];
      $args{signal}->manakai_onabort (sub {
        $_->abort for @$acs;
        $stop->();
      });
      $cancel_by_error = sub {
        $_->abort for @$acs;
      };

      return Promise->resolve->then (sub {
        die $error if defined $error;
        die $args{signal}->manakai_error if $args{signal}->aborted;

        my $ac = AbortController->new;
        push @$acs, $ac;
        $statechange->($handler, 'waiting');
        ($params->{beforewait} or sub { })->($handler, $ss, \%args, $data, $ac->signal, $dockers->{0}); # or throw
      })->then (sub {
        die $error if defined $error;
        die $args{signal}->manakai_error if $args{signal}->aborted;

        my $ac = AbortController->new;
        push @$acs, $ac;
        $statechange->($handler, 'waiting');
        ($params->{wait} or sub { })->($handler, $ss, \%args, $data, $ac->signal); # or throw
      })->then (sub {
        die $error if defined $error;
        die $args{signal}->manakai_error if $args{signal}->aborted;

        my $ac = AbortController->new;
        push @$acs, $ac;
        $statechange->($handler, 'initing');
        ($params->{init} or sub { })->($handler, $ss, \%args, $data, $ac->signal); # or throw
      })->catch (sub {
        $_->abort for @$acs;
        $acs = [];
        die $_[0];
      });
    })->then (sub {
      die $error if defined $error;
      die $args{signal}->manakai_error if $args{signal}->aborted;

      my ($r_s, $s_s) = promised_cv;
      $args{signal}->manakai_onabort (sub {
        $s_s->($stop->());
      });
      $cancel_by_error = sub {
        $s_s->(undef);
      };
      return [$data, $r_s];
    });
  })->catch (sub {
    my $e = $error // $_[0];
    $handler->{logs} .= "\n\n->start failed: |$e|";
    $args{signal}->manakai_onabort (sub { });
    $cancel_by_error = sub { };
    if ($args{allow_failure}) {
      $handler->{logs} .= "\nallow_failure is set; skip this server";
      return [{failed => 1}, Promise->resolve (undef)];
    }
    return $stop->()->catch (sub { })->then (sub { die $e });
  });
} # start

sub check_running ($) {
  my $self = shift;
  my $return = 1;
  my $names = [values %{$self->{container_names}}];
  return Promise->resolve->then (sub {
    return promised_until {
      my $container_name = shift @$names;
      return 'done' unless defined $container_name;
      my $cmd = Promised::Command->new ([
        'docker', 'top', $container_name,
      ]);
      $cmd->stdout (\(my $stdout = ''));
      $cmd->stderr (\(my $stderr = ''));
      return $cmd->run->then (sub { return $cmd->wait })->then (sub {
        my $result = $_[0];
        unless ($result->exit_code == 0) {
          $return = 0;
          return 'done';
        }
        return not 'done';
      });
    };
  })->then (sub {
    return $return;
  });
} # check_running

sub cat_file ($$) {
  my ($self, $path) = @_;
  my $return = {};
  return Promise->resolve->then (sub {
    my $names = [keys %{$self->{container_names}}];
    return promised_until {
      my $name = shift @$names;
      return 'done' unless defined $name;
      my $container_name = $self->{container_names}->{$name};
      my $cmd = Promised::Command->new ([
        'docker', 'exec', '-it', $container_name, 'cat', $path,
      ]);
      $cmd->stdout (\(my $stdout = ''));
      $cmd->stderr (\(my $stderr = ''));
      return $cmd->run->then (sub { return $cmd->wait })->then (sub {
        my $result = $_[0];
        if ($result->exit_code == 0) {
          $return->{$name} = $stdout;
        } else {
          $return->{$name} = undef;
        }
        return not 'done';
      });
    };
  })->then (sub {
    return $return;
  });
} # cat_file

sub check_container_reachability ($$$) {
  my ($class, $ss, $logs) = @_;

  my $port = 18080;
  
  my $context_name = __PACKAGE__ . '--' . time;
  my $container_name = $context_name . '--' . 'check_reachability';
  $container_name =~ s/[^A-Za-z0-9_.-]/_/g;
  
  my $docker = Promised::Command::Docker->new (
    image => 'quay.io/wakaba/docker-perl-app-base',
    command => ['/app/perl', '-MAnyEvent::Socket', '-e', q{$cv=AE::cv;$s=tcp_server "0",shift,sub{($fh)=@_;print$fh"HTTP/1.0 200 OK\x0D\x0A\x0D\x0A";$cv->send};close $fh;$cv->recv}, $port],
    docker_run_options => [
      '--name' => $container_name,
    ],
  );
  $docker->propagate_signal (1);
  $docker->signal_before_destruction ('KILL');
  return $docker->start->then (sub {
    return $docker->get_container_ipaddr;
  })->then (sub {
    my $ipaddr = shift;
    my $url = Web::URL->parse_string (qq<http://$ipaddr:$port>);
    return $ss->wait_for_http
        ($url, timeout => 10, name => 'container reachable');
  })->then (sub {
    return 1;
  }, sub {
    $logs->("check_container_reachability: |$_[0]|");
    return 0;
  })->finally (sub {
    return $docker->stop;
  });
} # check_container_reachability

1;
