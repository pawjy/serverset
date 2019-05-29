package ServerSet::DockerHandler;
use strict;
use warnings;
use Time::HiRes qw(time);
use AbortController;
use Promise;
use Promised::Flow;
use Promised::Command;
use Promised::Command::Docker;

use ServerSet::DefaultHandler;
push our @ISA, qw(ServerSet::DefaultHandler);

my $dockerhost = Web::Host->parse_string
    (Promised::Command::Docker->dockerhost_host_for_container);
sub dockerhost () { $dockerhost }

sub new_from_params ($$) {
  my ($class, $params) = @_;
  return bless {params => $params, container_names => [],
                logs => ''}, $class;
} # new_from_params

sub start ($$;%) {
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
  #return $ss->_start_docker_network->then (sub {
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

    my $dockers = [];
    $stop_dockers = sub {
      return Promise->all ([
        map { $_->stop } @$dockers
      ])->finally (sub {
        $dockers = [];
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
          docker_run_options => [
            '--name' => $container_name,
            (map { ('-v', $_) } @{$d->{volumes} or []}),
            (map { ('-p', $_) } @{$d->{ports} or []}),
            (map { ('--user', $_) } grep { defined $_ } ($d->{user})),
            (map { ('-e', $_ . '=' . $d->{environment}->{$_}) } keys %{$d->{environment} or {}}),
            #'--net' => $ss->docker_network_name,
            '--net' => 'host',
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
        push @$dockers, $docker;
        push @{$handler->{container_names}}, $container_name;
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
    $args{signal}->manakai_onabort (sub { });
    $cancel_by_error = sub { };
    return $stop->()->catch (sub { })->then (sub { die $e });
  });
} # start

sub check_running ($) {
  my $self = shift;
  my $return = 1;
  my $names = [@{$self->{container_names}}];
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

1;
