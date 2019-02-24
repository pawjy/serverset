package ServerSet::DockerStack;
use strict;
use warnings;
our $VERSION = '1.0';
use Path::Tiny;
use File::Temp;
use Promise;
use Promised::Flow;
use Promised::Command;
use Promised::Command::Signals;
use Promised::Command::Docker;
use Promised::File;
use JSON::PS;

sub new ($$) {
  my ($class, $def) = @_;
  my $temp = File::Temp->newdir;
  return bless {
    def => $def,
    _temp => $temp, # files removed when destroyed
    temp_path => path ($temp)->absolute,
    self_pid => $$,
  }, $class;
} # new

sub logs ($;$) {
  if (@_ > 1) {
    $_[0]->{logs} = $_[1];
  }
  return $_[0]->{logs} // sub {
    my $v = $_[0];
    $v .= "\n" unless $v =~ /\n\z/;
    warn $v;
  };
} # logs

sub propagate_signal ($;$) {
  if (@_ > 1) {
    $_[0]->{propagate_signal} = $_[1];
  }
  return $_[0]->{propagate_signal};
} # propagate_signal

sub signal_before_destruction ($;$) {
  if (@_ > 1) {
    $_[0]->{signal_before_destruction} = $_[1];
  }
  return $_[0]->{signal_before_destruction};
} # signal_before_destruction

sub stack_name ($;$) {
  if (@_ > 1) {
    $_[0]->{stack_name} = $_[1];
  }
  return $_[0]->{stack_name} //= rand;
} # stack_name

sub use_fallback ($;$) {
  if (@_ > 1) {
    $_[0]->{use_fallback} = $_[1];
  }
  return $_[0]->{use_fallback};
} # use_fallback

sub start ($) {
  my $self = $_[0];
  
  return Promise->reject ("Already running") if $self->{running};

  $self->{running} = {};

  my $method = '_start_docker_stack';
  return Promise->resolve->then (sub {
    if ($self->use_fallback) {
      # XXX As docker stack's networking is unstable, we don't use it
      # for now.
      return $method = '_start_dockers';
      
      my $cmd = Promised::Command->new (['docker', 'stack']);
      $cmd->stdout (sub { });
      $cmd->stderr (sub { });
      return $cmd->run->then (sub { return $cmd->wait })->then (sub {
        my $result = $_[0];
        unless ($result->exit_code == 0) {
          $method = '_start_dockers';
        }
      });
    } # use_fallback
  })->then (sub {
    return $self->$method;
  });
} # start

sub _start_docker_stack ($) {
  my $self = $_[0];

  my $def = $self->{def};
  $def->{version} //= '3.3';

  my $logs = $self->logs;
  my $propagate = $self->propagate_signal;
  my $before = $self->signal_before_destruction;
  my $stack_name = $self->stack_name;

  my $compose_path = $self->{temp_path}->child ('docker-compose.yml');

  my $out;
  my $start = sub {
    $out = '';
    my $start_cmd = Promised::Command->new ([
      'docker', 'stack', 'deploy',
      '--compose-file', $compose_path,
      $stack_name,
    ]);
    my $stderr = '';
    $start_cmd->stdout (sub {
      my $w = $_[0];
      Promise->new (sub { $_[0]->($logs->($w)) }) if defined $w;
    });
    $start_cmd->stderr (sub {
      my $w = $_[0];
      $stderr .= $w if defined $w;
      Promise->new (sub { $_[0]->($logs->($w)) }) if defined $w;
    });
    $start_cmd->propagate_signal ($propagate);
    $start_cmd->signal_before_destruction ($before);
    return $start_cmd->run->then (sub {
      return $start_cmd->wait;
    })->then (sub {
      my $result = $_[0];
      if ($result->exit_code == 1 and
          $stderr =~ /^this node is not a swarm manager./) {
        return $result;
      }
      die $result unless $result->exit_code == 0;
      return undef;
    });
  }; # $start

  if ($self->{propagate_signal}) {
    for my $name (qw(INT TERM QUIT)) {
      $self->{signal_handlers}->{$name}
          = Promised::Command::Signals->add_handler ($name => sub {
              return $self->stop;
            });
    }
  }

  return Promised::File->new_from_path ($compose_path)->write_byte_string (perl2json_bytes $def)->then (sub {
    return $start->()->then (sub {
      my $error = $_[0];
      if (defined $error) {
        my $init_cmd = Promised::Command->new ([
          'docker', 'swarm', 'init',
          '--advertise-addr', '127.0.0.1',
        ]);
        return $init_cmd->run->then (sub {
          return $init_cmd->wait;
        })->then (sub {
          my $result = $_[0];
          die $result unless $result->exit_code == 0;
          return $start->()->then (sub {
            my $error = $_[0];
            die $error if defined $error;
          });
        });
      } # $error
    });
  })->catch (sub {
    my $e = $_[0];
    return $self->stop->then (sub { die $e }, sub { die $e });
  });
} # _start_docker_stack

sub _start_dockers ($) {
  my $self = $_[0];

  my $def = $self->{def};

  my $logs = $self->logs;
  my $propagate = $self->propagate_signal;
  my $before = $self->signal_before_destruction;

  $self->{dockers} = [];
  return ((promised_for {
    my $name = $_[0];
    my $d = $def->{services}->{$name};

    $logs->("$d->{image}...\n");
    my $docker = Promised::Command::Docker->new (
      image => $d->{image},
      command => $d->{command},
      docker_run_options => [
        (map { ('-v', $_) } @{$d->{volumes} or []}),
        (map { ('-p', $_) } @{$d->{ports} or []}),
        (map { ('--user', $_) } grep { defined $_ } ($d->{user})),
        (map { ('-e', $_ . '=' . $d->{environment}->{$_}) } keys %{$d->{environment} or {}}),
        '--restart' => {
          any => 'always',
          'on-failure' => 'on-failure',
          none => 'no',
        }->{$d->{deploy}->{restart_policy}->{condition} || 'any'},
      ],
    );
    $docker->propagate_signal ($propagate);
    $docker->signal_before_destruction ($before);
    $docker->logs ($logs);

    push @{$self->{dockers}}, $docker;
    return $docker->start;
  } [keys %{$def->{services}}])->catch (sub {
    my $e = $_[0];
    return $self->stop->then (sub { die $e });
  }));
} # _start_dockers

sub stop ($) {
  my $self = $_[0];
  return Promise->resolve unless $self->{running};

  if (defined $self->{dockers}) {
    return $self->{running}->{stop_dockers} ||= promised_cleanup {
      delete $self->{dockers};
      delete $self->{running};
    } Promise->all ([map {
      $_->stop;
    } @{$self->{dockers}}]);
  }

  my $stop_cmd = Promised::Command->new ([
    'docker', 'stack', 'rm', $self->stack_name,
  ]);
  return $self->{running}->{stop_docker_stack} ||= promised_cleanup {
    delete $self->{signal_handlers};
    delete $self->{running};
  } $stop_cmd->run->then (sub {
    return $stop_cmd->wait;
  })->then (sub {
    my $result = $_[0];
    die $result unless $result->exit_code == 0;
  })->catch (sub {
    my $error = $_[0];
    $self->logs->("Failed to stop the docker containers: $error\n");
  });
} # stop

sub DESTROY ($) {
  my $self = $_[0];
  if ($self->{running} and
      defined $self->{self_pid} and $self->{self_pid} == $$) {
    require Carp;
    warn "$$: $self is to be destroyed while the docker is still running", Carp::shortmess;
    if (defined $self->{signal_before_destruction}) {
      $self->stop;
    }
  }
} # DESTROY

1;

=head1 LICENSE

Copyright 2019 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
