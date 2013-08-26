package EPrints::Plugin::Event::LockingEvent;

use EPrints::Plugin::Event;
use Data::Dumper;
@ISA = qw( EPrints::Plugin::Event );

#superclass to provide repository level lock files for events
use strict;

sub new
{
        my( $class, %params ) = @_;

        my $self = $class->SUPER::new(%params);

        $self->{log_data} = {};
	$self->{sleep_time} = 10; #number of seconds to sleep while waiting

        return $self;
}

sub set_verbose
{
	my ($self, $verbose) = @_;

	$self->{verbose} = $verbose;
}

sub blocked_by
{
	my ($self) = @_;

	my $blocked_by = $self->repository->config('block_map', $self->eventname);

	$blocked_by = [] unless $blocked_by;

	return $blocked_by;

}

sub blocked_by_plugins
{
	my ($self) = @_;

	my $blocked_by = $self->blocked_by;

	my $blocked_by_events = [];
	foreach my $event (@{$blocked_by})
	{
		push @{$blocked_by_events}, "Event::$event";
	}
	return $blocked_by_events;
}

sub wait
{
	my ($self) = @_;
	my $repo = $self->repository;

	while (1)
	{
		my $blocked = 0;
		foreach my $blocked_by (@{$self->blocked_by_plugins})
		{
			my $plugin = $repo->plugin($blocked_by);
			if (!$plugin)
			{
				$repo->log("Couldn't created $blocked_by plugin");
				next;
			}
			if ($plugin->is_locked)
			{
				$self->output_status("Blocked by $blocked_by");
				$blocked = 1;
				$self->{log_data}->{blocked_count}->{$blocked_by}++;
			}
		}
		last unless $blocked;

		my $t = $self->{sleep_time};
		sleep $t; #wait for the block to clear
	}

}

sub output_status
{
        my ($self, @message) = @_;

        return unless $self->{verbose};

        my $message = join('', @message);
        $message =~ s/\n/\n\t/g; #indent multiple lines

        print STDERR scalar localtime time,' -- ', $message, "\n";
}


#should be overridden, but in case it isn't
sub generate_log_string
{
	my ($self) = @_;

	return Dumper $self->{log_data};
}

sub write_log
{
	my ($self) = @_;

	my $filename = $self->logfile;
	open FILE, ">>$filename";
	binmode FILE, ":utf8";

	print FILE $self->generate_log_string;

	if ($self->{log_data}->{blocked_count})
	{
		print FILE "\n";
		print FILE "Blocked by:\n";
		foreach my $process (keys %{$self->{log_data}->{blocked_count}})
		{
			print FILE "\t$process -> " . $self->{log_data}->{blocked_count}->{$process} * $self->{sleep_time} . " seconds";
		}
	}
	print FILE "\n\n";

	close FILE;
}

#same filename and path used for locking and logging.  Just a different extension
sub _file_without_extension
{
	my ($self) = @_;

	my $filename = EPrints::Utils::escape_filename( $self->eventname );

	my $path = $self->repository->config('archiveroot') . '/var/' . $filename;
}

sub eventname
{
	my ($self) = @_;

	my $classname = ref $self;
	$classname =~ m/Event::(.*)/;

	return $1;
}

sub logfile
{
	my ($self) = @_;

	return $self->_file_without_extension . '.log';
}

sub lockfile
{
	my ($self) = @_;

	return $self->_file_without_extension . '.lock';
}

#returns true if this process is locked
#Note -- side-effect, will remove the lock file of a previously crashed process
sub is_locked
{
	my ($self) = @_;

	my $path = $self->lockfile;

	if (-e $path)
	{
		my $pid = $$;

		open FILE, "<", $path || $self->repository->log("Could not open $path for " . ref $self . "\n");
		my @contents = (<FILE>);
		my ($datestamp, $lockfilepid) = split(/[\n\t]/,$contents[0]);

		#kill(0) checks to see if we *can* kill the process.
		#if it returns true, then the process that created the lock is still running.
		my $alive = kill(0,$lockfilepid);
		if ($alive)
		{
			return 1;
		}
		else
		{
			$self->repository->log("Found old lock file at $path, with nonexitant processid.  --$datestamp -> $lockfilepid.  I am $pid, so I deleted the lock and continued (assume crashed process)");
			$self->remove_lock;
			return 0;
		}

	}


	return 0;
}

sub create_lock
{
	my ($self) = @_;

	my $path = $self->lockfile;

	open FILE, ">", $path || $self->repository->log("Could not open $path for " . ref $self . "\n");

	#print a datestamp and the processid to the lock file
	my $pid = $$;
	print FILE join("\t",(scalar localtime time),$pid) ;

	close FILE;
}

sub remove_lock
{
	my ($self) = @_;

	my $path = $self->lockfile;
	unlink $path || $self->repository->log("Could unlink $path for " . ref $self . "\n");
}

1;
