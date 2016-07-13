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

	$self->{previous_run_incomplete} = 0;
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
				$repo->log( (ref $self) ."Couldn't created $blocked_by plugin");
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

		$self->refresh_lock; #make sure us sleeping doesn't cause us to time out.
		my $t = $self->{sleep_time};
		sleep $t; #wait for the block to clear
	}

}

sub output_status
{
        my ($self, @message) = @_;

        return unless ($self->{verbose} || $self->{status_log_file}) ;

        my $message = join('', @message);
        $message =~ s/\n/\n\t/g; #indent multiple lines
	$message = ( scalar localtime time ) . " -- $message\n";

	#quick and dirty -- there may be implication if multiple concurrant instances write to the same file.
	if ($self->{status_log_file})
	{
		open (my $fh, ">>", $self->{status_log_file})
			|| die "Couldn't open log file: " . $self->{status_log_file};
		print { $fh } $message;
		close $fh;
	}

	if ($self->{verbose})
	{
        	print STDERR scalar $message;
	}
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

sub refresh_lock
{
	my ($self) = @_;

	my $path = $self->lockfile;

	if (!-e $path)
	{
		$self->repository->log( (ref $self) ."attempting to refresh lock on nonexistant file $path");
		return;
	}

	#update the timestamp on the file
	my $atime = time;
	my $mtime = $atime;
	utime $atime, $mtime, $path;
}

#returns the lock timeout in seconds
sub lock_timeout
{
	my ($self) = @_;

	return $self->{lock_timeout} if $self->{lock_timeout};
	return undef;
}

sub lockfile_content
{
	my ($self) = @_;
	my $path = $self->lockfile;

	return undef if (!-e $path);

	open FILE, "<", $path || $self->repository->log( (ref $self) ."Could not open $path for " . ref $self . "\n");

	my @contents = (<FILE>);
	my ($datestamp, $lockfile_pid) = split(/[\n\t]/,$contents[0]);
	return { datestamp => $datestamp, pid => $lockfile_pid }
}

#explicit check for timeout for superclass explicitivity
#should be used immediately after an ->is_locked call.
sub has_timed_out_lock
{
	my ($self) = @_;

	my $path = $self->lockfile;
	return 0 if (!-e $path);

	my $timeout = $self->lock_timeout;
	return 0 unless defined $timeout;

	my $current_time = time; #current time
	my $modified_time = (stat($path))[9]; #get lastmod date

	if ( ( $current_time - $modified_time) > $timeout )
	{
		return 1;
	}
	return 0;
}

#use with caution.  Will kill the process id in the lock file
sub kill_locked_process
{
	my ($self) = @_;

	my $path = $self->lockfile;
	return 0 if (!-e $path);

	my $lockfile_content = $self->lockfile_content;
	my $lockfile_pid = $lockfile_content->{pid};

	my $pid = $$;

	return if $pid == $lockfile_pid; #pprevent suicide

	$self->repository->log( (ref $self) ."Killing $lockfile_pid");
	#kill the process
	return kill 'SIGKILL', $lockfile_pid;
}


#returns true if this process is locked
#Note -- side-effect, will remove the lock file of a previously crashed process
#Note -- second side-effect, will remove the lock file it's older than the objects lock_timeout
sub is_locked
{
	my ($self) = @_;

	my $path = $self->lockfile;
	return 0 if (!-e $path);

	my $lockfile_content = $self->lockfile_content;
	my $lockfile_pid = $lockfile_content->{pid};
	my $lockfile_datestamp = $lockfile_content->{datestamp};

	if (!$lockfile_pid || !$lockfile_datestamp)
	{
		$self->output_status( (ref $self) . ' Bad lock file!');
		return 0;
	}

	my $pid = $$;

	return 0 if $pid == $lockfile_pid; #otherwise we might look like we're locked when it's us that's running

	#kill(0) checks to see if we *can* kill the process.
	#if it returns true, then the process that created the lock is still running.
	my $alive = kill(0,$lockfile_pid);

	if (!$alive)
	{
		$self->repository->log( (ref $self) ."Found old lock file at $path, with nonexitant processid.  --$lockfile_datestamp -> $lockfile_pid.  I am $pid, so I deleted the lock and continued (assume crashed process)");
		$self->remove_lock;
		$self->{previous_run_incomplete} = 1; #we might want the current run to take this into account
		return 0;
	}

	#process really is locked
	return 1;
}

sub create_lock
{
	my ($self) = @_;

	my $path = $self->lockfile;

	open FILE, ">", $path || $self->repository->log( (ref $self) ."Could not open $path for " . ref $self . "\n");

	#print a datestamp and the processid to the lock file
	my $pid = $$;
	print FILE join("\t",(scalar localtime time),$pid) ;

	close FILE;
}

sub remove_lock
{
	my ($self) = @_;

	my $path = $self->lockfile;
	unlink $path || $self->repository->log( (ref $self) ."Could unlink $path for " . ref $self . "\n");
}

1;
