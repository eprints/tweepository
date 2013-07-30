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

        return $self;
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

	print FILE $self->generate_log_string, "\n\n";

	close FILE;
}

#same filename and path used for locking and logging.  Just a different extension
sub _file_without_extension
{
	my ($self) = @_;

	my $classname = ref $self;
	$classname =~ m/Event::(.*)/;

	my $filename = EPrints::Utils::escape_filename( $1 );

	my $path = $self->repository->config('archiveroot') . '/var/' . $filename;
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
