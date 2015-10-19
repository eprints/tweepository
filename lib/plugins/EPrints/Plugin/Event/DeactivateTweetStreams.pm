package EPrints::Plugin::Event::DeactivateTweetStreams;

use EPrints::Plugin::Event::LockingEvent;
@ISA = qw( EPrints::Plugin::Event::LockingEvent );

use File::Path qw/ make_path /;
use Archive::Zip qw( :ERROR_CODES :CONSTANTS );
use File::Copy;
use JSON;

use strict;

#retire expired tweetstreams
sub action_deactivate_tweetstreams
{
	my ($self, %opts) = @_;

	$self->{verbose} = 1 if $opts{verbose};

	$self->{log_data}->{start_time} = scalar localtime time;

	if ($self->is_locked)
	{
		$self->repository->log( (ref $self) . " is locked.  Unable to run.\n");
		return;
	}
	$self->create_lock;

	my $repo = $self->repository;
	my $ts_ds = $self->repository->dataset('tweetstream');

	$self->output_status('Finding expired tweetstreams');

	my $expired_tweetstreamids = $self->expired_tweetstreamids;

	$self->output_status('Found ' . join(',',@{$expired_tweetstreamids}));

	foreach my $ts_id (@{$expired_tweetstreamids})
	{
		$self->wait;
		my $ts = $ts_ds->dataobj($ts_id);
		$ts->set_value('status', 'inactive');
		$ts->commit;
		$self->output_status("Setting $ts_id to inactive");

		push @{$self->{log_data}->{tweetstreams_deactivated}}, $ts_id;

		#remove package, if it exists -- this will be regenerated as the tweetstream is archived
		#20151011 -- actually, don't -- packages are removed when regenerated anyway
#		$ts->delete_export_package;
	}

	$self->{log_data}->{end_time} = scalar localtime time;
	$self->output_status("done");
	$self->write_log;
	$self->remove_lock;
}

sub expired_tweetstreamids 
{ 
        my ($self) = @_; 
        my $repo = $self->repository; 
        my $ts_ds = $repo->dataset('tweetstream'); 
 
        my $offset = 60*60*24*3; #5 days grace 
        my $deadline = EPrints::Time::get_iso_date(time - $offset); 
 
        my $search = $ts_ds->prepare_search; 
        $search->add_field( 
                $ts_ds->get_field( "expiry_date" ), 
                "-".$deadline );         
        $search->add_field( 
                $ts_ds->get_field( "status" ), 
                'active' );         
 
        my $results = $search->perform_search; 
 
        return $results->ids; 
}

sub generate_log_string
{
	my ($self) = @_;

	my $r = $self->{log_data}->{start_time} . ' to ' . $self->{log_data}->{end_time} . ': ';

	if ($self->{log_data}->{tweetstreams_deactivated} and scalar @{$self->{log_data}->{tweetstreams_deactivated}})
	{
		$r .= 'deactivated: ' . join(',',$self->{log_data}->{tweetstreams_deactivated});
	}
	else
	{
		$r .= 'nothing deactivated';
	}
	return $r . "\n";
}

1;
