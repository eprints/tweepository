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

		#remove package, if it exists -- this will be regenerated as the tweetstream is archived
		$ts->delete_export_package;
	}

	$self->output_status("done");
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



1;
