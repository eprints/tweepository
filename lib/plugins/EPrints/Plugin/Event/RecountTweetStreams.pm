package EPrints::Plugin::Event::RecountTweetStreams;

use EPrints::Plugin::Event::LockingEvent;
@ISA = qw( EPrints::Plugin::Event::LockingEvent );

use strict;

sub action_recount_tweetstreams
{
	my ($self, $verbose) = @_;

	$self->{verbose} = 1 if $verbose;

	#don't start if update_tweetstreams is running
	$self->wait;

	if ($self->is_locked)
	{
		$self->repository->log( (ref $self) . " is locked.  Unable to run.");
		return;
	}
	$self->create_lock;

	$self->output_status('Checking on update_tweetstreams');

	$self->output_status('update_tweetstreams not running');

	$self->output_status('running query to get counts.  This may take some time...');

	my $counts = $self->get_tweetstream_counts;

	$self->output_status('got counts, applying to active tweetstreams');

	my $ts_ds = $self->repository->dataset('tweetstream');
	foreach my $tweetstreamid (keys %{$counts})
	{
		my $ts = $ts_ds->dataobj($tweetstreamid);
		next unless $ts;

		if ($ts->value('status') eq 'active')
		{
			my $count = $counts->{$tweetstreamid};
			$self->output_status("Updating tweetstream $tweetstreamid to count $count");
			$ts->set_value('tweet_count', $count);
			$ts->commit;
		}
	}

	$self->remove_lock;
}

sub get_tweetstream_counts
{
	my ($self) = @_;

	my $db = $self->repository->database;

	my $counts = {};

	my $sql = 'SELECT tweetstreams, COUNT(*) FROM tweet_tweetstreams GROUP BY tweetstreams';

	my $sth = $db->prepare( $sql );
	$sth->execute;

	while (my $row = $sth->fetchrow_arrayref)
	{
		$counts->{$row->[0]} = $row->[1];
	}

	return $counts;
}


1;
