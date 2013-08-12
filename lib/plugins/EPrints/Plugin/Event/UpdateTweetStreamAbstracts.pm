package EPrints::Plugin::Event::UpdateTweetStreamAbstracts;

use Date::Calc qw/ Week_of_Year Delta_Days Add_Delta_Days /;
use Storable qw/ store retrieve /;
use Number::Bytes::Human qw/ format_bytes /;
use EPrints::Plugin::Event::LockingEvent;

@ISA = qw( EPrints::Plugin::Event::LockingEvent );

use strict;


#opts
#
# update_from_zero --> deletes the cache and regenerates everything;
sub action_update_tweetstream_abstracts
{
	my ($self, %opts) = @_;

        $self->{log_data}->{start_time} = scalar localtime time;
	my $repo = $self->repository;

	if ($self->is_locked)
	{
		$self->repository->log( (ref $self) . " is locked.  Unable to run.\n");
		return;
	}
	$self->create_lock;

	$self->{cache_file} = $repo->config('archiveroot') . '/var/' . 'tweetstream_update.cache';

	if ($opts{update_from_zero})
	{
		#remove the cache
		unlink $self->{cache_file} if -e $self->{cache_file}; 
		$self->{update_from_zero} = 1;
	}

	if ($opts{verbose})
	{
		$self->{verbose} = 1;
	}

	#global cache of this update's profile_image_urls
	$self->{profile_image_urls} = {};

	$self->update_tweetstream_abstracts();

	$self->remove_lock;
        $self->{log_data}->{end_time} = scalar localtime time;
	$self->write_log;

}

sub generate_log_string
{
	my ($self) = @_;

	my $l = $self->{log_data};

	my @r;

	push @r, '===========================================================================';
	push @r, '';
        push @r, "Aggregation started at:        " . $l->{start_time};
	push @r, "Tweetstream abstracts updated  " . join(',',sort {$a <=> $b} @{$l->{tweetstreams_updated}});
	push @r, '';
	push @r, "Iterated over                  " . $l->{iterate_tweet_count} . " tweets";
	push @r, "Iteration Low ID               " . $l->{lowest_tweetid};
	push @r, "Iteration High ID              " . ( $l->{highest_tweetid} ? $l->{highest_tweetid} : 'none');
	push @r, "Started iteration at           " . $l->{iterate_start_time};
	push @r, "Finished iteration at          " . $l->{iterate_end_time};
	push @r, '';
	push @r, "Updating Objects started at    " . $l->{update_objects_start_time};
	push @r, "Updating Objects finished at   " . $l->{update_objects_end_time};
	push @r, "Number of sleeps while blocked " . $l->{update_tweetstreams_sleeps};
	push @r, '';
	push @r, '';
	push @r, '';
	my $size = $l->{start_cache_file_size};
	$size = 0 unless $size;
	push @r, "Cache size at start             $size (" . format_bytes($size) . ")";
	$size = $l->{end_cache_file_size};
	push @r, "Cache size at end               $size (" . format_bytes($size) . ")";
	push @r, '';
	push @r, "Aggregation finished at:       " . $l->{end_time};
	push @r, '';
	push @r, '===========================================================================';


	return join("\n", @r);
}


sub update_tweetstream_abstracts
{
	my ($self) = @_;

	$self->output_status('Starting to update abstracts');

	my $repo = $self->repository;
	my $tweet_ds = $repo->dataset('tweet');
	my $tweetstream_ds = $repo->dataset('tweetstream');

	my $high_id = $self->get_highest_tweetid(); #the first thing we do!
	if (!$high_id)
	{
		$repo->log("Couldn't find highest tweet id\n");
		$high_id = 1;
	}

	#set the low ID to the previous update's high ID
	my $low_id = $self->read_cache_data('highest_tweet_processed');
	$low_id = 0 unless $low_id;
	$low_id = 0 if $self->{update_from_zero}; #should be unneccesary as update_from_zero will remove the cache

	$low_id += 1; #start at the *next* ID
	$self->{log_data}->{lowest_tweetid} = $low_id;

	$self->{log_data}->{iterate_start_time} = scalar localtime time;

	$self->output_status("Iterating from $low_id to $high_id");

	my $page_size = 100000; #number of tweets we process before tidying the data
	my $i = 0;
	my $data = {};
	my $tweet_count = 0;
	foreach my $tweetid ($low_id..$high_id)
	{
		my $tweet = $tweet_ds->dataobj($tweetid);
		next unless $tweet;

		next unless $tweet->is_set('tweetstreams');

		$tweet_count++; #number of processed tweets, for logging
		$self->{log_data}->{highest_tweetid} = $tweetid; #highest ID processed, for logging

		my $tweet_data = $self->tweet_to_data($tweet);

		my $tsids = $tweet->value('tweetstreams');

		foreach my $tsid (@{$tsids})
		{
			$data->{$tsid} = {} if (!$data->{$tsid}); #make sure we have an entry in the has for this tweetstream 

			$self->merge_in($data->{$tsid}, $tweet_data);
		}


		#tidy the accumulated data after a page of tweets
		$i++;
		$self->output_status('10000 processed') if $i % 10000 == 0;
		if ($i > $page_size)
		{
			$self->output_status("Page completed.  Currently on id $tweetid");
			#remove the least significant bits of count data to cut down on memory use
			$self->tidy_tweetstream_data($data);
			$i = 0;
		}
	}
	#one more tidy, for the last page
	$self->tidy_tweetstream_data($data);
	$self->output_status('Iteration Complete, starting updating of dataobjs');

	$self->{log_data}->{iterate_end_time} = scalar localtime time;
	$self->{log_data}->{iterate_tweet_count} = $tweet_count;

	#cache the profile_image_urls stored in each tweetstream, as we will be merging new values with old ones.
	foreach my $tsid (sort keys %{$data})
	{
		my $tweetstream = $tweetstream_ds->dataobj($tsid);
		next unless $tweetstream;
		next unless $tweetstream->is_set('top_from_users');

		my $tfu = $tweetstream->value('top_from_users');
		foreach my $val (@{$tfu})
		{
			my $user = $val->{from_user};
			next if ($self->{profile_image_urls}->{$user});

			my $self->{profile_image_urls}->{$user} = $val->{profile_image_url};
		}
	}

	$self->{log_data}->{update_objects_start_time} = scalar localtime time;
	$self->{log_data}->{update_tweetstreams_sleeps} = 0;
	my @updated_tweetstreams;
	my $update_tweetstreams = $repo->plugin('Event::UpdateTweetStreams');
	foreach my $tsid (sort keys %{$data})
	{
		#prepare the data
		my $ts_data = $data->{$tsid};

		my $cached_ts_data = $self->read_cache_data('tweetstreams', $tsid);
		if ($cached_ts_data)
		{
			$self->merge_in($ts_data, $cached_ts_data);
		}

		#wait until update_tweetstreams has finished as it will also be writing to tweetstream objects
		while ($update_tweetstreams->is_locked)
		{
			$self->{log_data}->{update_tweetstreams_sleeps}++;
			sleep 10;
		}

		my $tweetstream = $tweetstream_ds->dataobj($tsid);
		next unless $tweetstream;

		push @updated_tweetstreams, $tsid;

		$self->update_tweetstream($tweetstream, $ts_data); 
	}
	$self->{log_data}->{tweetstreams_updated} = \@updated_tweetstreams;
	$self->{log_data}->{update_objects_end_time} = scalar localtime time;

	$self->output_status('Updating Complete, tidying up');

	foreach my $tsid (keys %{$data})
	{
		$self->write_cache_data($data->{$tsid}, 'tweetstreams', $tsid);
	}
	$self->write_cache_data($high_id, 'highest_tweet_processed');

	$self->tidy_cache;

	$self->write_cache;
	$self->output_status('Finished');
}


sub tweet_to_data
{
	my ($self, $tweet) = @_;

	my $repo = $self->repository;
	my $tweet_ds = $repo->dataset('tweet');

	#tweet fieldnames are keys, tweetstream fieldnames are values
	my $fieldmap = $repo->config('update_tweetstream_abstracts','fieldmap');

	my $data = {};

	#handle multiple and non-multiple simple fields
	foreach my $field (keys %{$fieldmap})
	{
		next unless $tweet->is_set($field);
		my $val = $tweet->value($field);

		if ($field eq 'created_at')
		{
			#convert from a datetime to a date
			my ($date, $time) = split(/ /, $val);
			$val = $date;
		}

		if ($field eq 'retweeted_status')
		{

		}

		if (ref $val eq 'ARRAY')
		{
			foreach my $v (@{$val})
			{
				$data->{$field}->{$v}++;
			}
		}
		else
		{
			$data->{$field}->{$val}++;
		}
	}

	#a bit of a hack, but store the profile_image_urls for each from user at the top level of the object
	my $from_user = $tweet->value('from_user');
	if ($from_user && !$self->{profile_image_urls}->{$from_user})
	{
		$self->{profile_image_urls}->{$from_user} = $tweet->value('profile_image_url');
	}

	return $data;
}

sub merge_in
{
	my ($self, $destination_hashref, $new_data_hashref) = @_;

	foreach my $data_category (keys %{$new_data_hashref})
	{
		foreach my $data_point (keys %{$new_data_hashref->{$data_category}})
		{
			$destination_hashref->{$data_category}->{$data_point} += $new_data_hashref->{$data_category}->{$data_point};
		}
	}
}

sub get_highest_tweetid
{
	my ($self) = @_;

	my $db = $self->repository->database;

	my $sql = 'SELECT MAX(tweetid) FROM tweet';

	my $sth = $db->prepare( $sql );

	$sth->execute;

	return $sth->fetchrow_arrayref->[0];
}

#remove data that is no longer needed (cached stuff for expired tweetstreams)
sub tidy_cache
{
	my ($self) = @_;

	my $expired_tsids = $self->expired_tweetstreamids;
	my $ts_cache = $self->read_cache_data('tweetstreams');

	foreach my $tsid (@{$expired_tsids})
	{
		if (exists $ts_cache->{$tsid})
		{
			delete $ts_cache->{$tsid};
		}
	}
}

sub expired_tweetstreamids
{
	my ($self) = @_;
	my $repo = $self->repository;
	my $ts_ds = $repo->dataset('tweetstream');

	my $offset = 60*60*24*3; #5 days before tweetstreams expire
	my $deadline = EPrints::Time::get_iso_date(time - $offset);

	my $search = $ts_ds->prepare_search;
	$search->add_field(
		$ts_ds->get_field( "expiry_date" ),
		"-".$deadline );	

	my $results = $search->perform_search;

	return $results->ids;
}

sub write_cache_data
{
	my ($self, $data, @path) = @_;

	if (!$self->{cache})
	{
		$self->load_cache;
	}

	$self->_insert_into_hashref($data, $self->{cache}, @path);
}

sub read_cache_data
{
	my ($self, @path) = @_;

	if (!$self->{cache})
	{
		$self->load_cache;
	}

	my $c = $self->{cache};

	foreach my $k (@path)
	{
		if (exists $c->{$k})
		{
			$c = $c->{$k};
		}
		else
		{
			return undef;
		}
	}
	return $c;
}


#write the cache to the disk
sub write_cache
{
	my ($self) = @_;
	my $repo = $self->repository;
	my $cache_file = $self->{cache_file};

	store($self->{cache}, $cache_file) or $repo->log("Error updating tweetstream.  Couldn't write to $cache_file\n");

	$self->{log_data}->{end_cache_file_size} = -s $cache_file;
}

sub _insert_into_hashref
{
	my ($self, $data, $hashref, @path) = @_;

	my $c = $hashref;
	my $last_key = pop @path;

	foreach my $k (@path)
	{
		if (!exists $c->{$k})
		{
			$c->{$k} = {};
		}
		$c = $c->{$k};
	}

	$c->{$last_key} = $data;
}

#read from the cache and pull out data for any defined keys in $self->{tweetstream_data} (including the 'context' key)
sub load_cache
{
	my ($self) = @_;
	my $repo = $self->repository;
	my $cache_file = $self->{cache_file};

	if (!-e $cache_file)
	{
		$self->{log_data}->{start_cache_file_size} = 0;
		$self->{cache} = {};
		return;
	}

	my $cache_data = retrieve($cache_file);

	if (!defined $cache_data)
	{
		$repo->log("Error updating tweetstream.  Couldn't read from $cache_file\n");
		$self->{log_data}->{start_cache_file_size} = 0;
		$self->{cache} = {};
		return;
	}
	
	$self->{log_data}->{start_cache_file_size} = -s $cache_file;
	$self->{cache} = $cache_data;
}

sub update_tweetstream
{
	my ($self, $tweetstream, $data) = @_;

	my $repo = $self->repository;

	#tweet fieldnames are keys, tweetstream fieldnames are values
	my $fieldmap = $repo->config('update_tweetstream_abstracts','fieldmap');

	foreach my $fieldname (keys %{$fieldmap})
	{
		if ($fieldname eq 'created_at')
		{
			my ($period, $pairs) = $self->date_data_to_field_data($data->{$fieldname});
			$tweetstream->set_value('frequency_period',$period);
			$tweetstream->set_value('frequency_values',$pairs);
		}
		else
		{
			my $ts_fieldname = $fieldmap->{$fieldname}->{fieldname};
			my $subname = $fieldmap->{$fieldname}->{subname};

			my $n = $repo->config('tweetstream_tops',$ts_fieldname, 'n');

			my $val = $self->counts_to_field_data($subname, $data->{$fieldname}, $n);

			$tweetstream->set_value($ts_fieldname, $val);
		}
	}
	$tweetstream->commit;
}

sub date_data_to_field_data
{
	my ($self, $date_counts) = @_;

	my @sorted_dates = sort {$a cmp $b} keys %{$date_counts};

	my $first = $sorted_dates[0];
	my $last = $sorted_dates[$#sorted_dates];

	return (undef,undef) unless ($first && $last); #we won't bother generating graphs based on hours or minutes
	my $delta_days = Delta_Days($self->parse_datestring($first),$self->parse_datestring($last));

	return (undef,undef) unless $delta_days; #we won't bother generating graphs based on hours or minutes

#maximum day delta in each period class
	my $thresholds = {
		daily => (30*1),
		weekly => (52*7),
		monthly => (48*30),
	};

	my $period = 'yearly';
	foreach my $period_candidate (qw/ monthly weekly daily /)
	{
		$period = $period_candidate if $delta_days <= $thresholds->{$period_candidate};
	}

	my $label_values = {};
	my $pairs = [];

	$self->initialise_date_structures($label_values, $pairs, $first, $last, $period);

	foreach my $date (@sorted_dates)
	{
		my $label = $self->YMD_to_label($self->parse_datestring($date), $period);
		$label_values->{$label}->{value} += $date_counts->{$date};
	}

	return ($period, $pairs);
}	

sub initialise_date_structures
{
	my ($self, $label_values, $pairs, $first_date, $last_date, $period) = @_;

	my $current_date = $first_date;
	my $current_label = $self->YMD_to_label($self->parse_datestring($current_date),$period);
	my $last_label = $self->YMD_to_label($self->parse_datestring($last_date),$period);

	my ($year, $month, $day) = $self->parse_datestring($first_date);

	while ($current_label ne $last_label)
	{
		$label_values->{$current_label}->{label} = $current_label;
		$label_values->{$current_label}->{value} = 0;
		push @{$pairs}, $label_values->{$current_label};

		($year, $month, $day, $current_label) = $self->next_YMD_and_label($year, $month, $day, $current_label, $period);
	}

	$label_values->{$last_label}->{label} = $last_label;
	$label_values->{$last_label}->{value} = 0;
	push @{$pairs}, $label_values->{$last_label};
}

sub next_YMD_and_label
{
	my ($self, $year, $month, $day, $label, $period) = @_;

	my $new_label = $label;

	while ($new_label eq $label)
	{
		($year, $month, $day) = Add_Delta_Days ($year, $month, $day, 1);
		$new_label = $self->YMD_to_label($year, $month, $day, $period);
	}
	return ($year, $month, $day, $new_label);
}

sub YMD_to_label
{
	my ($self, $year, $month, $day, $period) = @_;

	return $year if $period eq 'yearly';
	return join('-',(sprintf("%04d",$year), sprintf("%02d",$month))) if $period eq 'monthly';
	return join('-',(sprintf("%04d",$year), sprintf("%02d",$month),sprintf("%02d",$day))) if $period eq 'daily';

	if ($period eq 'weekly')
	{
		my ($week, $wyear) = Week_of_Year($year, $month, $day);
		return "Week $week, $wyear";
	}

	return undef;
}


sub parse_datestring
{
        my ($self, $date) = @_;

        my ($year,$month,$day) = split(/[- ]/,$date);
        return ($year,$month,$day);
}


#takes a hashref of the form { 'foo' => 403, 'bar' => 600 ...}
#returns an ordered arrayref of the form [ { 'fieldid' => 'foo', count => '403', } ...]
#size is an optional argument that will trim the array to a specific size
sub counts_to_field_data
{
	my ($self, $fieldid, $data, $size) = @_;

	my @r;
	foreach my $k (sort {$data->{$b} <=> $data->{$a}} keys %{$data})
	{
		my $h = { $fieldid => $k, 'count' => $data->{$k} };
		if ($fieldid eq 'from_user')
		{
			$h->{profile_image_url} = $self->{profile_image_urls}->{$k};
		}
		push @r, $h
	}

	if ($size && (scalar @r > $size))
	{
		my @n = @r[0 .. ($size-1)];
		@r = @n;
	}

	return \@r;
}

#throw away the data that probably doesn't matter as we're processing lots and don't want to hammer the ram.
sub tidy_tweetstream_data
{
	my ($self, $data) = @_;
	my $repo = $self->repository;
	my $ts_ds = $repo->dataset('tweetstream');
	my $fieldmap = $repo->config('update_tweetstream_abstracts','fieldmap');

	TWEETSTREAM: foreach my $ts_id (keys %{$data})
	{
		my $tweetstream = $ts_ds->dataobj($ts_id);
		next TWEETSTREAM unless $tweetstream;
		my $tweet_count = $tweetstream->value('tweet_count');
		my $ts_data = $data->{$ts_id};

		COUNTSET: foreach my $fieldname (keys %{$ts_data})
		{
			next COUNTSET unless $fieldmap->{$fieldname}->{tidy};

			my $counts = $ts_data->{$fieldname};
			my @values = keys %{$counts};

			#how many shall we hold on to?  10% of the number of tweets + 10 times the number we will display.
			#bigger set for bigger streams and big enough sets for very small streams
			#this may need tweaking
			my $n = $repo->config('tweetstream_tops',$fieldname, 'n');
			$n = 50 unless $n;
			my $max = $n * 10;
			$max += int ($tweet_count / 10);

			next COUNTSET unless scalar @values > $max;

			@values = sort { $ts_data->{$fieldname}->{$b} <=> $ts_data->{$fieldname}->{$a} } @values;

			my @to_remove = @values[$max..$#values]; #take from index $max to the end

			foreach my $key (@to_remove)
			{
				delete $counts->{$key};
				#if we're removing a user, also remove the user's image URL (stored at object level)
				if ($fieldname eq 'top_from_users')
				{
					delete $self->{profile_image_urls}->{$key};
				}
			}
		}
	}
}

1;
