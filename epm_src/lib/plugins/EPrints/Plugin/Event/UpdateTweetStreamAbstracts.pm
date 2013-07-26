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

	#initialise state variables
	$self->{tweetstream_data} = {};
	$self->{cache_file} = $repo->config('archiveroot') . '/var/' . 'tweetstream_update.cache';

	if ($opts{update_from_zero} and -e $self->{cache_file})
	{
		#remove the cache
		unlink $self->{cache_file}; #perhaps we need exception handling here?
		$self->{update_from_zero} = 1;
	}


	$self->{profile_image_urls} = {};
	$self->{highest_tweetid} = 0; #don't iterate over anything higher than this (could mess up the cache)

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
	push @r, "Iterated over                  " . ($l->{iterate_tweet_count} ? $l->{iterate_tweet_count} : 0) . " tweet rows";
	push @r, "Iteration Low ID               " . ($l->{lowest_tweetid} ? $l->{lowest_tweetid} : 0);
	push @r, "Iteration High ID              " . $l->{highest_tweetid};
	push @r, "Started iteration at           " . $l->{iterate_start_time};
	push @r, "Finished iteration at          " . $l->{iterate_end_time};
	push @r, '';
	push @r, "Mysql queries started at:      " . $l->{queries_start_time};
	push @r, "Mysql queries finished at:     " . $l->{queries_end_time};
	push @r, '';
	my $size = $self->{log_data}->{start_cache_file_size};
	$size = 0 unless $size;
	push @r, "Cache size at start             $size (" . format_bytes($size) . ")";
	$size = $self->{log_data}->{end_cache_file_size};
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

	my $repo = $self->repository;
	my $db = $repo->database;

	$self->get_highest_tweetid(); #the first thing we do!
	if (!$self->{highest_tweetid})
	{
		$repo->log("Couldn't find highest tweet id\n");
	}
	$self->{log_data}->{highest_tweetid} = $self->{highest_tweetid};

	$self->get_tweetstream_counts;

	$self->initialise_wanted_streams;

	my @tsids = $self->tweetstreamids;
	$self->{log_data}->{tweetstreams_updated} = \@tsids;

	#don't read the counts from the cache.
	my $tmp_counts = $self->{tweetstream_data}->{context}->{counts};
	$self->read_cache;
	$self->{tweetstream_data}->{context}->{counts} = $tmp_counts;

	$self->update_tweet_counts; #replace cached counts with live counts in each tweetstream

	$self->{log_data}->{iterate_start_time} = scalar localtime time;
	$self->iterate_over_tweets; #walk all over the database (the hard bit) -- $self->{tweetstream_data}->{context}->{highest_seen_id} makes this only do the new stuff
	$self->{log_data}->{iterate_end_time} = scalar localtime time;

	$self->write_cache; #cache the hard bit

	$self->{log_data}->{queries_start_time} = scalar localtime time;
	$self->aggregate_with_queries; #direct queries -- pretty snappy!
	$self->{log_data}->{queries_end_time} = scalar localtime time;

	$self->prepare_tweetstream_data;

	foreach my $tweetstreamid ($self->tweetstreamids)
	{
		$self->update_tweetstream($tweetstreamid);
	}
}

sub get_highest_tweetid
{
	my ($self) = @_;

	my $sth = $self->run_query({
		'select' => ['MAX(tweetid)'],
		'from' => 'tweet',
	});
	$self->{highest_tweetid} = $sth->fetchrow_arrayref->[0];
}


sub update_tweet_counts
{
	my ($self) = @_;

	foreach my $tweetstreamid ($self->tweetstreamids)
	{
		$self->{tweetstream_data}->{$tweetstreamid}->{tweet_count} = 
			$self->{tweetstream_data}->{context}->{counts}->{$tweetstreamid};
	}
}

sub tweetstreamids
{
	my ($self) = @_;

	my @r = ();
	foreach my $id (keys %{$self->{tweetstream_data}})
	{
		next if $id eq 'context';
		push @r, $id;
	}
	return @r;
}

#overwrite anything in the active tweetstream_data hash to the cache (so we don't lose old data).
sub write_cache
{
	my ($self) = @_;
	my $repo = $self->repository;

	my $cache_file = $self->{cache_file};
	my $cache_data;
	if (-e $cache_file)
	{
		$cache_data = retrieve($cache_file);

		if (!defined $cache_data)
		{
			$repo->log("Error updating tweetstream.  Couldn't read from $cache_file\n");
		}
	}

	foreach my $k (keys %{$self->{tweetstream_data}})
	{
		$cache_data->{$k} = $self->{tweetstream_data}->{$k};
	}

	store($cache_data, $cache_file) or $repo->log("Error updating tweetstream.  Couldn't write to $cache_file\n");
	$self->{log_data}->{end_cache_file_size} = -s $cache_file;
}

#read from the cache and pull out data for any defined keys in $self->{tweetstream_data} (including the 'context' key)
sub read_cache
{
	my ($self) = @_;
	my $repo = $self->repository;

	$self->{log_data}->{cache_file_size} = 0;

	my $cache_file = $self->{cache_file};
	if (!-e $cache_file)
	{
		#if it doesn't exist, we'll just start from scratch 
		$self->{log_data}->{start_cache_file_size} = 0;
		return;
	}
	$self->{log_data}->{start_cache_file_size} = -s $cache_file;

	my $cache_data = retrieve($cache_file);

	if (!defined $cache_data)
	{
		$repo->log("Error updating tweetstream.  Couldn't read from $cache_file\n");
	}

	foreach my $tweetstreamid ($self->tweetstreamids)
	{
		$self->{tweetstream_data}->{$tweetstreamid} = $cache_data->{$tweetstreamid} if $cache_data->{$tweetstreamid};
	}
	$self->{tweetstream_data}->{context} = $cache_data->{context} if $cache_data->{context};
}


sub get_tweetstream_counts
{
	my ($self) = @_;

	my $sth = $self->run_query({'select' => ['tweetstreams', 'COUNT(*)'], from => 'tweet_tweetstreams', groupby => 'tweetstreams'});
	while (my $row = $sth->fetchrow_arrayref)
	{
		$self->{tweetstream_data}->{context}->{counts}->{$row->[0]} = $row->[1];
	}

	return if $self->{update_from_zero}; #don't set an old count -- we'll pretend we don't know it if we're updating from zero

	$sth = $self->run_query({'select' => ['tweetstreamid', 'tweet_count'], from => 'tweetstream'});
	while (my $row = $sth->fetchrow_arrayref)
	{
		$self->{tweetstream_data}->{context}->{oldcounts}->{$row->[0]} = $row->[1];
	}
}

sub initialise_wanted_streams
{
	my ($self) = @_;

	#if there's no old count, or if the old count and the cound differ, we'll need to update
	foreach my $tweetstreamid (keys %{$self->{tweetstream_data}->{context}->{counts}})
	{
		if (
			!$self->{tweetstream_data}->{context}->{oldcounts}->{$tweetstreamid}
		||
			(
				$self->{tweetstream_data}->{context}->{oldcounts}->{$tweetstreamid} != 
				$self->{tweetstream_data}->{context}->{counts}->{$tweetstreamid}
			)
		)
		{
			$self->{tweetstream_data}->{$tweetstreamid} = {}; #ready to accept data
		}
	}
}


sub iterate_over_tweets
{
	my ($self) = @_;

	#page over all data, aggregating as we go
	my $page_size = 100000;
	$self->{tweetstream_data}->{context}->{highest_seen_id} = 0 unless $self->{tweetstream_data}->{context}->{highest_seen_id};

	ITERATION: while (1) #we'll break out when we've processed the last page.
	{
		my $sth = $self->run_query({
			'select' => [qw/tweetid twitterid from_user profile_image_url created_at_year created_at_month created_at_day/],
			from => 'tweet',
			orderby => 'tweetid',
			where => "tweetid > " . $self->{tweetstream_data}->{context}->{highest_seen_id},
			limit =>$page_size,
		});

		#EXIT POINT -- if we have no more rows to process
		last ITERATION unless ($sth->rows);

		while (my $row = $sth->fetchrow_hashref)
		{
			my $tweetid = $row->{tweetid};

			#ANOTHER EXIT POINT (to ignore counting newly harvested items [subtle caching bug]
			last ITERATION if $tweetid > $self->{highest_tweetid};

			if (!$self->{log_data}->{lowest_tweetid})
			{
				$self->{log_data}->{lowest_tweetid} = $tweetid;
			}
			$self->{log_data}->{iterate_tweet_count}++;

			$self->{tweetstream_data}->{context}->{highest_seen_id} = $tweetid;
			my $tweetstreamids = $self->tweet_tweetstreams($tweetid);

			my $date = join('-',(
				sprintf("%04d",$row->{created_at_year}),
				sprintf("%02d",$row->{created_at_month}),
				sprintf("%02d",$row->{created_at_day})
			));

			foreach my $tweetstreamid (@{$tweetstreamids})
			{
				next unless $self->{tweetstream_data}->{$tweetstreamid}; #we're only interested in it if it's been initialised.
				$self->{tweetstream_data}->{$tweetstreamid}->{dates}->{$date}++;
				if ($row->{from_user})
				{
					$self->{tweetstream_data}->{$tweetstreamid}->{counts}->{top_from_users}->{$row->{from_user}}++;
					$self->{tweetstream_data}->{$tweetstreamid}->{profile_image_urls}->{$row->{from_user}}
						= $row->{profile_image_url} if $row->{profile_image_url};
				}
			}
		}
	#tidy data function seems not to be working.  We'll reevaluate depending on how big our data is.
		$self->tidy_data($self->{tweetstream_data});
	}
}

sub aggregate_with_queries
{
	my ($self) = @_;

	#operations over whole tweetstreams
	foreach my $tweetstreamid ($self->tweetstreamids)
	{
		#oldest and newest tweets
		$self->{tweetstream_data}->{$tweetstreamid}->{oldest_tweets} = $self->get_old_or_new($tweetstreamid, 'old');
		$self->{tweetstream_data}->{$tweetstreamid}->{newest_tweets} = $self->get_old_or_new($tweetstreamid, 'new');

	#top things (from multiple fields)
		foreach my $fieldid (qw/ hashtags tweetees urls_from_text /)
		{
			my $counts = $self->get_top_data_multiple($tweetstreamid, $fieldid);
			$self->{tweetstream_data}->{$tweetstreamid}->{counts}->{"top_$fieldid"} = $counts;
		}
	}

	#multiple column max counts for csv export.  Now global for performance
	foreach my $fieldname (qw/ hashtags tweetees urls_from_text /)
	{
		my $sth = $self->run_query({
			'select' => ['COUNT(*)'],
			'from' => "tweet_$fieldname",
			'groupby' => 'tweetid',
			'orderby' => 'COUNT(*) DESC',
			'limit' => 1,
		});
		my $v = $sth->fetchrow_arrayref->[0];
		foreach my $tweetstreamid ($self->tweetstreamids)
		{
			$self->{tweetstream_data}->{$tweetstreamid}->{$fieldname.'_ncols'} = $v;
		}
	}
}

sub prepare_tweetstream_data
{
	my ($self) = @_;

	#fill global $self->{profile_image_urls} var
	#not very pretty, but moving them to outside of a tweetstream context
	#makes preparing the data easier.
	foreach my $tweetstreamid ($self->tweetstreamids)
	{
		my $urls = delete ($self->{tweetstream_data}->{$tweetstreamid}->{profile_image_urls});
		foreach my $username (keys %{$urls})
		{
			$self->{profile_image_urls}->{$username} = $urls->{$username};
		}
	}
}



sub update_tweetstream
{
	my ($self, $tweetstreamid) = @_;
	my $repo = $self->repository;

	my $data = $self->{tweetstream_data}->{$tweetstreamid};

	my $ts = $repo->dataset('tweetstream')->dataobj($tweetstreamid);
	return unless $ts;

	foreach my $fieldname (qw/ newest_tweets oldest_tweets tweet_count hashtags_ncols tweetees_ncols urls_from_text_ncols /)
	{
		$ts->set_value($fieldname, $data->{$fieldname});
	}

	my ($period, $pairs) = $self->date_data_to_field_data($data->{dates});
	$ts->set_value('frequency_period',$period);
	$ts->set_value('frequency_values',$pairs);

	foreach my $fieldnamepart (qw/ hashtag from_user tweetee url_from_text /)
	{
		my $fieldname = "top_$fieldnamepart" . 's';
		#exception with bad choice of English.
		$fieldname = 'top_urls_from_text' if $fieldname eq 'top_url_from_texts';

		my $n = $repo->config('tweetstream_tops',$fieldname, 'n');
		my $val = $self->counts_to_field_data($fieldnamepart, $data->{counts}->{$fieldname},$n);
		$ts->set_value($fieldname, $val);
	}
	$ts->commit;
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
sub tidy_data
{
	my ($self) = @_;
	my $repo = $self->repository;

	foreach my $tweetstreamid ($self->tweetstreamids)
	{
		foreach my $count_id (keys %{$self->{tweetstream_data}->{$tweetstreamid}->{counts}})
		{
			my $n = $repo->config('tweetstream_tops',$count_id, 'n');
			$n = 50 unless $n;

			#how many shall we hold on to?  10% of the number of tweets + 10 times the number we will display.
			#bigger set for bigger streams and big enough sets for very small streams
			#this may need tweaking
			my $max = int($n*10);
			if ($self->{tweetstream_data}->{$tweetstreamid}->{tweet_count})
			{
				$max += int ($self->{tweetstream_data}->{$tweetstreamid}->{tweet_count} / 10);
			}
			next if scalar keys %{$self->{tweetstream_data}->{$tweetstreamid}->{counts}->{$count_id}} < $max; #if we have 1000 times as many as we need, we'll trim.

			my $new_counts = {};
			my $new_urls = {};
			my @sorted_keys =
			sort {
				$self->{tweetstream_data}->{$tweetstreamid}->{counts}->{$count_id}->{$b}
				<=>
				$self->{tweetstream_data}->{$tweetstreamid}->{counts}->{$count_id}->{$a}
			} keys %{$self->{tweetstream_data}->{$tweetstreamid}->{counts}->{$count_id}};
			#only keep 1000 times as much as we'll need
			foreach my $k ( @sorted_keys[0 .. ($max-1)] )
			{
				$new_counts->{$k} = $self->{tweetstream_data}->{$tweetstreamid}->{counts}->{$count_id}->{$k};
				if ($count_id eq 'top_from_users')
				{
					$new_urls->{$k} = $self->{tweetstream_data}->{$tweetstreamid}->{profile_image_urls}->{$k};
				}
			}
			$self->{tweetstream_data}->{$tweetstreamid}->{counts}->{$count_id} = $new_counts;
			if ($count_id eq 'top_from_users')
			{
				$self->{tweetstream_data}->{$tweetstreamid}->{profile_image_urls} = $new_urls;
			}
		}
	}	
}

sub get_top_data_multiple
{
	my ($self, $tweetstreamid, $fieldname) = @_;
	my $repo = $self->repository;

	my $n = $repo->config('tweetstream_tops',"top_$fieldname",'n');
	$n = 50 unless $n;

	my $args = {
		'select' => [ "tweet_$fieldname.$fieldname", "count(*)" ],
		from => "tweet_$fieldname LEFT JOIN tweet_tweetstreams ON tweet_$fieldname.tweetid = tweet_tweetstreams.tweetid",
		where => "tweet_tweetstreams.tweetstreams = $tweetstreamid",
		groupby => "tweet_$fieldname.$fieldname",
		orderby => 'count(*) desc',
		limit => $n,
	};

	my $sth = $self->run_query($args);
	my $r = {};

	while (my $row = $sth->fetchrow_arrayref)
	{
		$r->{$row->[0]} = $row->[1];
	}
	return $r;
}


#$choice should be 'old' or 'new'
sub get_old_or_new
{
	my ($self, $tweetstreamid, $choice) = @_;
	my $repo = $self->repository;

	my $n_oldest = $repo->config('tweetstream_tweet_renderopts','n_oldest');
	my $n_newest = $repo->config('tweetstream_tweet_renderopts','n_newest');

	#note hight limit as it's faster to sort by tweetid, but we need twitterid, which is approximately the same.
	#to be safe, we grab loads more than we need.
	my $sub_query_args = {
		'select' => ['tweet.tweetid', 'tweet.twitterid'],
		from => 'tweet LEFT JOIN tweet_tweetstreams ON tweet.tweetid = tweet_tweetstreams.tweetid',
		where => "tweet_tweetstreams.tweetstreams = $tweetstreamid",
		orderby => 'tweet_tweetstreams.tweetid',
		limit => 5000 + $n_oldest,
	};

	if ($choice eq 'new')
	{
		$sub_query_args->{orderby} = 'tweet_tweetstreams.tweetid DESC';
		$sub_query_args->{limit} = 5000 + $n_newest;
	}


	my $args = {
		'select' => ['foo.tweetid'],
		from => '(' . $self->build_sql($sub_query_args) . ') AS foo',
		orderby => 'foo.twitterid',
		'limit' => $n_oldest,
	};

	if ($choice eq 'new')
	{
		$args->{orderby} = 'foo.twitterid DESC';
		$args->{limit} = $n_newest;
	}

	my $sth = $self->run_query($args);
	
	my $arr = $self->query_to_arrayref($sth);

	#they will be in reverse order at this point, so re-reverse
	if ($choice eq 'new')
	{
		my @tmp = reverse @{$arr};
		$arr = \@tmp;
	}

	return $arr;
}

#the first column of the results will be returned as an arrayref
sub query_to_arrayref
{
	my ($self, $sth) = @_;
	my $r = [];

	while (my $row = $sth->fetchrow_arrayref)
	{
		push @{$r}, $row->[0];
	}
	return $r;
}


sub tweet_tweetstreams
{
	my ($self, $tweetid) = @_;

	my $sth = $self->run_query({'select' => ['tweetstreams'], from => 'tweet_tweetstreams', where => "tweetid = $tweetid"});
	my $r = [];
	while (my $row = $sth->fetchrow_arrayref)
	{
		push @{$r}, $row->[0];
	}
	return $r;
}

sub run_query
{
	my ($self, $parts, $noise) = @_;
	my $db = $self->repository->database;

	my $sql = $self->build_sql($parts);

	print STDERR (localtime time) . "  Running $sql\n" if $noise;

	my $sth = $db->prepare( $sql );
	$sth->execute;
	return $sth;
}

sub build_sql
{
	my ($self, $parts) = @_;

	my $sql = 'SELECT ';
	$sql .= join(', ',@{$parts->{'select'}});
	$sql .= ' FROM ';
	$sql .= $parts->{'from'};
	if ($parts->{where})
	{
		$sql .= ' WHERE ' . $parts->{'where'};
	}
	if ($parts->{groupby})
	{
		$sql .= ' GROUP BY ' . $parts->{groupby};
	}
	if ($parts->{orderby})
	{
		$sql .= ' ORDER BY ' . $parts->{orderby};
	}
	if ($parts->{limit})
	{
		$sql .= ' LIMIT ' . $parts->{limit};
	}
	return $sql;
}

1;
