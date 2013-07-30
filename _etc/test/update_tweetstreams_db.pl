#!/usr/bin/perl -I/opt/eprints3/perl_lib

use strict;
use warnings;
use EPrints;
use Date::Calc qw/ Week_of_Year Delta_Days Add_Delta_Days /;
use Storable qw/ store retrieve /;

my $ep = EPrints->new;
my $repo = $ep->repository('tweets');
my $db = $repo->database;

my $cache_file = $repo->config('archiveroot') . '/var/' . 'tweetstream_update.cache';

#GLOBAL VARS
my $GLOBAL_tweetstream_data = {};
my $GLOBAL_profile_image_urls;
my $GLOBAL_highest_tweetid = 0; #don't iterate over anything higher than this (could mess up the cache)

get_highest_tweetid(); #the first thing we do!
exit "Couldn't find highest tweet id\n" unless $GLOBAL_highest_tweetid;

get_tweetstream_counts();

initialise_wanted_streams();

#don't read the counts from the cache.
my $tmp_counts = $GLOBAL_tweetstream_data->{context}->{counts};
read_cache();
$GLOBAL_tweetstream_data->{context}->{counts} = $tmp_counts;

update_tweet_counts(); #replace cached counts with live counts in each tweetstream

iterate_over_tweets(); #walk all over the database (the hard bit) -- $GLOBAL_tweetstream_data->{context}->{highest_seen_id} makes this only do the new stuff

write_cache(); #cache the hard bit

aggregate_with_queries(); #direct queries -- pretty snappy!

prepare_tweetstream_data();


foreach my $tweetstreamid (tweetstreamids())
{
	update_tweetstream($tweetstreamid);
}

print STDERR (localtime time) . "All finished!\n";
exit;

sub get_highest_tweetid
{
	my $sth = run_query({
		'select' => ['MAX(tweetid)'],
		'from' => 'tweet',
	});
	$GLOBAL_highest_tweetid = $sth->fetchrow_arrayref->[0];
}


sub update_tweet_counts
{
	foreach my $tweetstreamid (tweetstreamids())
	{
		$GLOBAL_tweetstream_data->{$tweetstreamid}->{tweet_count} = 
			$GLOBAL_tweetstream_data->{context}->{counts}->{$tweetstreamid};
	}
}

sub tweetstreamids
{
	my @r = ();
	foreach my $id (keys %{$GLOBAL_tweetstream_data})
	{
		next if $id eq 'context';
		push @r, $id;
	}
	return @r;
}

#overwrite anything in the active tweetstream_data hash to the cache (so we don't lose old data).
sub write_cache
{
	my $cache_data;
	if (-e $cache_file)
	{
		$cache_data = retrieve($cache_file);

		if (!defined $cache_data)
		{
			$repo->log("Error updating tweetstream.  Couldn't read from $cache_file\n");
			exit;
		}
	}

	foreach my $k (keys %{$GLOBAL_tweetstream_data})
	{
		$cache_data->{$k} = $GLOBAL_tweetstream_data->{$k};
	}

	store($cache_data, $cache_file) or $repo->log("Error updating tweetstream.  Couldn't write to $cache_file\n");
}

#read from the cache and pull out data for any defined keys in $GLOBAL_tweetstream_data (including the 'context' key)
sub read_cache
{
	return unless -e $cache_file; #if it doesn't exist, we'll just start from scratch 

	my $cache_data = retrieve($cache_file);

	if (!defined $cache_data)
	{
		$repo->log("Error updating tweetstream.  Couldn't read from $cache_file\n");
		exit;
	}

	foreach my $tweetstreamid (tweetstreamids())
	{
		$GLOBAL_tweetstream_data->{$tweetstreamid} = $cache_data->{$tweetstreamid} if $cache_data->{$tweetstreamid};
	}
	$GLOBAL_tweetstream_data->{context} = $cache_data->{context} if $cache_data->{context};
}


sub get_tweetstream_counts
{
	my $sth = run_query({'select' => ['tweetstreams', 'COUNT(*)'], from => 'tweet_tweetstreams', groupby => 'tweetstreams'}, 1);
	while (my $row = $sth->fetchrow_arrayref)
	{
		$GLOBAL_tweetstream_data->{context}->{counts}->{$row->[0]} = $row->[1];
	}

	$sth = run_query({'select' => ['tweetstreamid', 'tweet_count'], from => 'tweetstream'}, 1);
	while (my $row = $sth->fetchrow_arrayref)
	{
		$GLOBAL_tweetstream_data->{context}->{oldcounts}->{$row->[0]} = $row->[1];
	}
}

sub initialise_wanted_streams
{
	#if there's no old count, or if the old count and the cound differ, we'll need to update
	foreach my $tweetstreamid (keys %{$GLOBAL_tweetstream_data->{context}->{counts}})
	{
		if (
			!$GLOBAL_tweetstream_data->{context}->{oldcounts}->{$tweetstreamid}
		||
			(
				$GLOBAL_tweetstream_data->{context}->{oldcounts}->{$tweetstreamid} != 
				$GLOBAL_tweetstream_data->{context}->{counts}->{$tweetstreamid}
			)
		)
		{
print STDERR "Initialising Tweetsteam $tweetstreamid\n";
			$GLOBAL_tweetstream_data->{$tweetstreamid} = {}; #ready to accept data
		}
	}
}


sub iterate_over_tweets
{
	#page over all data, aggregating as we go
	my $page_size = 100000;
	$GLOBAL_tweetstream_data->{context}->{highest_seen_id} = 0 unless $GLOBAL_tweetstream_data->{context}->{highest_seen_id};

	ITERATION: while (1) #we'll break out when we've processed the last page.
	{
		my $sth = run_query({
			'select' => [qw/tweetid twitterid from_user profile_image_url created_at_year created_at_month created_at_day/],
			from => 'tweet',
			orderby => 'tweetid',
			where => "tweetid > " . $GLOBAL_tweetstream_data->{context}->{highest_seen_id},
			limit =>$page_size,
		},1);

		#EXIT POINT -- if we have no more rows to process
		last ITERATION unless ($sth->rows);

		while (my $row = $sth->fetchrow_hashref)
		{
			my $tweetid = $row->{tweetid};

			#ANOTHER EXIT POINT (to ignore counting newly harvested items [subtle caching bug]
			last ITERATION if $tweetid > $GLOBAL_highest_tweetid;

			$GLOBAL_tweetstream_data->{context}->{highest_seen_id} = $tweetid;
			my $tweetstreamids = tweet_tweetstreams($tweetid);

			my $date = join('-',(
				sprintf("%04d",$row->{created_at_year}),
				sprintf("%02d",$row->{created_at_month}),
				sprintf("%02d",$row->{created_at_day})
			));

			foreach my $tweetstreamid (@{$tweetstreamids})
			{
				next unless $GLOBAL_tweetstream_data->{$tweetstreamid}; #we're only interested in it if it's been initialised.
				$GLOBAL_tweetstream_data->{$tweetstreamid}->{dates}->{$date}++;
				if ($row->{from_user})
				{
					$GLOBAL_tweetstream_data->{$tweetstreamid}->{counts}->{top_from_users}->{$row->{from_user}}++;
					$GLOBAL_tweetstream_data->{$tweetstreamid}->{profile_image_urls}->{$row->{from_user}}
						= $row->{profile_image_url} if $row->{profile_image_url};
				}
			}
		}
	#tidy data function seems not to be working.  We'll reevaluate depending on how big our data is.
		tidy_data($GLOBAL_tweetstream_data);
	}
}

sub aggregate_with_queries
{
	#operations over whole tweetstreams
	foreach my $tweetstreamid (tweetstreamids())
	{
		#oldest and newest tweets
		$GLOBAL_tweetstream_data->{$tweetstreamid}->{oldest_tweets} = get_old_or_new($tweetstreamid, 'old');
		$GLOBAL_tweetstream_data->{$tweetstreamid}->{newest_tweets} = get_old_or_new($tweetstreamid, 'new');

	#top things (from multiple fields)
		foreach my $fieldid (qw/ hashtags tweetees urls_from_text /)
		{
			my $counts = get_top_data_multiple($tweetstreamid, $fieldid);
			$GLOBAL_tweetstream_data->{$tweetstreamid}->{counts}->{"top_$fieldid"} = $counts;
		}
	}

	#multiple column max counts for csv export.  Now global for performance
	foreach my $fieldname (qw/ hashtags tweetees urls_from_text /)
	{
		my $sth = run_query({
			'select' => ['COUNT(*)'],
			'from' => "tweet_$fieldname",
			'groupby' => 'tweetid',
			'orderby' => 'COUNT(*) DESC',
			'limit' => 1,
		},1);
		my $v = $sth->fetchrow_arrayref->[0];
		foreach my $tweetstreamid (tweetstreamids())
		{
			$GLOBAL_tweetstream_data->{$tweetstreamid}->{$fieldname.'_ncols'} = $v;
		}
	}
}

sub prepare_tweetstream_data
{
	#fill global $GLOBAL_profile_image_urls var
	foreach my $tweetstreamid (tweetstreamids())
	{
		my $urls = delete ($GLOBAL_tweetstream_data->{$tweetstreamid}->{profile_image_urls});
		foreach my $username (keys %{$urls})
		{
			$GLOBAL_profile_image_urls->{$username} = $urls->{$username};
		}
	}
}



sub update_tweetstream
{
	my ($tweetstreamid) = @_;

	my $data = $GLOBAL_tweetstream_data->{$tweetstreamid};

print STDERR (localtime time) . "Updating $tweetstreamid\n";
	my $ts = $repo->dataset('tweetstream')->dataobj($tweetstreamid);
	return unless $ts;

	foreach my $fieldname (qw/ newest_tweets oldest_tweets tweet_count hashtags_ncols tweetees_ncols urls_from_text_ncols /)
	{
		$ts->set_value($fieldname, $data->{$fieldname});
	}

	my ($period, $pairs) = date_data_to_field_data($data->{dates});
	$ts->set_value('frequency_period',$period);
	$ts->set_value('frequency_values',$pairs);

	foreach my $fieldnamepart (qw/ hashtag from_user tweetee url_from_text /)
	{
		my $fieldname = "top_$fieldnamepart" . 's';
		#exception with bad choice of English.
		$fieldname = 'top_urls_from_text' if $fieldname eq 'top_url_from_texts';

		my $n = $repo->config('tweetstream_tops',$fieldname, 'n');
		my $val = counts_to_field_data($fieldnamepart, $data->{counts}->{$fieldname},$n);
		$ts->set_value($fieldname, $val);
	}
	$ts->commit;
}

sub date_data_to_field_data
{
	my ($date_counts) = @_;

	my @sorted_dates = sort {$a cmp $b} keys %{$date_counts};

	my $first = $sorted_dates[0];
	my $last = $sorted_dates[$#sorted_dates];

	return (undef,undef) unless ($first && $last); #we won't bother generating graphs based on hours or minutes

	my $delta_days = Delta_Days(parse_datestring($first),parse_datestring($last));

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

	initialise_date_structures($label_values, $pairs, $first, $last, $period);

	foreach my $date (@sorted_dates)
	{
		my $label = YMD_to_label(parse_datestring($date), $period);
		$label_values->{$label}->{value} += $date_counts->{$date};
	}

	return ($period, $pairs);
}	

sub initialise_date_structures
{
	my ($label_values, $pairs, $first_date, $last_date, $period) = @_;

	my $current_date = $first_date;
	my $current_label = YMD_to_label(parse_datestring($current_date),$period);
	my $last_label = YMD_to_label(parse_datestring($last_date),$period);

	my ($year, $month, $day) = parse_datestring($first_date);

	while ($current_label ne $last_label)
	{
		$label_values->{$current_label}->{label} = $current_label;
		$label_values->{$current_label}->{value} = 0;
		push @{$pairs}, $label_values->{$current_label};

		($year, $month, $day, $current_label) = next_YMD_and_label($year, $month, $day, $current_label, $period);
	}

	$label_values->{$last_label}->{label} = $last_label;
	$label_values->{$last_label}->{value} = 0;
	push @{$pairs}, $label_values->{$last_label};
}

sub next_YMD_and_label
{
	my ($year, $month, $day, $label, $period) = @_;

	my $new_label = $label;

	while ($new_label eq $label)
	{
		($year, $month, $day) = Add_Delta_Days ($year, $month, $day, 1);
		$new_label = YMD_to_label($year, $month, $day, $period);
	}
	return ($year, $month, $day, $new_label);
}

sub YMD_to_label
{
	my ($year, $month, $day, $period) = @_;

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
        my ($date) = @_;

        my ($year,$month,$day) = split(/[- ]/,$date);
        return ($year,$month,$day);
}


#takes a hashref of the form { 'foo' => 403, 'bar' => 600 ...}
#returns an ordered arrayref of the form [ { 'fieldid' => 'foo', count => '403', } ...]
#size is an optional argument that will trim the array to a specific size
sub counts_to_field_data
{
	my ($fieldid, $data, $size) = @_;

	my @r;
	foreach my $k (sort {$data->{$b} <=> $data->{$a}} keys %{$data})
	{
		my $h = { $fieldid => $k, 'count' => $data->{$k} };
		if ($fieldid eq 'from_user')
		{
			$h->{profile_image_url} = $GLOBAL_profile_image_urls->{$k};
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
	foreach my $tweetstreamid (tweetstreamids())
	{
		foreach my $count_id (keys %{$GLOBAL_tweetstream_data->{$tweetstreamid}->{counts}})
		{
			my $n = $repo->config('tweetstream_tops',$count_id, 'n');
			$n = 50 unless $n;

			#how many shall we hold on to?  10% of the number of tweets + 10 times the number we will display.
			#bigger set for bigger streams and big enough sets for very small streams
			#this may need tweaking
			my $max = int($n*10);
			if ($GLOBAL_tweetstream_data->{$tweetstreamid}->{tweet_count})
			{
				$max += int ($GLOBAL_tweetstream_data->{$tweetstreamid}->{tweet_count} / 10);
			}
			next if scalar keys %{$GLOBAL_tweetstream_data->{$tweetstreamid}->{counts}->{$count_id}} < $max; #if we have 1000 times as many as we need, we'll trim.

			my $new_counts = {};
			my $new_urls = {};
			my @sorted_keys =
			sort {
				$GLOBAL_tweetstream_data->{$tweetstreamid}->{counts}->{$count_id}->{$b}
				<=>
				$GLOBAL_tweetstream_data->{$tweetstreamid}->{counts}->{$count_id}->{$a}
			} keys %{$GLOBAL_tweetstream_data->{$tweetstreamid}->{counts}->{$count_id}};
			#only keep 1000 times as much as we'll need
			foreach my $k ( @sorted_keys[0 .. ($max-1)] )
			{
				$new_counts->{$k} = $GLOBAL_tweetstream_data->{$tweetstreamid}->{counts}->{$count_id}->{$k};
				if ($count_id eq 'top_from_users')
				{
					$new_urls->{$k} = $GLOBAL_tweetstream_data->{$tweetstreamid}->{profile_image_urls}->{$k};
				}
			}
			$GLOBAL_tweetstream_data->{$tweetstreamid}->{counts}->{$count_id} = $new_counts;
			if ($count_id eq 'top_from_users')
			{
				$GLOBAL_tweetstream_data->{$tweetstreamid}->{profile_image_urls} = $new_urls;
			}
		}
	}	
}

sub get_top_data_multiple
{
	my ($tweetstreamid, $fieldname) = @_;

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

	my $sth = run_query($args, 1);
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
	my ($tweetstreamid, $choice) = @_;
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
		from => '(' . build_sql($sub_query_args) . ') AS foo',
		orderby => 'foo.twitterid',
		'limit' => $n_oldest,
	};

	if ($choice eq 'new')
	{
		$args->{orderby} = 'foo.twitterid DESC';
		$args->{limit} = $n_newest;
	}

	my $sth = run_query($args, 1);
	
	my $arr = query_to_arrayref($sth);

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
	my ($sth) = @_;
	my $r;

	while (my $row = $sth->fetchrow_arrayref)
	{
		push @{$r}, $row->[0];
	}
	return $r;
}


sub tweet_tweetstreams
{
	my ($tweetid) = @_;

	my $sth = run_query({'select' => ['tweetstreams'], from => 'tweet_tweetstreams', where => "tweetid = $tweetid"});
	my $r = [];
	while (my $row = $sth->fetchrow_arrayref)
	{
		push @{$r}, $row->[0];
	}
	return $r;
}

sub run_query
{
	my ($parts, $noise) = @_;

	my $sql = build_sql($parts);

print STDERR (localtime time) . "  Running $sql\n" if $noise;
	my $sth = $db->prepare( $sql );
	$sth->execute;
	return $sth;
}

sub build_sql
{
	my ($parts) = @_;

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


