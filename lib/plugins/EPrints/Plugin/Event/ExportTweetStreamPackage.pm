package EPrints::Plugin::Event::ExportTweetStreamPackage;

use EPrints::Plugin::Event::LockingEvent;
@ISA = qw( EPrints::Plugin::Event::LockingEvent );

use File::Path qw/ make_path /;
use Archive::Zip;
use File::Copy;

use strict;

sub _initialise_constants
{
	my ($self) = @_;

	$self->{search_page_size} = 25000; #page size 
	$self->{max_per_file}->{csv} = 10000;
	$self->{max_per_file}->{json} = 10000;

}

sub action_export_tweetstream_packages
{
	my ($self, @ids) = @_;

        $self->{log_data}->{start_time} = scalar localtime time;

	my $repo = $self->repository;

	if ($self->is_locked)
	{
		$self->repository->log( (ref $self) . " is locked.  Unable to run.\n");
		return;
	}
	$self->create_lock;

	$self->_initialise_constants();

	foreach my $id (@ids)
	{
		my $ts = $repo->dataset('tweetstream')->dataobj($id);
		next unless $ts;
		if ($ts->value('status') ne 'archived')
		{
			$self->export_single_tweetstream($ts);
		}
	}

	$self->remove_lock;
        $self->{log_data}->{end_time} = scalar localtime time;
	$self->write_log;
}


sub action_export_queued_tweetstream_packages
{
	my ($self) = @_;

        $self->{log_data}->{start_time} = scalar localtime time;

	my $repo = $self->repository;

	if ($self->is_locked)
	{
		$self->repository->log( (ref $self) . " is locked.  Unable to run.\n");
		return;
	}
	$self->create_lock;

	my $ds = $repo->dataset('tsexport');

	#tidy up after possible previos crash (set all 'running' to 'pending')
	my @exports = $ds->search( filters => [ {
                meta_fields => [qw( status )],
                value => 'running',
        },] )->get_records;
	foreach my $export(@exports)
	{
		$export->set_value('status','pending');
		$export->commit;
	}

	my $pending_count = $ds->search( filters => [ {
                meta_fields => [qw( status )],
                value => 'pending',
        },] )->count;

	$self->_initialise_constants();

	if ($pending_count >= 1)
	{
		$self->export_requested_tweetstreams;
	}

	$self->remove_lock;
        $self->{log_data}->{end_time} = scalar localtime time;
	$self->write_log;

}



sub export_requested_tweetstreams
{
	my ($self) = @_;

	my $repo = $self->repository;

	my $export_ds = $repo->dataset('tsexport');

	#process a maximum of 100 records in this run.  Leave more for the next run
	my @exports = $export_ds->search( filters => [ {
                meta_fields => [qw( status )],
                value => 'pending',
        },] )->get_records(0,100);

	my $done_timestamps = {};
	foreach my $export (@exports)
	{
		my $tsid = $export->value('tweetstream');
		if (!$done_timestamps->{$tsid})
		{
			$export->set_value('status','running');
			$export->commit();
			my $ts = $repo->dataset('tweetstream')->dataobj($tsid) if $tsid;
			if
			(
				$ts 
				&& ($ts->value('status') eq 'active') #only export active tweetstreams
			)
			{
				$self->export_single_tweetstream($ts);
			}
		}
		$done_timestamps->{$tsid} = EPrints::Time::get_iso_timestamp();
		#it either failed, succeeded or was a dupliate
		$export->set_value('status','finished');
		$export->set_value('date_completed',  $done_timestamps->{$tsid});
		$export->commit;
	}
}

sub _generate_sql_query
{
	my ($self, $order_field, $tsid) = @_;

	#we don't care about ordering 
	if ($order_field eq 'tweetid')
	{
		return "SELECT tweetid FROM tweet_tweetstreams WHERE tweetstreams = $tsid AND tweetid > ? ORDER BY tweetid LIMIT " . $self->{search_page_size};
	}
	
	#order_field is twitterid
	my @parts;
	push @parts, 'SELECT tweet.tweetid, tweet.twitterid';
	push @parts, 'FROM tweet JOIN tweet_tweetstreams ON tweet.tweetid = tweet_tweetstreams.tweetid';
	push @parts, "WHERE tweet_tweetstreams.tweetstreams = $tsid";
	push @parts, "AND tweet.twitterid > ?";
	push @parts, "ORDER BY tweet.twitterid ";
	push @parts, 'LIMIT ' . $self->{search_page_size};

	return join(' ',@parts);
}

sub reset_tmp_dir
{
	my ($self) = @_;

	$self->{tmp_dir} = File::Temp->newdir( "ep-ts-export-tempXXXXX", TMPDIR => 1 );
}


#unordered is a flag that's set if the content is not ordered
sub create_fh
{
	my ($self, $type, $page, $unordered) = @_;

	if (!$self->{tmp_dir})
	{
		$self->reset_tmp_dir;
	}
	my $base_dir = $self->{tmp_dir};

	my $filename;
	if ($type eq 'tweetstreamXML')
	{
		$filename = $base_dir . '/tweetstream.xml'; 
	}
	else
	{
		$filename = $base_dir . '/tweets' . sprintf("%04d",$page);
		$filename .= '-unordered' if $unordered;
		$filename .= ".$type";
	}

	open (my $fh, ">:encoding(UTF-8)", $filename) or die "cannot open $filename for writing: $!";

	return $fh;
}

sub initialise_file
{
	my ($self, $type) = @_;

	my $fh = $self->{files}->{$type}->{filehandle};
	if ($type eq 'csv')
	{
		my $ts = $self->{current_tweetstream};
		print $fh EPrints::Plugin::Export::TweetStream::CSV::csv_headings($ts);	
	}
	elsif ($type eq 'json')
	{
		print $fh "{\n  \"tweets\": [\n"; 
	}

}

sub close_file
{
	my ($self, $type) = @_;

	my $fh = $self->{files}->{$type}->{filehandle};
	return unless $fh;

	if ($type eq 'json')
	{
		print $fh "\n  ]\n}"; 
	}
	close $fh;
	$self->{files}->{$type}->{filehandle} = undef;

}

sub write_to_filehandle
{
	my ($self, $type, $data, $unordered) = @_;

	#close filehandle if we are about to write item n+1 to it
	if
	(
		$self->{max_per_file}->{$type} && #if this type does paging
		defined $self->{files}->{$type}->{count} && #if this type has been initialised (a bit of a hack)
		$self->{files}->{$type}->{count} >= $self->{max_per_file}->{$type}
	)
	{
		$self->close_file($type);
	}

	#create new file if we don't have a filehandle
	if (!$self->{files}->{$type}->{filehandle})
	{
		$self->{files}->{$type}->{page}++;
		$self->{files}->{$type}->{filehandle} = $self->create_fh($type, $self->{files}->{$type}->{page}, $unordered);
		$self->{files}->{$type}->{count} = 0;
		$self->initialise_file($type);
	}

	my $fh = $self->{files}->{$type}->{filehandle};

	if ($type eq 'json' && $self->{files}->{$type}->{count}) #if it's not the first json entry
	{
		print $fh ",\n"; #record separator
	}

	$self->{files}->{$type}->{count}++;
	print $fh $data;
}

sub append_tweet_to_file
{
	my ($self, $tweet, $unordered) = @_;

	my $ts = $self->{current_tweetstream};
	my $csv = EPrints::Plugin::Export::TweetStream::CSV::tweet_to_csvrow($tweet, $ts->csv_cols);
	$self->write_to_filehandle('csv', $csv, $unordered);

	my $json = EPrints::Plugin::Export::TweetStream::JSON::tweet_to_json($tweet, 6, 0 );
	$self->write_to_filehandle('json', $json, $unordered);
}

sub write_tweetstream_metadata
{
	my ($self) = @_;
	my $repo = $self->repository;

	my $ts = $self->{current_tweetstream};

	my $xml = $ts->to_xml;
	my $fh = $self->create_fh('tweetstreamXML');
#	binmode($fh, ":utf8");
	print $fh $repo->xml->to_string($xml);
	close $fh;

}


sub t
{
	my ($msg) = @_;
	print STDERR scalar localtime time, $msg, "\n";
}

sub export_single_tweetstream
{
	my ($self, $ts) = @_;

	#don't generate a package if it's already been archived
	#there may be nothing in the database
	return if $ts->value('status') eq 'archived';

	$self->reset_tmp_dir; #need a new tempdir for each export
	$self->{files} = {}; #reset filename counters etc. -- should rewrite the whole process to make it less hacky

	$self->{current_tweetstream} = $ts;

	$self->output_status('Generating Package for tweetstream ' . $ts->value('tweetstreamid'));
	$self->{log_data}->{tweetstreams_exported}->{$ts->value('tweetstreamid')}->{package_generation_start_time} = scalar localtime time;

	my $repo = $self->repository;
	my $db = $repo->database;
	my $ds = $repo->dataset('tweet');
	my $tsid = $ts->id;

	my $tweet_count = $ts->value('tweet_count');
	my $n = 0;

	$self->write_tweetstream_metadata;

	my $order_field = 'twitterid';
	my $unordered = 0;
	if ($tweet_count > 2000000) #starts to cause issues sorting on twitterid
	{
		$order_field = 'tweetid';
		$unordered = 1;
	}

	#wait before we query the database (don't get in the way of the other processes
	$self->wait;
	my $sth = $db->prepare($self->_generate_sql_query($order_field, $tsid));
$self->output_status('Running Query');
	$sth->execute(0);
$self->output_status('Query Completed');

	my $highid = 0;
	while ($sth->rows > 0)
	{
		$n += $sth->rows;

		$self->output_status("Packaging: $n of $tweet_count -- high_id -> $highid");

		while (my $row = $sth->fetchrow_hashref)
		{
			$highid = $row->{$order_field}; #they're coming out in ascending order, so we don't need to care about testing if it's higher
	
			my $tweet = $ds->dataobj($row->{tweetid});

			next unless $tweet;

			$self->{log_data}->{tweetstreams_exported}->{$tsid}->{package_tweet_count}++; 
			$self->append_tweet_to_file($tweet, $unordered);

		}
		#wait before we query the database
		$self->wait;

#		$sth = $db->prepare($self->_generate_sql_query($tsid, $highid));
$self->output_status('Running Query');
		$sth->execute($highid);
$self->output_status('Query Completed');
	}
	#tidy up
	$self->close_file('csv');
	$self->close_file('json');

	$self->output_status('generating zip file');

	$ts->delete_export_package;

	my $final_filepath = $ts->export_package_filepath;

	create_zip($self->{tmp_dir}, "tweetstream$tsid", $final_filepath );

	$self->{log_data}->{tweetstreams_exported}->{$ts->value('tweetstreamid')}->{package_filesize} = -s $final_filepath;
	$self->{log_data}->{tweetstreams_exported}->{$ts->value('tweetstreamid')}->{package_generation_end_time} = scalar localtime time;
	$self->output_status('Done generating package');
}


sub create_zip
{
	my ($dir_to_zip, $dirname_in_zip, $zipfile) = @_;

	my $z = Archive::Zip->new();

	$z->addTree($dir_to_zip, $dirname_in_zip);
	$z->writeToFileNamed($zipfile);
}


sub generate_log_string
{
	my ($self) = @_;

	my $l = $self->{log_data};

	my @r;

	push @r, '===========================================================================';
	push @r, '';
        push @r, "Export started at:        " . $l->{start_time};
	push @r, '';
	if ($self->{log_data}->{tweetstreams_exported} && scalar keys %{$self->{log_data}->{tweetstreams_exported}})
	{
		foreach my $tsid (keys %{$self->{log_data}->{tweetstreams_exported}})
		{
			my $ts_log = $self->{log_data}->{tweetstreams_exported}->{$tsid};
			push @r, "$tsid: " . $ts_log->{package_generation_start_time} . ' to ' . $ts_log->{package_generation_end_time} . ". Filesize: " . $ts_log->{package_filesize};
		}
	}
	else
	{
		push @r, 'No Tweetstream Packages Generated';
	}
	push @r, '';
	push @r, "Export finished at:       " . $l->{end_time};
	push @r, '';
	push @r, '===========================================================================';

	return join("\n", @r);
}

1;
