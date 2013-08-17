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

	$self->{search_page_size} = 5000; #page size 
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
		$self->export_single_tweetstream($ts);
	}

	$self->remove_lock;
        $self->{log_data}->{end_time} = scalar localtime time;
	$self->write_log;
}


sub action_export_queued_tweetstream_packages
{
	my ($self) = @_;

	my $repo = $self->repository;

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

	return unless $pending_count >= 1; #just leave if there are no requests

        $self->{log_data}->{start_time} = scalar localtime time;

	if ($self->is_locked)
	{
		$self->repository->log( (ref $self) . " is locked.  Unable to run.\n");
		return;
	}
	$self->create_lock;

	$self->_initialise_constants();

	$self->export_requested_tweetstreams;

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
			if ($ts)
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
	my ($self, $tsid, $high_twitterid) = @_;

	my @parts;
	push @parts, 'SELECT tweet.tweetid, tweet.twitterid';
	push @parts, 'FROM tweet JOIN tweet_tweetstreams ON tweet.tweetid = tweet_tweetstreams.tweetid';
	push @parts, "WHERE tweet_tweetstreams.tweetstreams = $tsid";
	if ($high_twitterid)
	{
		push @parts, "AND tweet.twitterid > $high_twitterid";
	}
	push @parts, 'ORDER BY tweet.twitterid';
	push @parts, 'LIMIT ' . $self->{search_page_size};

	return join(' ',@parts);
}

sub reset_tmp_dir
{
	my ($self) = @_;

	$self->{tmp_dir} = File::Temp->newdir( "ep-ts-export-tempXXXXX", TMPDIR => 1 );
}

sub create_fh
{
	my ($self, $type, $page) = @_;

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
		$filename = $base_dir . '/tweets' . sprintf("%04d",$page) . ".$type";
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

	if ($type eq 'json')
	{
		print $fh "\n  ]\n}"; 
	}
	close $fh;
	$self->{files}->{$type}->{filehandle} = undef;

}

sub write_to_filehandle
{
	my ($self, $type, $data) = @_;

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
		$self->{files}->{$type}->{filehandle} = $self->create_fh($type, $self->{files}->{$type}->{page});
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
	my ($self, $tweet) = @_;

	my $ts = $self->{current_tweetstream};
	my $csv = EPrints::Plugin::Export::TweetStream::CSV::tweet_to_csvrow($tweet, $ts->csv_cols);
	$self->write_to_filehandle('csv', $csv);

	my $json = EPrints::Plugin::Export::TweetStream::JSON::tweet_to_json($tweet, 6, 0 );
	$self->write_to_filehandle('json', $json);
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

	$self->reset_tmp_dir; #need a new tempdir for each export
	$self->{files} = {}; #reset filename counters etc. -- should rewrite the whole process to make it less hacky

	$self->{current_tweetstream} = $ts;

	$self->output_status('Generating Package for tweetstream ' . $ts->value('tweetstreamid'));

	my $repo = $self->repository;
	my $db = $repo->database;
	my $ds = $repo->dataset('tweet');
	my $tsid = $ts->id;

	$self->write_tweetstream_metadata;

	#wait before we query the database (don't get in the way of the other processes
	$self->wait;

	$self->output_status('processing a page of tweets');

	my $sth = $db->prepare($self->_generate_sql_query($tsid));
	$db->execute($sth);

	while ($sth->rows > 0)
	{
		my $highid;
		while (my $row = $sth->fetchrow_hashref)
		{
			$highid = $row->{twitterid}; #they're coming out in ascending order, so we don't need to care about testing if it's higher
	
			my $tweet = $ds->dataobj($row->{tweetid});

			next unless $tweet;

			$self->append_tweet_to_file($tweet);

		}
		#wait before we query the database
		$self->wait;

		$self->output_status('processing a page of tweets');
		$sth = $db->prepare($self->_generate_sql_query($tsid, $highid));
		$db->execute($sth);
	}
	#tidy up
	$self->close_file('csv');
	$self->close_file('json');

	$self->output_status('generating zip file');

	my $final_filepath = $ts->export_package_filepath;
	unlink $final_filepath if -e $final_filepath; #if there's one already, delete it before creating the zip
	create_zip($self->{tmp_dir}, "tweetstream$tsid", $final_filepath );

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
	push @r, "Export finished at:       " . $l->{end_time};
	push @r, '';
	push @r, '===========================================================================';


	return join("\n", @r);
}

1;
