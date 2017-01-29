package EPrints::Plugin::Event::ArchiveTweetStreams;

use EPrints::Plugin::Event::ExportTweetStreamPackage;
@ISA = qw( EPrints::Plugin::Event::ExportTweetStreamPackage );

use File::Path qw/ make_path /;
use Archive::Zip qw( :ERROR_CODES :CONSTANTS );
use File::Copy;
use JSON::XS qw/ /;
use JSON::PP qw/ /;
use Cwd;

use strict;

sub action_archive_tweetstreams
{
	my ($self, %args) = @_;

        $self->{log_data}->{start_time} = scalar localtime time;

	my $repo = $self->repository;

	$self->{verbose} = 1 if $args{verbose};

	if ($self->is_locked)
	{
		$self->repository->log( (ref $self) . " is locked.  Unable to run.\n");
		return;
	}
	$self->create_lock;

	$self->_initialise_constants();

	$self->wait;

	$self->output_status('Packaging Inactive TweetStreams');
	$self->package_inactive_tweetstreams(%args);

	$self->output_status('Removing Archived Tweets from DB');
	$self->remove_archived_tweetstream_tweets_from_database(%args);

	$self->output_status('Retiring Done');

	$self->remove_lock;
        $self->{log_data}->{end_time} = scalar localtime time;
	$self->write_log;
}

sub package_inactive_tweetstreams
{
	my ($self, %args) = @_;
	my $repo = $self->repository;

	$self->output_status('Getting Inactive TweetStreams');

	my $ts_ds = $self->repository->dataset('tweetstream');
	my $search = $ts_ds->prepare_search;
	$search->add_field($ts_ds->get_field('status'), 'inactive');
	my $list = $search->perform_search;

	my @tweetstreams = $list->get_records; #should be safe as it's unlikely there'll be millions of tweetstreams retiring today

	TWEETSTREAM: foreach my $ts(@tweetstreams)
	{
		#check if the existing package is good enough
		if (-e $ts->export_package_filepath)
		{
			$self->output_status("Verifying existing package for " . $ts->value('tweetstreamid'));
                	if ($self->verify_package($ts))
                	{
				$self->output_status("Package successfully verified");
				#we might not have to build a package
				$ts->set_value('status', 'archived');
				$ts->commit;
				next TWEETSTREAM;
			}
			else
			{
				$self->output_status("Package failed verification");
			}
		}


		$self->output_status("Creating Package for " . $ts->value('tweetstreamid')); 

		$self->export_single_tweetstream($ts);

		$self->output_status("Package created, now verifying");

		my $n = 0;
		if ($self->verify_package($ts))
		{
			#setting status to 'archived' will prevent the package from being regenerated
			#it will also trigger deletion of tweets from DB
			$ts->set_value('status', 'archived');
			$ts->commit;
		}
		else
		{
#let's keep this around while we're debugging.
#			$ts->delete_export_package; #don't allow downloading of it -- it failed validation

			#todo -- prevent downloading of package while it's being validated.

			$self->output_status('Verification Failed, not retiring tweetstream');
			$repo->log('Could not verify package for inactive tweetstream ' . $ts->value('tweetstreamid'));
		}
		
	}
}

sub remove_archived_tweetstream_tweets_from_database
{
	my ($self, %args) = @_;
	my $repo = $self->repository;

	$self->output_status('Getting Archived TweetStreams');

	my $ts_ds = $self->repository->dataset('tweetstream');
	my $search = $ts_ds->prepare_search;
	$search->add_field($ts_ds->get_field('status'), 'archived');
	my $list = $search->perform_search;

	my @tweetstreams = $list->get_records; #should be safe as it's unlikely there'll be millions of tweetstreams retiring today

	TWEETSTREAM: foreach my $ts(@tweetstreams)
	{
		my $tweet_count = $ts->value('tweet_count');
		my $ts_id = $ts->value('tweetstreamid');

		#don't do this tweetstream if it's small and we only archive large ones.
		if (
			$repo->config('tweepository_only_archive_large_tweetstreams')
			&& $tweet_count <= $repo->config('tweepository_export_threshold')
		)
		{
			$self->output_status("Tweetstream $ts_id is small.  Not removing tweets from DB");
			next TWEETSTREAM;
		}

		if (!-e $ts->export_package_filepath)
		{
			#a tweetstream is not of status 'archived' unless a package has been generated and verified, so this is just paranoia.
			$self->output_status("No package for Tweetstream $ts_id -- not removing tweets from DB");
			next TWEETSTREAM;
		}

		$self->output_status("Removing Tweetstream $ts_id tweets from DB");
		#remove all tweets from the database
		my $page_size = 100000;
		my $n = 0;
		my $highest_id = 0;
		while (1)
		{
			$n += $page_size;
			$self->output_status("TweetStream $ts_id: Removing $n of (about) $tweet_count tweets");

			$self->wait();
			$self->output_status("Getting Tweets higher than $highest_id");

			#we're just removing them, so we don't care about order -- use the natural order of the database
			#also, we don't care about low_id because we're simply removing them in batches
			my $tweets = $ts->tweets($page_size, 0, 'tweetid');
 
			last unless $tweets->count; #exit loop if there are no results returned

			$self->output_status("Removing Tweets");
			$tweets->map( sub
			{
				my ($repo, $ds, $tweet, $tweetstream) = @_;
				$highest_id = $tweet->value('tweetid'); #the tweets come out in twitterid order, so we don't need to test this.
				$tweet->remove_from_tweetstream($ts);
				$self->wait();
			}, $self);
		}
	}
}


#before we remove any tweets from the database, we'll do a full parse of all the JSON files and check how many tweets we have
sub verify_package
{
	my ($self, $ts) = @_;
	my $repo = $self->repository;
	my $tsid = $ts->id;

	#get path to package
	my $filename = $ts->export_package_filepath;
	return 0 unless -e $filename;

	my $tweet_count = 0;

	if ($filename =~ m/\.zip$/)
	{
		 $tweet_count = $self->count_tweets_in_zip($ts, $filename);
	}
	elsif ($filename =~ m/\.tar\.gz/)
	{
		$tweet_count = $self->count_tweets_in_tar($ts, $filename);
	}

	$self->wait;
	#check that there out count from the package matches the count from the database (refresh by query if necessary).
	my $updated_tweet_count = $ts->count_with_query;
	if ($updated_tweet_count != $ts->value('tweet_count'))
	{
		$ts->set_value('tweet_count', $updated_tweet_count);
		$ts->commit;
	}

	$self->output_status("Count from query: $updated_tweet_count, Count from JSON verify: $tweet_count\n");

	if ($tweet_count != $updated_tweet_count)
	{
		$self->{log_data}->{packages_validated}->{$tsid}->{end_state} = 'package invalid (count mismatch)';
		$self->{log_data}->{packages_validated}->{$tsid}->{validate_end_time} = scalar localtime time;
		
		$repo->log("Tweetstream " . $ts->value('tweetstreamid') . " package $filename contains $tweet_count tweets, but the dataobj contains $updated_tweet_count");
		return 0;
	}
	
	$self->{log_data}->{packages_validated}->{$tsid}->{end_state} = 'Export Successful';
	$self->{log_data}->{packages_validated}->{$tsid}->{validate_end_time} = scalar localtime time;

	return 1;
}

sub count_tweets_in_tar
{
	my ($self, $ts, $filename) = @_;

	$self->output_status("Checking tar $filename\n");

	my $repo = $self->repository;
	my $tsid = $ts->value('tweetstreamid');
	my $tweet_count = 0;

	#unpack to temp directory
	my %args = ( TMPDIR => 1 );
	if ($self->repository->config('tweetstream_export_package_directory'))
	{
		$args{DIR} = $self->repository->config('tweetstream_export_package_directory');
	}
	my $tmp_dir = File::Temp->newdir( "ep-ts-verify-tempXXXXX", %args);

	copy($filename, $tmp_dir);

	my $filename_no_path = $ts->export_package_filename;

	#quick and dirty OS call
	my $cwd = cwd();
	chdir $tmp_dir;

	my $cmd = $repo->config('executables', 'tar');
	`$cmd xzf $filename_no_path`;

	chdir $cwd;

	my $json_files = [];
	$self->all_json_files_in_dir($tmp_dir, $json_files);
	
	foreach my $json_file (@{$json_files})
	{
		my $fh;
		if (open ($fh, '<', $json_file))
		{
			$tweet_count += $self->count_tweets_in_json($fh, $json_file);
		}
		else
		{
			$repo->log("Couldn't open $json_file");
		}
	}
	return $tweet_count;
}

sub all_json_files_in_dir
{
	my ($self, $path, $json_files) = @_;

	if (-f $path && $path =~ m/\.json$/)
	{
		push @{$json_files}, $path;
	}
	if (-d $path)
	{
		opendir(DIR, $path);
		my @files = grep { !/^\.{1,2}$/ } readdir (DIR); #ignore . and ..
		closedir(DIR);
		@files = map { $path . '/' . $_ } @files; #full paths
		foreach (sort @files)
		{
			$self->all_json_files_in_dir($_, $json_files);
		}
	}
}

sub count_tweets_in_zip
{
	my ($self, $ts, $filename) = @_;
	my $repo = $self->repository;
	my $tsid = $ts->value('tweetstreamid');

	$self->{log_data}->{packages_validated}->{$tsid}->{validate_start_time} = scalar localtime time;
	if (!-e $filename)
	{
		$self->{log_data}->{packages_validated}->{$tsid}->{end_state} = 'package missing';
		$self->{log_data}->{packages_validated}->{$tsid}->{validate_end_time} = scalar localtime time;
		$repo->log("export file $filename doesn't exist");
		return 0;
	}

	#unzip package
	my $files;

	# Read a Zip file
	my $zip = Archive::Zip->new();
	unless ( $zip->read( $filename ) == AZ_OK ) {
		$repo->log("Unreadable zip file at $filename");
		return 0;
	}

	foreach my $member ($zip->members)
	{
		next if $member->isDirectory;
		my $fname = $member->fileName;

		$fname =~ m/\.([^\.]*)$/;
		my $extension = $1;

		next unless $extension;

		push @{$files->{$extension}}, $fname;
	}

	my $tweet_count = 0;
	#foreach json file
	$files->{json} = [] unless $files->{json}; #in case of empty tweetstreams
	foreach my $json_file (sort @{$files->{json}})
	{
		my $fh = file_in_zip_to_fh($json_file, $zip);
		$tweet_count += $self->count_tweets_in_json($fh, $json_file);
	}

	return $tweet_count;
}

sub count_tweets_in_json
{
	my ($self, $filehandle, $filename) = @_;

	my $tweet_count = 0;

	$self->output_status('verifying json file ' . $filename);

	my @json_txt = <$filehandle>;
	my $tweets = $self->decode_tweet_json(join('',@json_txt));

	if (!$tweets)
	{
		return 0;
	}

	foreach my $json_tweet (@{$tweets->{tweets}})
	{
		if (!$json_tweet->{id})
		{
			$self->output_status("Tweet with no ID in $filename in $filename\n");
			return 0;
		}
		$tweet_count++;
	}
	return $tweet_count;
}


sub decode_tweet_json 
{
	my ($self, $json_txt) = @_;

        my $json = JSON::XS->new->allow_nonref;

        my $tweets = eval { $json->utf8()->decode($json_txt); };
        if (!$@)
        {
		return $tweets;
        }
	$self->output_status("JSON::XS failed, moving onto JSON::PP");

	#PP is painfully slow, but allow loose decoding, which ignores some invalid utf8 text that appears to be creeping in...
        $json = JSON::PP->new->allow_nonref;
	$json->loose(1); 

        $tweets = eval { $json->utf8()->decode($json_txt); };
        if (!$@)
        {
		return $tweets;
        }
	$self->output_status("JSON::PP failed");

	return undef;
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
			if ($self->{log_data}->{packages_validated}->{$tsid})
			{
				push @r, "\tValidation: "
					. $self->{log_data}->{packages_validated}->{$tsid}->{validate_start_time} 
					. ' to '
					. $self->{log_data}->{packages_validated}->{$tsid}->{validate_end_time}
					. ' --> '
					. $self->{log_data}->{packages_validated}->{$tsid}->{end_state};
			}
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



#File::Zip's function to provide a handle to a zipped file
#doesn't seem to work, so we'll write to a temp file and give a handle to that
sub file_in_zip_to_fh
{
	my ($filename, $zip) = @_;

	my $tmp_fh = File::Temp->new( TEMPLATE => "ep-ts-import_unzipXXXXX", TMPDIR => 1 );

	my $member = $zip->memberNamed($filename);
	$member->extractToFileHandle($tmp_fh);

	#move to start of file
	seek($tmp_fh, 0, 0);

	return $tmp_fh;
}

1;
