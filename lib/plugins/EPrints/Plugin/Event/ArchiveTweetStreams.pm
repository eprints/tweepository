package EPrints::Plugin::Event::ArchiveTweetStreams;

use EPrints::Plugin::Event::ExportTweetStreamPackage;
@ISA = qw( EPrints::Plugin::Event::ExportTweetStreamPackage );

use File::Path qw/ make_path /;
use Archive::Zip qw( :ERROR_CODES :CONSTANTS );
use File::Copy;
use JSON;

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

	$self->output_status('Getting Inactive TweetStreams');

	my $ts_ds = $self->repository->dataset('tweetstream');
	my $search = $ts_ds->prepare_search;
	$search->add_field($ts_ds->get_field('status'), 'inactive');
	my $list = $search->perform_search;

	my @tweetstreams = $list->get_records; #should be safe as it's unlikely there'll be millions of tweetstreams retiring today

	TWEETSTREAM: foreach my $ts(@tweetstreams)
	{

		#don't do this tweetstream if it's small and we only archive large ones.
		if ($repo->config('tweepository_only_archive_large_tweetstreams'))
		{
			my $tweet_count = $ts->value('tweet_count');
			#just in case
			if (!defined $tweet_count)
			{
				$tweet_count = $ts->count_with_query;
			}

			if (
				$tweet_count <= $repo->config('tweepository_export_threshold'))
			{
				#generate final package if needed (note that the process that makes it inactive removes the package), but don't erase from database
				if (!-e $ts->export_package_filepath)
				{
					$self->output_status($ts->value('tweetstreamid') . " is small, creating package");
					$self->wait;
					$self->export_single_tweetstream($ts);
				}
				$self->output_status($ts->value('tweetstreamid') . " is small, not removing from DB");

				#################
				next TWEETSTREAM;
			}
		}

		$self->output_status("Creating Package for " . $ts->value('tweetstreamid')); 

		$self->export_single_tweetstream($ts);

		$self->output_status("Package created, now verifying");

		my $n = 0;
		if ($self->verify_package($ts))
		{
			#remove all tweets from the database
			my $page_size = 1000;
			my $tweet_count = $ts->value('tweet_count');
			while (1)
			{
				$self->wait;
				$n += $page_size;
				$self->output_status("Removing $n of (about) $tweet_count tweets");
				my $tweets = $ts->tweets($page_size);
				last unless $tweets->count; #exit if there are no results returned
				$tweets->map( sub
				{
					my ($repo, $ds, $tweet, $tweetstream) = @_;
					$tweet->remove_from_tweetstream($ts);
				}, $self);
			}
			$ts->set_value('status', 'archived');
			$ts->commit;
		}
		else
		{
			$ts->delete_export_package; #don't allow downloading of it -- it failed validation

			#todo -- prevent downloading of package while it's being validated.

			$self->output_status('Verification Failed, not retiring tweetstream');
			$repo->log('Could not verify package for inactive tweetstream ' . $ts->value('tweetstreamid'));
		}
		
	}

	$self->output_status('Retiring Done');

	$self->remove_lock;
        $self->{log_data}->{end_time} = scalar localtime time;
	$self->write_log;
}

#before we remove any tweets from the database, we'll do a full parse of all the JSON files and check how many tweets we have
sub verify_package
{
	my ($self, $ts) = @_;
	my $repo = $self->repository;
	my $tsid = $ts->value('tweetstreamid');

	#get path to package
	my $filename = $ts->export_package_filepath;

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

        my $json = JSON->new->allow_nonref;

	my $tweet_count = 0;
	#foreach json file
	foreach my $json_file (sort @{$files->{json}})
	{
		my $fh = file_in_zip_to_fh($json_file, $zip);
		my @json_txt = <$fh>;

		$self->output_status('verifying json file ' . $json_file);

                my $tweets = eval { $json->utf8()->decode(join('',@json_txt)); };
		if ($@)
		{
			$repo->log("Problem parsing $json_file in $filename\n");
			return 0;
		}	

		foreach my $json_tweet (@{$tweets->{tweets}})
		{
			if (!$json_tweet->{id})
			{
				$repo->log("Tweet with ID in $json_file in $filename\n");
				return 0;
			}
			$tweet_count++;
		}
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
