package EPrints::Plugin::Event::UpdateTweetStreams;

use EPrints::Plugin::Event::LockingEvent;
@ISA = qw( EPrints::Plugin::Event::LockingEvent );

use strict;

use URI;
use LWP::UserAgent;
use JSON;
use Encode qw(encode);
use Net::Twitter::Lite::WithAPIv1_1;

my $HTTP_RETRIES = 5; #for network errors
my $TWITTER_TIMEOUT = 30; #timeout on twitter API
my $QUERY_RETRIES = 5; #for API errors
my $QUERIES_BEFORE_RATE_CHECK = 100; #because one query may use more than one of the quota if it's complex

sub new
{
        my( $class, %params ) = @_;

        my $self = $class->SUPER::new(%params);

	$self->{sleep_time} = 10; #number of seconds to sleep while waiting
	$self->{lock_timeout} = 60*15; #15 minute

        return $self;
}

sub action_update_tweetstreams
{
	my ($self, %opts) = @_;

	$self->{verbose} = $opts{verbose};
	$self->{status_log_file} = $opts{status_log_file};
	$self->{max_tweets_per_session} = $self->repository->config('tweepository_max_tweets_per_session');

	if ($self->is_locked)
	{
		if ($self->has_timed_out_lock)
		{
			$self->repository->log( (ref $self) . " Logfile has timed out -- assuming hung process");
			$self->kill_locked_process;
			sleep(5); #give the process time to be killed;
			#use is_locked to check if the process is running and clear the lock file if need be.
			if ($self->is_locked)
			{
				$self->repository->log( (ref $self) . " Unable to clear lock on timed out process");
				return;
			}
		}
		else
		{
			$self->repository->log( (ref $self) . " is locked.  Unable to run.");
			return;
		}
	}
	$self->create_lock;

	$self->{log_data}->{start_time} = EPrints::Time::iso_datetime;

	$self->wait;

	my $nt = $self->connect_to_twitter;
	if (!$nt)
	{
		$self->repository->log( (ref $self) . " was unable to connect to twitter.");
		return;
	}

	$self->output_status('Connected to Twitter');

	my $limit = $self->get_search_rate_limit($nt); 

	$self->output_status("Initial Rate Limit: $limit");

	my $active_tweetstreams = $self->active_tweetstreams;
	my $queue_items = {};
	$active_tweetstreams->map(
		\&EPrints::Plugin::Event::UpdateTweetStreams::create_queue_item,
		{ queue_items => $queue_items, event_plugin => $self }
	);
	my @queue = values %{$queue_items};

	$self->output_status('Queue has ' . scalar @queue . ' items');

	QUERYSET: while ($limit > 0)
	{
		my $n = $QUERIES_BEFORE_RATE_CHECK;
		$n = $limit if $limit < $n;
		QUERY: for (1..$n)
		{
			if (scalar @queue ==0)
			{
				$self->output_status('Update queue emptied');
				$self->{log_data}->{end_state} = 'Update queue emptied';
				last QUERYSET;
			}

			my $current_item = shift @queue;
			my $results = undef;
			my $results_flag = 0;
			my $err = undef;
			my $end_state = undef;

			RETRY: foreach my $retry (1..$HTTP_RETRIES)
			{
				$self->output_status('Attempting to query: ' . $current_item->{search_params}->{q} . ", attempt $retry");
				if (!$nt->authorized)
				{
					$self->output_status('Oops, not authorise.  Reconnecting....');
					sleep 10;
					$nt = $self->connect_to_twitter;
					next RETRY; #try again
				}
				$self->refresh_lock; #update datestamp on lock
$self->output_status('About to search');
				eval {
					local $SIG{ALRM} = sub { die "timeout\n" }; #\n required
					alarm $TWITTER_TIMEOUT; 
					$results = $nt->search($current_item->{search_params});
					alarm 0;
				};
				alarm 0; #just in case of twitter errors within timeout
$self->output_status('Search Complete');

				#if we have an error, sleep and then try again, otherwise exit the retry loop.
				#note that this approach only records the final error -- oh well.
				if ( $err = $@ ) {
$self->output_status('Error Occurred');
					#handle response codes -- see https://dev.twitter.com/docs/error-codes-responses
					if (
						ref $err
						and
						(
							$err->isa('Net::Twitter::Error')
							or $err->isa('Net::Twitter::Lite::Error')
						)
					)
					{
						$self->output_status('Twitter error occurred');
						if ($err->code == 403) #no more data for this stream -- we've gone back as far as we can
						{
							$self->output_status('Err 403: No more results for this search');
							$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{end_state} = 'No More Results (403, went back as far as possible)';
							last RETRY;
						}
						elsif ($err->code == 429) #rate limit reached -- stop all requests
						{
							$self->output_status('ERR 429: API limit exceeded');
							$limit = -1; #we've gone over the limit
							last RETRY;
						}
					}
					my $code = $err->code;
					my $class = ref $err;
					$self->output_status("Error $code of type $class");
					sleep 10;
					next RETRY;
				}
				else
				{
$self->output_status('No error occurred');
					$self->output_status('Results successfully retrieved');
					$results_flag = 1;
					last RETRY; #we have our results
				}
			}
$self->output_status('Retry loop complete');

			#process results and put the current item at the end of the queue (if appropriate)
			if ($results_flag)
			{
$self->output_status('Results flag set');
				my $results_count = scalar @{$results->{statuses}};
				$current_item->{session_tweet_count} += $results_count;

				#no errors, process the results
				if ($results_count < 1)#if an empty page of results, assume no more tweets
				{
					$self->output_status('Empty Results Set');
					$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{end_state} = 'Update Completed (no more tweets from twitter)';
				}
				else
				{
					$self->output_status('Retrieved ', scalar @{$results->{statuses}}, ' statuses (since_id: ) ' . $current_item->{search_params}->{since_id});
					$self->process_results($current_item, $results);

					$self->output_status('Tweets created');
#disabled this check -- it appears from the logs that sometimes e.g. 98 tweets are returned.  Not sure why
					#if less than a page of data, assume we've reached the end of the results
#					if ($results_count < $current_item->{search_params}->{count})
#					{
#						$self->output_status('Less than ' . $current_item->{search_params}->{count} . ' results -- not requeueing');
#						$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{end_state} = 'Update Completed (partial page from twitter)';
#					}
					#if this tweetstream has had more than its fair share of API (it's crazy big)
					if ($current_item->{session_tweet_count} > $self->{max_tweets_per_session})
					{
						$self->output_status('Tweetstream ' . $current_item->{id} . ' has harvested ' . $current_item->{session_tweet_count} . ', so has not been requeued.');
						$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{end_state} = 'Reached tweepository session limit';
					}
					else
					{ 
						$self->output_status('Full results set -- requeueing');
						#requeue the current item

						###############
						#REQUEUE ITEM##
						###############
						push @queue, $current_item;
					}
				}
			}
			else
			{
$self->output_status('results flag not set');
				#we tried N times, and failed -- do not re-queue the current item
				if (ref($err) and $err->isa('Net::Twitter::Error'))
				{

					$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{end_state} =
						"BAD RESPONSE FROM TWITTER:\n" .
						"\tHTTP Response Code: " . $err->code . "\n" .
						"\tHTTP Message......: " . $err->message . "\n" .
						"\tTwitter error.....: " . $err->error . "\n";
				}
				else
				{
					$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{end_state} = 'Unexpected Error: ' . $@;
					#do not re-queue the current item
				}
				$self->output_status($self->{log_data}->{tweetstreams}->{$current_item->{id}}->{end_state});
			}

		}
		#update the limit, just in case.
		$limit = $self->get_search_rate_limit($nt);
		$self->output_status("Updated limit.  It's now $limit");
	}

	$self->output_status("Updating ended");
	$self->{log_data}->{end_time} = EPrints::Time::iso_datetime;
	$self->{log_data}->{api_rate_limit} = $limit;

	#write logs for each tweetstream
	my $ts_ds = $self->repository->dataset('tweetstream');
	foreach my $tsid (keys %{$self->{log_data}->{tweetstreams}})
	{
		my $ts_log = $self->{log_data}->{tweetstreams}->{$tsid};

                my $count = v($ts_log->{tweets_created},0) + v($ts_log->{tweets_added},0);
                my $end = v($ts_log->{end_state},'Unknown Endstate');

		my $ts = $ts_ds->dataobj($tsid);
		next unless $ts;
		$ts->log_update($self->{log_data}->{start_time},$self->{log_data}->{end_time},$count,$end);
	}

	$self->write_log;
	$self->remove_lock;
}

sub process_results
{
	my ($self, $current_item, $results) = @_;

	my $repo = $self->repository;
	my $tweetstream_ds = $repo->dataset('tweetstream');

	my $tweet_dataobjs = [];

	#create a tweet dataobj for each tweet and store the objid in the queue item
	TWEET_IN_UPDATE: foreach my $tweet (@{$results->{statuses}})
	{
		$self->{log_data}->{tweets_processed}++; #global count
		$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{earliest_seen} = $tweet->{created_at}; #keep updating this as we walk backwards, though it

		#only need to set these once
		if (!$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{latest_seen})
		{
			#search results go backwards, so the first result returned will be the latest one
			$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{latest_seen} = $tweet->{created_at};
			$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{search_string} = $current_item->{search_params}->{q};
		}

		#keep track of the lowest twitterid we've seen for paging
		if (
			!$current_item->{search_params}->{max_id}
			|| $tweet->{id} < $current_item->{search_params}->{max_id}
		)
		{
			$current_item->{search_params}->{max_id} = $tweet->{id};
		}

		#check to see if we already have a tweet with this twitter id in this repository
		my $tweetobj = EPrints::DataObj::Tweet::tweet_with_twitterid($repo, $tweet->{id});
		if (!defined $tweetobj)
		{
			$self->output_status('Creating Tweet Object');
			$tweetobj = EPrints::DataObj::Tweet->create_from_data(
				$self->repository,
				{
					twitterid => $tweet->{id},
					json_source => $tweet,
#this is now handled by a call to $tweetstream->add_tweets
#					tweetstreams => $current_item->{tweetstreamids},
				} 
			);
			$tweetobj->commit; #will enrich the tweet

			$self->{log_data}->{tweets_created}++; #global_count
			$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{tweets_created}++;
		}
		else
		{
			$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{tweets_added}++;
			$self->output_status('Tweet Object Exists');
		}

		#safe to do because we're updating in pages of 100 -- we won't run out of memory
		push @{$tweet_dataobjs}, $tweetobj;
	}

	$self->output_status('Created all tweet objects');

	#set max_id for paging
	$current_item->{search_params}->{max_id}--; #set it to one lower to an ID we have previously seen for paging

	foreach my $tweetstreamid (@{$current_item->{tweetstreamids}})
	{
		$self->output_status("Adding tweets to $tweetstreamid");
		my $tweetstream = $tweetstream_ds->dataobj($tweetstreamid);
		die ("UNEXPECTED CRITICAL ERROR: couldn't create tweetstream $tweetstreamid") unless $tweetstream;

		$tweetstream->add_tweets($tweet_dataobjs);
	}
}

sub connect_to_twitter
{
	my ($self) = @_;

	my %nt_args = ( traits => [qw/API::RESTv1_1/], ssl => 1 );
	foreach (qw( consumer_key consumer_secret access_token access_token_secret ))
	{
		$nt_args{$_} = $self->repository->config('twitter_oauth',$_);
	}

	my $nt = Net::Twitter::Lite::WithAPIv1_1->new( %nt_args );

#handle this error properly;
	if (!$nt->authorized)
	{
		$self->repository->log( (ref $self) . " Net::Twitter Oauth issue\n");
		return undef;
	}
	return $nt;
}


#return a value or an empty string
sub v
{
	my ($val, $default) = @_;
	return $val if defined $val;
	return $default if defined $default;
	return '';
}

sub generate_log_string
{
	my ($self) = @_;
	my $l = $self->{log_data};

	my @r;

	push @r, "Update started at: " . v($l->{start_time});
	push @r, "Update finished at: " . v($l->{end_time});
	push @r, "API Queries left: " . v($l->{api_rate_limit});
	push @r, v($l->{tweets_processed}, 0) . " tweets processed";
	push @r, v($l->{tweets_created}, 0) . " tweets created";
	push @r, (scalar keys %{$l->{tweetstreams}}, 0) . " tweetstreams updated:";

	foreach my $ts_id (sort keys %{$l->{tweetstreams}})
	{
		my $ts = $l->{tweetstreams}->{$ts_id};

		my $new = v($ts->{tweets_created},0);
		my $added = v($ts->{tweets_added},0);
		my $end = v($ts->{end_state},'Unknown Endstate');
		my $earliest = v($ts->{earliest_seen},'unknown');
		my $latest = v($ts->{latest_seen},'unknown');

		push @r, "\t$ts_id: " . v($ts->{search_string},'undef') ;
		push @r, "\t\t$new created";
		push @r, "\t\t$added existing tweets added (stream overlap or page shifting)";
		push @r, "\t\tFrom: $earliest";
		push @r, "\t\tTo:   $latest";
		push @r, "\t\tCompleted with status: $end";
	}

	my $end = v($l->{end_state},'No Known Errors');
	push @r, "Complete with status: " . $end;

	return join("\n",@r);
}

sub create_queue_item
{
	my ($repo, $ds, $tweetstream, $info) = @_;

	my $queue_items = $info->{queue_items};
	my $event_plugin = $info->{event_plugin};

	return unless $tweetstream->is_set('search_string');
	return if (
		$tweetstream->is_set('status')
		&& $tweetstream->value('status') eq 'archived'
	);  #should never be true, but let's be explicit.

	my $search_string = $tweetstream->value('search_string');

	my $geocode = '';
	$geocode = $tweetstream->value('geocode') if $tweetstream->is_set('geocode');

	my $key = $search_string . 'XXXXXXX' . $geocode;

	if ($queue_items->{$key})
	{
		push @{$queue_items->{$key}->{tweetstreamids}}, $tweetstream->id;
		$queue_items->{$key}->{id} = join(',',sort(@{$queue_items->{$key}->{tweetstreamids}}));
		#set the highest_twitterid for this query to whichever tweetstream has the lowest high_id
		if (
			!$event_plugin->{previous_run_incomplete}
			&& $queue_items->{$key}->{search_params}->{since_id} > $tweetstream->highest_twitterid
		)
		{
			$queue_items->{$key}->{search_params}->{since_id} = $tweetstream->highest_twitterid;
		}
	}
	else
	{
		$queue_items->{$key} = {
			id => $tweetstream->id, #id for logging
			search_params => {
				q => $search_string,
				count => 100,
				include_entities => 1,
	#			max_id => Will be set to the lowest id we find for the purposes of paging
				since_id => 0, #will be set to a proper value later
			},
			tweetstreamids => [ $tweetstream->id ], #for when two streams have identical search strings
			retries => $QUERY_RETRIES, #if there's a failure, we'll try again.
			session_tweet_count => 0, #how many tweets have we added this session?
		};

		#get all available results to fill in possible holes if we've previously crashed
		if (!$event_plugin->{previous_run_incomplete})
		{
			$queue_items->{$key}->{search_params}->{since_id} = $tweetstream->highest_twitterid;
		}

		#optional param
		$queue_items->{$key}->{search_params}->{geocode} = $geocode if $geocode;
	}
}


sub active_tweetstreams
{
	my ($self) = @_;

	my $ds = $self->repository->get_dataset( "tweetstream" );

	my $searchexp = EPrints::Search->new(
			session => $self->repository,
			dataset => $ds,
			);
	my $today = EPrints::Time::get_iso_date( time );
	$searchexp->add_field(
			$ds->get_field( "expiry_date" ),
			$today."-" );
#	$searchexp->add_field(
#			$ds->get_field( "status" ),
#			"active" );
	

	return $searchexp->perform_search;
}


sub get_search_rate_limit
{
	my ($self, $nt) = @_;


	RETRY: foreach my $retry (1..$HTTP_RETRIES)
        {
		my $rl;
		$self->output_status('About to Get rate limit');
		eval {
			local $SIG{ALRM} = sub { die "timeout\n" }; #\n required
				alarm $TWITTER_TIMEOUT; 
			$rl = $nt->rate_limit_status();
			alarm 0;
		};
		alarm 0; #just in case of twitter errors within timeout
		if ($@)
		{
			$self->output_status('Uncategorised error or twitter timout, retrying...');
			sleep 10;
			next RETRY;
		}
		else
		{
			#no error

			$self->output_status('Got rate limit');
			#walk down the nested hash
			foreach my $key (qw( resources search /search/tweets remaining ))
			{
				if (!exists $rl->{$key})
				{
					$rl = undef;
					return $rl;
				}
				$rl = $rl->{$key};
			}
			return $rl;

		}
	}
	$self->output_status('Unable to get rate limit.  Giving up.');
	return 0; #something's gone very wrong.  Let's just say we're out of API.
};





1;
