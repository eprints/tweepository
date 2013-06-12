package EPrints::Plugin::Event::UpdateTweetStreams;

use EPrints::Plugin::Event::LockingEvent;
@ISA = qw( EPrints::Plugin::Event::LockingEvent );

use strict;

use URI;
use LWP::UserAgent;
use JSON;
use Encode qw(encode);
use Net::Twitter::Lite::WithAPIv1_1;

my $RETRIES = 5;

sub action_update_tweetstreams
{
	my ($self) = @_;
	my $FEEDS_IN_PARALLEL = 3;

	$self->{log_data}->{start_time} = scalar localtime time;

	if ($self->is_locked)
	{
		$self->repository->log( (ref $self) . " is locked.  Unable to run.");
		return;
	}

	my $nt = $self->connect_to_twitter;
	return unless $nt;

	$self->create_lock;

	my $active_tweetstreams = $self->active_tweetstreams;
	my $queue_items = {};
	$active_tweetstreams->map( \&EPrints::Plugin::Event::UpdateTweetStreams::create_queue_item, $queue_items);

	my @queue = values %{$queue_items};

	my $ua = LWP::UserAgent->new;
	my $nosort = 0;

#cache the IDs of all items created in this session
#that way, when we find one that wasn't created in this session
#we know we've gone back far enough
	my $created_this_session = {};

	ITEM: while ( scalar @queue ) #future development -- test API limits too
	{
		#prioritise by date, but have some parallelisation
		#nosort flag counts down from FEEDS_IN_PARALLEL
		if (!$nosort)
		{
			@queue = $self->order_queue(@queue);
			$nosort = $FEEDS_IN_PARALLEL + 1;
		}
		$nosort--;

		#remove item from the front of the queue
		my $current_item = shift @queue;

		my $max_http_retries = 5;
		RETRY: foreach my $retries (1..$max_http_retries)
		{
			my $results;

			if (!$nt->authorized)
			{
				$nt = $self->connect_to_twitter;

				if (!$nt)
				{
					$self->{log_data}->{end_state} = 'Couldn\'t reconnect to twitter ';
					last ITEM;
				}
			} 

			eval {
				$results = $nt->search($current_item->{search_params});
			};
			if ( my $err = $@ ) {
				if (ref($err) and $err->isa('Net::Twitter::Error'))
				{
	#make decisions based on HTTP response codes
					my $code = $err->code;
					if ($code == 403) #forbidden -- probably because we've gone back too many pages on this item
					{
						#We've got all we can.  Move onto the next and let this one fall off of the queue
						$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{end_state} = 'No more results';
						next ITEM;
					}

	
					#if we got five errors in a row (twitter gets bogged down sometimes)
					if ($retries == $max_http_retries)
					{
						my $msg = "HTTP Response Code: " . $err->code . "\n" .
							  "HTTP Message......: " . $err->message . "\n" .
							  "Twitter error.....: " . $err->error . "\n";

						#otherwise, stop trying to get this tweetstream
						$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{end_state} = 'BAD RESPONSE FROM TWITTER: ' . $msg;
						next ITEM;
					}
				}
				else
				{
					if ($retries == $max_http_retries)
					{
						#unexpected error
						$self->{log_data}->{end_state} = 'Unexpected Error ' . $@;;
						last ITEM;
					}
				}
			}
			else
			{
				if (!scalar @{$results->{statuses}})#if an empty page of results, assume no more tweets
				{
					$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{end_state} = 'No more results';
					next ITEM;
				}
				$self->process_results($created_this_session, $current_item, $results);
				#no errors, so stop retrying
				last RETRY;
			}


			sleep 10;
		}

#paging needs to be updated
		#request the next page of results (unless we've reached a previously seen item)
		if ($current_item->{search_params}->{max_id})
		{
			$current_item->{search_params}->{max_id}++;
		}

		#a bit of a hack, but if the tweetstream has finished harvesting, it will set the end_state log data
		push @queue, $current_item unless $self->{log_data}->{tweetstreams}->{$current_item->{id}}->{end_state};
		
	}

	#tweetstream is only committed when the tweets are enriched

	$self->{log_data}->{end_time} = scalar localtime time;
	$self->write_log;
	$self->remove_lock
}

sub process_results
{
	my ($self, $created_this_session, $current_item, $results) = @_;

	my $update_finished;

	#create a tweet dataobj for each tweet and store the objid in the queue item
	TWEET_IN_UPDATE: foreach my $tweet (@{$results->{statuses}})
	{

		$self->{log_data}->{tweets_processed}++; #global count
		$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{earliest_seen} = $tweet->{created_at}; #keep updating this as we walk backwards, though it
		#only need to set these once
		if (!$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{latest_seen})
		{
			$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{latest_seen} = $tweet->{created_at};
			$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{search_string} = $current_item->{search_params}->{q};
		}

		$update_finished = 0;

		$current_item->{search_params}->{max_id} = $tweet->{id}; #highest ID, for consistant paging
		$current_item->{orderval} = $tweet->{id}; #lowest processed so far, for queue ordering

		#check to see if we already have a tweet with this twitter id in this repository
		my $tweetobj = EPrints::DataObj::Tweet::tweet_with_twitterid($self->repository,$tweet->{id});
		if (!defined $tweetobj)
		{
			$tweetobj = EPrints::DataObj::Tweet->create_from_data(
				$self->repository,
				{
					twitterid => $tweet->{id},
					json_source => $tweet,
					tweetstreams => $current_item->{tweetstreamids},
				} 
			);
			$created_this_session->{$tweet->{id}} = 1;

			$self->{log_data}->{tweets_created}++; #global_count
			$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{tweets_created}++;
		}
		#an existing tweet harvested is either a crossover with another stream, or it's the last tweet from the previous update.
		#add and log if it isn't the last from the previous update.
		else
		{
			#compare $current_item->{tweetstreams} to $tweetobj->value('tweetstreams') if the tweetobj wasn't created in this session
			#If they differ, then at least one of the tweetstreams needs to have this tweet added to it.
			#If they're identical, then we've gone back as far as we need to go.
			if (
				!$created_this_session->{$tweetobj->value('twitterid')} and
				(
					join(',',sort(@{$current_item->{tweetstreamids}})) eq
					join(',',sort(@{$tweetobj->value('tweetstreams')}))
				)
			)
			{
				$update_finished = 1;
			}
			else
			{
				$tweetobj->add_to_tweetstreamid($current_item->{tweetstreamids});
				$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{tweets_added}++;
			}
		}

		#only the first in the update (id = max_id) doesn't have a following tweet
		#this will also set the flag on the first item from the previous update.
		if ($current_item->{search_params}->{max_id} != $tweet->{id})
		{
			$tweetobj->set_next_in_tweetstream($current_item->{tweetstreamids});
		}
		$tweetobj->commit;

		if ($update_finished) #the one we're considering is the same or younger than the oldest in our stream
		{
			$self->{log_data}->{tweetstreams}->{$current_item->{id}}->{end_state} = 'Update Complete';
			last TWEET_IN_UPDATE;
		}
	}
}

sub connect_to_twitter
{
	my ($self) = @_;

	my %nt_args = ( traits => [qw/API::RESTv1_1/] );
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

sub order_queue
{
	my ($self, @queue) = @_;

	return sort { ( $a->{orderval} ? $b->{orderval} : -1 ) <=> ( $b->{orderval} ? $a->{orderval} : -1) } @queue; #if there's no orderval, sort highest (i.e. prioritise new streams)
}

sub create_queue_item
{
	my ($repo, $ds, $tweetstream, $queue_items) = @_;

	return unless $tweetstream->is_set('search_string');

	my $search_string = $tweetstream->get_value('search_string');
	my $geocode = '';
	$geocode = $tweetstream->get_value('geocode') if $tweetstream->is_set('geocode');

	my $key = $search_string . 'XXXXXXX' . $geocode;

	if ($queue_items->{$key})
	{
		push @{$queue_items->{$key}->{tweetstreamids}}, $tweetstream->id;
		$queue_items->{$key}->{id} = join(',',sort(@{$queue_items->{$key}->{tweetstreamids}}));
	}
	else
	{
		$queue_items->{$key} = {
			id => $tweetstream->id, #id for logging
			search_params => {
				q => $search_string,
				count => 100,
				include_entities => 1,
	#			max_id => set to the ID of every tweet we process for paging
			},
			tweetstreamids => [ $tweetstream->id ], #for when two streams have identical search strings
			retries => $RETRIES, #if there's a failure, we'll try again.
			orderval => 0,
		};
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

	return $searchexp->perform_search;
}






1;
