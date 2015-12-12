foreach my $pluginid (qw/
	Event::ArchiveTweetStreams
	Event::DeactivateTweetStreams
	Event::ExportTweetStreamPackage
	Event::LockingEvent
	Event::RecountTweetStreams
	Event::UpdateTweetStreamAbstracts
	Event::UpdateTweetStreams
	Export::TweetStream::CSV
	Export::TweetStream::GraphML
	Export::TweetStream::GoogleMap
	Export::TweetStream::HTML
	Export::TweetStream::JSON
	Screen::EPMC::tweepository
	Screen::ManageTweetstreamsLink
	Screen::RequestTweetStreamExport
	Screen::ExportArchivedPackage
	Screen::ExportUpdateLog
	Screen::GoogleMap
	Screen::TweetStreamSearch
/)
{
	$c->{plugins}{$pluginid}{params}{disable} = 0;
}


$c->{plugins}->{"Workflow::View"}->{appears}->{key_tools} = 100;
$c->{plugins}->{"Workflow::Edit"}->{appears}->{key_tools} = 200;
$c->{plugins}->{"RequestTweetStreamExport"}->{appears}->{key_tools} = 300;

#turn off menus that aren't related to twitter harvesting
if ($c->{tweepository_simplify_menus})
{
	$c->{plugins}{"Screen::DataSets"}{appears}{key_tools} = undef;
	$c->{plugins}{"Screen::Items"}{params}{disable} = 1;
	$c->{plugins}{"Screen::User::SavedSearches"}{params}{disable} = 1;
	$c->{plugins}{"Screen::Review"}{params}{disable} = 1;
	$c->{plugins}{"Screen::Staff::EPrintSearch"}{params}{disable} = 1;
	$c->{plugins}{"Screen::Staff::IssueSearch"}{params}{disable} = 1;
	$c->{plugins}{"Screen::Staff::HistorySearch"}{params}{disable} = 1;
	$c->{plugins}{"Screen::Admin::RegenAbstracts"}{params}{disable} = 1;
	$c->{plugins}{"Screen::Admin::RegenViews"}{params}{disable} = 1;
	$c->{plugins}{"Screen::Subject::Edit"}{params}{disable} = 1;
	$c->{plugins}{"Screen::MetaField::Listing"}{params}{disable} = 1;

	$c->{plugins}->{"Screen::FirstTool"}->{params}->{default} = "ManageTweetstreamsLink";
}

$c->{block_map} = {
#UpdateTweetStreams is the highest priority process, so it sits on top of eveything.
#RecountTweetStreams is run by hand if something goes wrong with the running counts that UpdateTweetStreams does
	'RecountTweetStreams' => [qw/ UpdateTweetStreams /],
	'UpdateTweetStreams' => [qw/ RecountTweetStreams /],

#UpdateTweetStreamAbstracts is the nightly job, and it will give way to the above processes to avoid hitting the database and delaying UpdateTweetStreams
	'UpdateTweetStreamAbstracts' => [qw/ RecountTweetStreams UpdateTweetStreams /],


#User Requested Package -- blocked by processes above as they keep the repository running
	'ExportTweetStreamPackage' => [qw/ RecountTweetStreams UpdateTweetStreams UpdateTweetStreamAbstracts /],

#Retiring Tweetstreams -- lowest priority
	'DeactivateTweetStreams' => [qw/ RecountTweetStreams UpdateTweetStreams UpdateTweetStreamAbstracts ExportTweetStreamPackage /],
	'ArchiveTweetStreams' => [qw/ RecountTweetStreams UpdateTweetStreams UpdateTweetStreamAbstracts ExportTweetStreamPackage DeactivateTweetStreams /]
};

#aggregation of metadata
#maps name of field in tweet to name of aggregate field in tweetstream
#also controls whether the field will get tidied in the cache (throw away less frequent values to save space)
$c->{'update_tweetstream_abstracts'}->{fieldmap} = {
        'from_user' => { fieldname => 'top_from_users', subname => 'from_user', tidy => 1 },
        'created_at' => { fieldname => '##process_as_exception', tidy => 0 },
        'hashtags' => { fieldname => 'top_hashtags', subname => 'hashtag', tidy => 1},
        'tweetees' => { fieldname => 'top_tweetees', subname => 'tweetee', tidy => 1},
        'urls_from_text' => { fieldname => 'top_urls_from_text', subname => 'url_from_text', tidy => 1},
	'in_reply_to_status_id' => { fieldname => 'top_reply_tos', subname => 'reply_to', tidy => 1 },
	'retweeted_status_id' => { fieldname => 'top_retweeted', subname => 'retweeted', tidy => 1 },
};



#tweetstream latest_tool
$c->{tweetstream_latest_tool_modes} = {
        default => {
		citation => "default",
               filters => [
                       { meta_fields => [ "tweet_count" ], value => "1-" }
               ],

	}
};

#set up the datasets
$c->{datasets}->{tweet} = {
	class => "EPrints::DataObj::Tweet",
	sqlname => "tweet",
	sql_counter => "tweetid",
	import => 1,
	index => 0,
};

$c->{datasets}->{tweetstream} = {
	class => "EPrints::DataObj::TweetStream",
	sqlname => "tweetstream",
	sql_counter => "tweetstreamid",
	import => 1,
	index => 1,
};


#lightweight dataobj for storing requests for export
$c->{datasets}->{tsexport} = {
	class => "EPrints::DataObj::TweetStreamExport",
	sqlname => "tsexport",
	sql_counter => "tsexportid",
	import => 1,
	index => 0,
};

$c->add_dataset_field( 'tsexport', { name=>"tsexportid", type=>"counter", required=>1, import=>0, can_clone=>1, sql_counter=>"tsexportid" }, );
$c->add_dataset_field( 'tsexport', { name=>"tweetstream", type=>"itemref", datasetid=> 'tweetstream', required => 1 }, );
$c->add_dataset_field( 'tsexport', { name=>"userid", type=>"itemref", datasetid=>"user", required=>1 }, );
$c->add_dataset_field( 'tsexport', { name=>"status", type=>"set", options => [qw( pending running finished )] }, );
$c->add_dataset_field( 'tsexport', 
{
	name=>"datestamp", type=>"time", required=>0, import=>0,
	render_res=>"minute", render_style=>"short", can_clone=>0
} );
$c->add_dataset_field( 'tsexport',
{
	name=>"date_completed", type=>"time", required=>0, import=>0,
	render_res=>"minute", render_style=>"short", can_clone=>0
} );



#base metadata
$c->add_dataset_field( 'tweet', { name=>"tweetid", type=>"counter", required=>1, import=>0, can_clone=>1, sql_counter=>"tweetid" }, );
$c->add_dataset_field( 'tweet', { name=>"twitterid", type=>"bigint", required=>1 }, );
#$c->add_dataset_field( 'tweet', { name=>"datestamp", type=>"date" }, ); #stores the creation time of the object
$c->add_dataset_field( 'tweet', { name=>"json_source", type=>"storable", required=>1, render_value => 'EPrints::DataObj::Tweet::render_json_source' }, ); #full source kept for futureproofing

#extracted tweet metadata
$c->add_dataset_field( 'tweet', { name=>"text", type=>"text" }, );
$c->add_dataset_field( 'tweet', { name=>"from_user", type=>"text", render_value => 'EPrints::DataObj::Tweet::render_from_user' }, );
$c->add_dataset_field( 'tweet', { name=>"from_user_id", type=>"bigint" }, );
$c->add_dataset_field( 'tweet', { name=>"in_reply_to_status_id", type=>"bigint" }, );
$c->add_dataset_field( 'tweet', { name=>"retweeted_status_id", type=>"bigint" }, );
$c->add_dataset_field( 'tweet', { name=>"profile_image_url", type=>"url", render_value => 'EPrints::DataObj::Tweet::render_profile_image_url' }, );
$c->add_dataset_field( 'tweet', { name=>"iso_language_code", type=>"text" }, );
$c->add_dataset_field( 'tweet', { name=>"source", type=>"text" }, );
$c->add_dataset_field( 'tweet', { name=>"created_at", type=>"time"}, );
$c->add_dataset_field( 'tweet', { name=>"coordinates", type=>"text", multiple=>1}, );

#value added extraction and enrichment
$c->add_dataset_field( 'tweet', { name=>"text_is_enriched", type=>"boolean" }, );
$c->add_dataset_field( 'tweet', { name=>"text_enriched", type=>"longtext", render_value => 'EPrints::DataObj::Tweet::render_text_enriched' }, );
$c->add_dataset_field( 'tweet', { name=>"tweetees", type=>"text", multiple=>1 }, );
$c->add_dataset_field( 'tweet', { name=>"hashtags", type=>"text", multiple=>1 }, );

#store URLs from tweet
$c->add_dataset_field( 'tweet', { name=>"urls_from_text", type=>"url", multiple => 1 }, );

#store URL hops -- no longer used, but is valuable data.  Should be reenabled later.
$c->add_dataset_field( 'tweet', { name=>"target_urls", type=>"url", multiple => 1 }, );
#store URL hops -- no longer used, but is valuable data.  Should be reenabled later.
$c->add_dataset_field( 'tweet', { 
	name=>"url_redirects",
	type=>"compound",
	multiple=>1,
	fields=>[
		{
			sub_name=>"url",
			type=>"url",
		},
		{
			sub_name=>"redirects_to",
			type=>"url",
		},
	]
}, );
#a list of tweetstreams to which this tweet belongs
#volatile to speed up operations which add and remove tweets from tweetstreams
$c->add_dataset_field( 'tweet', { name=>"tweetstreams", type=>"itemref", datasetid=> 'tweetstream', required => 1, multiple => 1, volatile => 1 }, );

#a flag to prevent enrichment being done more than once on commit
$c->add_dataset_field( 'tweet', { name=>"newborn", type=>"boolean"}, );



#system metadata
$c->add_dataset_field( 'tweetstream', { name=>"tweetstreamid", type=>"counter", required=>1, import=>0, can_clone=>1, sql_counter=>"tweetstreamid" }, );
$c->add_dataset_field( 'tweetstream', { name=>"userid", type=>"itemref", datasetid=>"user", required=>1 }, );
$c->add_dataset_field( 'tweetstream', { name =>"status", type => 'set', options => [ 'active', 'inactive', 'archived' ] } );

#core metadata (set by user)
$c->add_dataset_field( 'tweetstream', { name=>"search_string", type=>"text", required=>"yes" }, );
$c->add_dataset_field( 'tweetstream', { name=>"geocode", type=>"text" }, );
$c->add_dataset_field( 'tweetstream', { name=>"expiry_date", type=>"date", required=>"yes" }, );
$c->add_dataset_field( 'tweetstream', { name=>"start_date", type=>"date" } );
$c->add_dataset_field( 'tweetstream', { name => "title", type=>'text' }, );
$c->add_dataset_field( 'tweetstream', { name => "abstract", type=>'longtext' }, );
$c->add_dataset_field( 'tweetstream', { name => "project_title", type=>'text' }, );

#state metadata (counts and stuff to avoid database queries)
$c->add_dataset_field( 'tweetstream', { name=>"tweet_count", type=>'bigint', volatile=>1} );
$c->add_dataset_field( 'tweetstream', { name=>"oldest_tweets", type=>"itemref", datasetid=>'tweet', multiple => 1, render_value => 'EPrints::DataObj::TweetStream::render_tweet_field' }, );
$c->add_dataset_field( 'tweetstream', { name=>"newest_tweets", type=>"itemref", datasetid=>'tweet', multiple => 1, render_value => 'EPrints::DataObj::TweetStream::render_tweet_field' }, );


#digest information store anything that appears more than once.
$c->add_dataset_field( 'tweetstream', { 
	name => "top_hashtags", type=>"compound", multiple=>1,
	'fields' => [
	{
		'sub_name' => 'hashtag',
		'type' => 'text',
	},
	{
		'sub_name' => 'count',
		'type' => 'int',
	}],
	render_value => 'EPrints::DataObj::TweetStream::render_top_tag_cloud',
},);
$c->add_dataset_field('tweetstream',  {
	name => "top_from_users", type=>"compound", multiple=>1,
	'fields' => [
	{
		'sub_name' => 'from_user',
		'type' => 'text',
	},
	{
		'sub_name' => 'profile_image_url',
		'type' => 'url',
	},
	{
		'sub_name' => 'count',
		'type' => 'int',
	}],
	render_value => 'EPrints::DataObj::TweetStream::render_top_field',
},);
$c->add_dataset_field('tweetstream',  { name => "top_tweetees", type=>"compound", multiple=>1,
	'fields' => [
	{
		'sub_name' => 'tweetee',
		'type' => 'text',
	},
	{
		'sub_name' => 'count',
		'type' => 'int',
	}
	],
	render_value => 'EPrints::DataObj::TweetStream::render_top_tag_cloud',
},);
$c->add_dataset_field('tweetstream',  { name => "top_urls_from_text", type=>"compound", multiple=>1,
	'fields' => [
	{
		'sub_name' => 'url_from_text',
		'type' => 'url',
	},
	{
		'sub_name' => 'count',
		'type' => 'int',
	}
	],
	render_value => 'EPrints::DataObj::TweetStream::render_top_field',
},);

$c->add_dataset_field('tweetstream',  { name => "top_reply_tos", type=>"compound", multiple=>1,
	'fields' => [
	{
		'sub_name' => 'reply_to',
		'type' => 'bigint',
	},
	{
		'sub_name' => 'count',
		'type' => 'int',
	}
	],
	render_value => 'EPrints::DataObj::TweetStream::render_top_field',
},);

$c->add_dataset_field('tweetstream',  { name => "top_retweeted", type=>"compound", multiple=>1,
	'fields' => [
	{
		'sub_name' => 'retweeted',
		'type' => 'bigint',
	},
	{
		'sub_name' => 'count',
		'type' => 'int',
	}
	],
	render_value => 'EPrints::DataObj::TweetStream::render_top_field',
},);

$c->add_dataset_field('tweetstream', { name => 'newest_coordinates', type => 'compound', multiple => 1, 
	'fields' => [
	{
		'sub_name' => 'lon',
		'type' => 'text',
	},
	{
		'sub_name' => 'lat',
		'type' => 'text',
	}
	],
	render_value => 'EPrints::DataObj::TweetStream::render_newest_coordinates',
});

#for creation of the bar chart
$c->add_dataset_field( 'tweetstream', { name => "frequency_period", type => 'set', options => [ 'daily', 'weekly', 'monthly', 'yearly' ] }, );
$c->add_dataset_field( 'tweetstream', { name => "frequency_values", type => 'compound', multiple=>1,
	'fields' => [
	{
		'sub_name' => 'label',
		'type' => 'text',
	},
	{
		'sub_name' => 'value',
		'type' => 'int',
	}
	],
	render_value => 'EPrints::DataObj::TweetStream::render_top_frequency_values',
},);

#for generating CSV, these store the highest count of each of the multiple fields
$c->add_dataset_field( 'tweetstream', { name => "hashtags_ncols", type=>'int', volatile => '1' }, );
$c->add_dataset_field( 'tweetstream', { name => "tweetees_ncols", type=>'int', volatile => '1' }, );
$c->add_dataset_field( 'tweetstream', { name => "urls_from_text_ncols", type=>'int', volatile => '1' }, );


#fields used to render things on the abstract page
$c->add_dataset_field( 'tweetstream', { name=>"rendered_tweetlist", virtual=> 1, type=>"int", render_value => 'EPrints::DataObj::TweetStream::render_tweet_list' }, );
$c->add_dataset_field( 'tweetstream', { name => "tools", type=>'boolean', virtual => '1', render_value => 'EPrints::DataObj::TweetStream::render_tools' }, );
$c->add_dataset_field( 'tweetstream', { name => "exports", type=>'boolean', virtual => '1', render_value => 'EPrints::DataObj::TweetStream::render_exports' }, );

#fields for exporting to a Web Observatory
$c->add_dataset_field('tweetstream', { name => 'web_observatory_export', type => 'set', options => [ 'yes', 'no' ]});
$c->add_dataset_field('tweetstream', { name => 'web_observatory_id', type => 'text'});
$c->add_dataset_field('tweetstream', { name => 'web_observatory_collection', type => 'text'});

{
package EPrints::DataObj::Tweet;

our @ISA = ( 'EPrints::DataObj' );

use EPrints;
use EPrints::Search;
use JSON;
use Date::Parse;
use URI::Find;
use HTML::Entities;

use strict;


######################################################################
=pod

=item $tweet = EPrints::DataObj::Tweet->new( $session, $tweetid )

Load the tweet with the ID of $tweetid from the database and return
it as an EPrints::DataObj::Tweet object.

=cut
######################################################################

sub new
{
	my( $class, $session, $tweetid ) = @_;

	return $session->get_database->get_single( 
		$session->get_repository->get_dataset( "tweet" ),
		$tweetid );
}


######################################################################
=pod

=item $tweet = EPrints::DataObj::Tweet->new_from_data( $session, $data )

Construct a new EPrints::DataObj::Tweet object based on the $data hash 
reference of metadata.

Used to create an object from the data retrieved from the database.

=cut
######################################################################

sub new_from_data
{
	my( $class, $session, $known ) = @_;

	return $class->SUPER::new_from_data(
			$session,
			$known,
			$session->get_repository->get_dataset( "tweet" ) );
}



######################################################################
# =pod
# 
# =item $dataobj = EPrints::DataObj->create_from_data( $session, $data, $dataset )
# 
# Create a new object of this type in the database. 
# 
# $dataset is the dataset it will belong to. 
# 
# $data is the data structured as with new_from_data.
# 
# =cut
######################################################################

sub create_from_data
{
	my( $class, $session, $data, $dataset ) = @_;

	my $new_tweet = $class->SUPER::create_from_data( $session, $data, $dataset );

	#$new_tweet->set_value( "datestamp", EPrints::Time::get_iso_timestamp() );

	$new_tweet->update_triggers();
	
	if( scalar( keys %{$new_tweet->{changed}} ) > 0 )
	{
		# Remove empty slots in multiple fields
		$new_tweet->tidy;

		# Write the data to the database
		$session->get_database->update(
			$new_tweet->{dataset},
			$new_tweet->{data},
			$new_tweet->{changed} );
	}

	$session->get_database->counter_minimum( "tweetid", $new_tweet->get_id );

	return $new_tweet;
}

######################################################################
=pod

=item $dataset = EPrints::DataObj::Tweet->get_dataset_id

Returns the id of the L<EPrints::DataSet> object to which this record belongs.

=cut
######################################################################

sub get_dataset_id
{
	return "tweet";
}

######################################################################
=pod

=item $defaults = EPrints::DataObj::Tweet->get_defaults( $session, $data )

Return default values for this object based on the starting data.

=cut
######################################################################

# inherits


######################################################################
=pod

=item $tweet = EPrints::DataObj::Tweet::tweet_with_twitterid( $session, $twitterid )

Return the EPrints::tweet with the specified $twitterid, or undef if they
are not found.

=cut
######################################################################

sub tweet_with_twitterid
{
	my( $repo, $twitterid ) = @_;

	my $dataset = $repo->dataset( "tweet" );

	my $results = $dataset->search(
		filters => [
			{
				meta_fields => [qw( twitterid )],
				value => $twitterid, match => "EX"
			}
		]);

	return $results->item( 0 );
}


######################################################################
=pod

=item $tweet->commit( [$force] )

Write this object to the database.

If $force isn't true then it only actually modifies the database
if one or more fields have been changed.

If this item is not in any tweetstreams, then remove it.

=cut
######################################################################

sub commit
{
	my( $self, $force ) = @_;

	my $repo = $self->repository;
	$self->set_value('newborn', 'TRUE') if !$self->is_set('newborn');

	$self->update_triggers();

	if ($self->get_value('newborn') eq 'TRUE')
	{
		if ($self->is_set('json_source')) #should always be true, but just in case....
		{
			$self->process_json;
		}
		$self->set_value('tweetees', $self->tweetees);
		$self->set_value('hashtags', $self->hashtags);
		$self->set_value('newborn', 'FALSE');
		$self->set_value('text_is_enriched', 'FALSE');
		$self->enrich_text;
	}

	if( !defined $self->{changed} || scalar( keys %{$self->{changed}} ) == 0 )
	{
		# don't do anything if there isn't anything to do
		return( 1 ) unless $force;
	}

	if ($self->is_set('web_observatory_id'))
	{
		my $wo_id = $self->value('web_observatory_id');
		my $wo_conf = $repo->config('web_observatories');
		if (!$wo_conf->{$wo_id})
		{
			$self->set_value('web_observatory_id'); #unset
			#notify?
		}
		else
		{	
			my $current_username = $repo->current_user->value('username');
			my $allowed = 0;		
			foreach	my $username (@{$wo_conf->{$wo_id}->{authorised_users}})
			{
				if ($username eq $current_username)
				{
					$allowed = 1;
					last;
				}
			}
			if (!$allowed)
			{
				$self->set_value('web_observatory_id'); #unset
				#need to notify?
			}
		}
	}


	my $success = $self->SUPER::commit( $force );
	
	return( $success );
}


######################################################################
=pod

=item $success = $tweet->remove

Remove this tweet from the database.

=cut
######################################################################

sub remove
{
	my( $self ) = @_;
	
	my $success = 1;

	# remove tweet record
	my $tweet_ds = $self->{session}->get_repository->get_dataset( "tweet" );
	$success = $success && $self->{session}->get_database->remove(
		$tweet_ds,
		$self->get_value( "tweetid" ) );
	
	return( $success );
}

#takes an array ref and a (scalar or array ref) and returs an arrayref containing only one of each value
sub dedup_add
{
	my ($self, $arr_ref, $val) = @_;

	if (not ref $val)
	{
		$val = [$val];
	}

	push @{$arr_ref}, @{$val};

	my %dedup;
	foreach (@{$arr_ref})
	{
		$dedup{$_} = 1;
	}

	my @deduped = keys %dedup;

	return \@deduped;
}


#remove from the passed tweetstream

sub remove_from_tweetstream
{
	my ($self, $tweetstream) = @_;

	my $new_tweetstreams = [];

	foreach my $id (@{$self->get_value('tweetstreams')})
	{
		push @{$new_tweetstreams}, $id unless ( $id == $tweetstream->id );
	}

	if (scalar @{$new_tweetstreams})
	{
		$self->set_value('tweetstreams', $new_tweetstreams);
		$self->commit;
	}
	else
	{
		$self->remove;
	}
}


######################################################################
=pod

=item $success = $tweet->process_json

Extract tweet metadata from the source json

=cut
######################################################################

sub process_json
{
	my ( $self ) = @_;

	return 0 unless $self->is_set('json_source');

	my $tweet_data = $self->get_value('json_source');

	#pull the data out and stick it in metafields
	foreach my $fieldname (qw/ text from_user from_user_id profile_image_url iso_language_code source in_reply_to_status_id /)
	{
		if ($tweet_data->{$fieldname})
		{
			$self->set_value($fieldname, $tweet_data->{$fieldname});
		}

	}

	if (
		$tweet_data->{coordinates}
		&& $tweet_data->{coordinates}->{coordinates}
	)
	{
		my $lon = $tweet_data->{coordinates}->{coordinates}->[0];
		my $lat = $tweet_data->{coordinates}->{coordinates}->[1];

		$self->set_value('coordinates', [$lat,$lon]);
	}

	if (exists $tweet_data->{retweeted_status})
	{
		my $retweeted_status_id = $tweet_data->{retweeted_status}->{id};
		$self->set_value('retweeted_status_id', $retweeted_status_id) if $retweeted_status_id;
	}

	#API v1.1 hacks
	if ($tweet_data->{metadata}->{iso_language_code})
	{
		$self->set_value('iso_language_code', $tweet_data->{metadata}->{iso_language_code});
	}
	if ($tweet_data->{user}->{screen_name})
	{
		$self->set_value('from_user', $tweet_data->{user}->{screen_name});
		$self->set_value('from_user_id', $tweet_data->{user}->{id});
		$self->set_value('profile_image_url', $tweet_data->{user}->{profile_image_url});
	}


	#convert created at to eprints timestame
	my $time = str2time($tweet_data->{created_at});
	$self->set_value('created_at',EPrints::Time::get_iso_timestamp($time));

	return 1;
}

sub tweetees
{
	my ($self) = @_;

	my $message = $self->get_value('text');
	return [] unless $message;

	my @tweetees = ($message =~ m/\@[A-Za-z0-9-_]+/g);
	return \@tweetees;
}

sub hashtags
{
	my ($self) = @_;

	my $message = $self->get_value('text');
	return [] unless $message;

	my @tags = ($message =~ m/#[A-Za-z0-9-_]+/g);
	return \@tags;
}

sub enrich_text
{
        my ($self) = @_;

        my $message = $self->get_value('text');
        return unless $message;

	my $url_map = {};
	my $expanded_urls = [];
	my $tweet_data = $self->get_value('json_source');
	if ($tweet_data->{entities})
	{
		foreach my $url_data (@{$tweet_data->{entities}->{urls}})
		{
			$url_map->{$url_data->{url}} = $url_data->{expanded_url};
			push @{$expanded_urls}, $url_data->{expanded_url};
		}
	}

        my $expanded_message = $message;

	my @URLS;

        my $finder = URI::Find->new(sub{
                my($uri, $orig_uri) = @_;

		my $url_to_use = $orig_uri;

		if ($url_map->{$orig_uri})
		{
			$url_to_use = $url_map->{$orig_uri};
		}

		push @URLS, $url_to_use;

                #escape HASH and AT symbols in the urls so that regexp for user and hashtag insertion don't change them
                $orig_uri =~ s/#/ESCAPED_HASH/g;
                $orig_uri =~ s/\@/ESCAPED_AT/g;

                return '<a href="'.$url_to_use.'">'.$orig_uri.'</a>';
        });
        $finder->find(\$expanded_message);

        #now insert links to hashtags and usernames - how do we stop this from modifying text inside a long URL
        $expanded_message =~ s|\@([A-Za-z0-9-_]+)|<a href="http://twitter.com/$1">$&</a>|g;
        $expanded_message =~ s|#([A-Za-z0-9-_]+)|<a href="http://twitter.com/hashtag/$1">$&</a>|g;

        #now unescape HASH and AT
        $expanded_message =~ s/ESCAPED_HASH/#/g;
        $expanded_message =~ s/ESCAPED_AT/\@/g;

	#figure out if we have urls from entities (backwards compatibility to when twitter didn't supply entities)
	if (scalar @{$expanded_urls})
	{
		$self->set_value('urls_from_text', $self->remove_bad_urls($expanded_urls));
	}
	else
	{
		$self->set_value('urls_from_text', $self->remove_bad_urls(\@URLS));
	}

        $self->set_value('text_enriched', "$expanded_message"); #should have all the links expanded out now.

	$self->set_value('text_is_enriched', 'TRUE');
}

#a number of URLs are detected that are not usefule.  Remove these.
sub remove_bad_urls
{
	my ($self, $urls) = @_;

	my $good_urls;

	my $bad_urls = {
		'http://' => 1,
		'http:/' => 1,
		'http:' => 1,
		'http://t.c' => 1,
		'http://t' => 1,
		'http://t.co' => 1, #technically valid, but almost always a mistake
		'https://' => 1,
		'https:/' => 1,
		'https:' => 1,
		'https://t.c' => 1,
		'https://t' => 1,
		'https://t.co' => 1, #technically valid, but almost always a mistake
	};

	foreach my $url (@{$urls})
	{
		next if $bad_urls->{$url};
 		push @{$good_urls}, $url;
	}

	return $good_urls;
}

sub render_li
{
	my ($self) = @_;

	my $xml = $self->{session}->xml;
	my $twitterid = $self->get_value('twitterid');

	my $li = $xml->create_element('li', class=>'tweet', id=>'tweet-' . $twitterid);
	$li->appendChild($self->render_span);
	return $li;
}


sub render_span
{
	my ( $self ) = @_;

	my $xml = $self->{session}->xml;

	my $twitterid = $self->get_value('twitterid');

	my $span = $xml->create_element('span', class=>'tweet-body');

	my $anchor = $xml->create_element('a', name => $twitterid);
	$span->appendChild($anchor);

	$span->appendChild($self->render_value('profile_image_url'));

	my $text_part = $xml->create_element('span', class=>'tweet-text-part');
	$span->appendChild($text_part);

	$text_part->appendChild($self->render_value('from_user'));

	$text_part->appendChild($xml->create_text_node(' '));

	my $text_span = $xml->create_element('span', class=>'text', id=>'tweet-'.$self->get_value('twitterid'));
	$text_part->appendChild($self->render_value('text_enriched'));

	$text_part->appendChild($xml->create_text_node(' '));

	my $meta_span = $xml->create_element('span', class=>'meta');
	$meta_span->appendChild($self->render_value('created_at'));
	$meta_span->appendChild($xml->create_element('br'));
	$meta_span->appendChild($xml->create_text_node('Tweet ID: ' . $self->get_value('twitterid')));
	$text_part->appendChild($meta_span);

	return $span;
}

sub render_json_source
{
        my( $session , $field , $value , $alllangs , $nolink , $object ) = @_;

	my $json = JSON->new->allow_nonref;
	my $json_data = $json->pretty->encode($value);
	return EPrints::Extras::render_preformatted_field($session, $field, $json_data, $alllangs , $nolink , $object);
}


sub render_profile_image_url
{
        my( $session , $field , $value , $alllangs , $nolink , $object ) = @_;

	my $xml = $session->xml;

	my $span = $xml->create_element('span', class=>'author-thumb');
	my $a = $xml->create_element('a', href=>'http://twitter.com/' . $object->get_value('from_user'));
	$a->appendChild($xml->create_element('img', height=>"48", width=>"48", class=>'author-thumb', src=>$value, onerror=>"this.onerror=''; this.src='/images/tweepository/egg.png';"));
	$span->appendChild($a);

	return $span;
}

sub render_from_user
{
        my( $session , $field , $value , $alllangs , $nolink , $object ) = @_;

	my $xml = $session->xml;

	my $a = $xml->create_element('a', href=>'http://twitter.com/' . $value);
	$a->appendChild($xml->create_text_node($value));
	return $a;
}

sub render_text_enriched
{
        my( $session , $field , $value , $alllangs , $nolink , $object ) = @_;

	return $object->render_value('text') unless $value; #enrich_text may not have been called

	my $xml = $session->xml;

	my $text_span = $xml->create_element('span', class=>'text', id=>'tweet-'.$object->get_value('twitterid'));
#I'm not sure I'm doing this right, but I've found a way that works.  What's the EPrints way of doing this?

	my $doc = eval { EPrints::XML::parse_xml_string( "<fragment>".$value."</fragment>" ); };
#	my $doc = eval { EPrints::XML::parse_xml_string( "<fragment>".decode_entities($value)."</fragment>" ); };

	if( $@ )
	{
		$session->get_repository->log( "Error rendering text_enriched on tweet " . $object->get_id . " for text:\n\t$value\nError:\n\t$@" );

		return $object->render_value('text'); #fall back to the simple text value #fall back to the simple text value #fall back to the simple text value 
	}
	else
	{
		my $top = ($doc->getElementsByTagName( "fragment" ))[0];
		foreach my $node ( $top->getChildNodes )
		{
			$text_span->appendChild(
			$session->clone_for_me( $node, 1 ) );
		}
		EPrints::XML::dispose( $doc );
	}
	return $text_span;

}


sub data_for_export
{
	my ($self) = @_;

	my $data;

	if ($self->is_set('json_source'))
	{
		$data = $self->value('json_source');
	}
	else #should never be true, but let's have something to fall back to/
	{
		foreach my $fieldname (qw/ from_user text created_at /) 
		{
			$data->{$fieldname} = $self->value($fieldname) if $self->is_set($fieldname);
		}
	}
	foreach my $fieldname (qw/ text_enriched urls_from_text /)
	{
		$data->{eprints_value_added}->{$fieldname} = $self->value($fieldname) if ($self->is_set($fieldname));
	}

	return $data;
}


1;


}


{
package EPrints::DataObj::TweetStream;

our @ISA = ( 'EPrints::DataObj' );

use EPrints;
use EPrints::Search;
use Date::Calc qw/ Week_of_Year Delta_Days Add_Delta_Days /;
use File::Path qw/ make_path /;

use strict;


#add an arrayref of tweet objects to this tweetstream
#note that this function commits the object
sub add_tweets
{
	my ($self, $tweets) = @_;

	return unless scalar @{$tweets}; #paranoia -- make sure there are tweets in the array

	my $repo = $self->repository;
	my $tweet_ds = $repo->dataset('tweet');

	#we may need to update the highest and lowest.  Initialise varables to assist with this.
	my $refresh_needed = {};
	my $highest_and_lowest = {};
	foreach my $fieldname(qw/ newest_tweets oldest_tweets /)
	{
		$refresh_needed->{$fieldname} = 0;
		$highest_and_lowest->{$fieldname} = 0;
		if ($self->is_set($fieldname))
		{
			my $tweetids = $self->value($fieldname);

			my $index = 0;
			$index = -1 if $fieldname eq 'newest_tweets';

			my $tweet = $tweet_ds->dataobj($tweetids->[$index]);
			$highest_and_lowest->{$fieldname} = $tweet->value('twitterid') if $tweet;
		}
		$refresh_needed->{$fieldname} = 1 unless $highest_and_lowest->{$fieldname};
	}

	#keep a rolling count of the number of tweets -- initialise the count
	my $tweet_count;
	if (!$self->is_set('tweet_count'))
	{
		$tweet_count = $self->count_with_query;
	}
	else
	{
		$tweet_count = $self->value('tweet_count');
	}	

	#now add each tweet
	foreach my $tweet (@{$tweets})
	{
		if ($self->_add_tweet($tweet))
		{
			$tweet_count++;

			#do we need to update oldest or newest?
			$refresh_needed->{newest_tweets} = 1 if ($tweet->value('twitterid') > $highest_and_lowest->{newest_tweets});
			$refresh_needed->{oldest_tweets} = 1 if ($tweet->value('twitterid') < $highest_and_lowest->{oldest_tweets});

			#collect the most recent N coordinates
			if ($tweet->is_set('coordinates'))
			{
				my $newest_coords = $self->value('newest_coordinates');
				$newest_coords = [] unless defined $newest_coords;

				my $coords = $tweet->value('coordinates');
				push @{$newest_coords}, { lat => $coords->[0], lon => $coords->[1] };

				my $n = $repo->config('tweepository_newest_coordinates_n');
				$n = 100 unless $n; #need config variable

				if ( (scalar @{$newest_coords}) > $n)
				{
					my @c = @{$newest_coords}[-$n..-1];
					$newest_coords = \@c;
				}
				$self->set_value('newest_coordinates', $newest_coords);
			}

			#update ncols fields if necessary
			foreach my $tweet_fieldname (qw/ hashtags tweetees urls_from_text /)
			{
				if ($tweet->is_set($tweet_fieldname))
				{
					my $val = $tweet->value($tweet_fieldname);
					my $n = scalar @{$val};

					my $tweetstream_fieldname = $tweet_fieldname . '_ncols';
					if ($self->is_set($tweetstream_fieldname))
					{
						my $ts_n = $self->value($tweetstream_fieldname);
						if ($n > $ts_n)
						{
							$self->set_value($tweetstream_fieldname, $n);
						}
					}
					else
					{
						$self->set_value($tweetstream_fieldname, $n);
					}
				}
			}
		}
	}

	#update oldest and newest tweets if needed
	foreach my $fieldname (qw/ newest_tweets oldest_tweets /)
	{
		if ($refresh_needed->{$fieldname})
		{
			my $val = $self->_generate_oldest_or_youngest_tweets($fieldname, $tweets);
			$self->set_value($fieldname, $val);
		}
	}

	$self->set_value('tweet_count', $tweet_count);
	$self->commit;
}

sub _merge_tweets_with_tweet_field
{
	my ($self, $obj_array, $fieldid) = @_;

	my $repo = $self->repository;
	my $tweet_ds = $repo->dataset('tweet');

	my @new_array;

	foreach my $obj (@{$obj_array})
	{
		push @new_array, $obj;
	}

	if ($self->is_set($fieldid))
	{
		my $objids = $self->value($fieldid);
		foreach my $objid (@{$objids})
		{
			my $obj = $tweet_ds->dataobj($objid);
			push @new_array, $obj if $obj;
		}
	}
	return \@new_array;
}

sub _generate_oldest_or_youngest_tweets
{
	my ($self, $fieldname, $new_tweets) = @_;
	my $repo = $self->repository;

	my $all_tweets = $self->_merge_tweets_with_tweet_field($new_tweets, $fieldname);

	my $n = 100;
	$n = $repo->config('tweetstream_tweet_renderopts','n_oldest') if ($fieldname eq 'oldest_tweets');
	$n = $repo->config('tweetstream_tweet_renderopts','n_newest') if ($fieldname eq 'newest_tweets');

	my @sorted_tweets = sort {$a->value('twitterid') <=> $b->value('twitterid')} @{$all_tweets};

	if ($fieldname eq 'newest_tweets')
	{
		@sorted_tweets = @sorted_tweets[(1-$n)..-1]; #get highest IDs (RHS of the array);
	}
	else
	{
		@sorted_tweets = @sorted_tweets[0..($n-1)]; #get lowest IDs (LHS of the array);
	}

	my $tweetids = [];
	foreach my $tweet (@sorted_tweets)
	{
		next unless $tweet; #safety check
		push @{$tweetids}, $tweet->value('tweetid');
	}

	return $tweetids;
}


#not intended to be called, use add_tweets instead
sub _add_tweet
{
	my ($self, $tweet) = @_;

	#don't add if it's already in this tweetstream
	my $tsids = $tweet->value('tweetstreams');
	foreach my $tsid (@{$tsids})
	{
		return 0 if $tsid == $self->value('tweetstreamid');
	}

	push @{$tsids}, $self->value('tweetstreamid');
	$tweet->set_value('tweetstreams', $tsids);
	$tweet->commit;
	return 1;
}


sub pending_package_request
{
	my ($self) = @_;
	return $self->_package_request('pending');
}
sub running_package_request
{
	my ($self) = @_;
	return $self->_package_request('running');
}

sub _package_request
{
	my ($self, $status) = @_;
	my $repo = $self->repository;
	my $ds = $repo->dataset('tsexport');

	my %options;
	$options{filters} = [{
		meta_fields => [qw( tweetstream )],
		value => $self->id,
	},
	{
		meta_fields => [qw( status )],
		value => $status,
	},
	];
	$options{custom_order} = '-tsexportid';

	my $list = $ds->search(%options);

	if ($list->count >= 1)
	{
		return $list->item(0);
	}
	return undef;
}

sub delete_export_package
{
	my ($self) = @_;

	if (-e $self->export_package_filepath)
	{
		unlink $self->export_package_filepath
	}
}

sub export_package_filename
{
	my ($self) = @_;

	my $target_dir = $self->export_package_directory;
	my $filename =  'tweetstream' . $self->id . '_package';
	my $zip_filename = $filename . '.zip';
	my $tar_filename = $filename . '.tar.gz';

	#this code handles the switch from .zip to .tar.gz -- only return .zip for existing packages
	my $zip_path = $target_dir . $zip_filename;
	my $tar_path = $target_dir . $tar_filename;

	#if there's an existing file, return that (there should only ever be one)
	return $zip_filename if -e $zip_path;
	return $tar_filename if -e $tar_path;

	#there isn't an existing one -- we should use zip files for small and tar files for big tweetstreams
	#the tweet count is a rolling total and get incremented every time a tweet is added, so this should be safe
	if ($self->value('tweet_count') > $self->repository->config('tweepository_archive_tar_threshold'))
	{
		return $tar_filename;
	}
	return $zip_filename;
}

sub export_package_directory
{
	my ($self) = @_;

	my $target_dir = $self->repository->config('archiveroot') . '/tweepository_packages/export/';

	make_path($target_dir) unless -d $target_dir;

	return $target_dir;
}

sub update_log_filepath
{
	my ($self) = @_;
	my $repo = $self->repository;

	my $repository = @_;

	my $target_dir = $self->export_package_directory;

	my $filename = 'tweetstream' . $self->id . '_updates.csv';

	return "$target_dir$filename";
}

sub log_update
{
	my ($self, $start_time, $end_time, $count, $status) = @_;

	my $file = $self->update_log_filepath;

	if (!-e $file)
	{
		open FILE, ">$file"; #handle exception
		print FILE EPrints::Plugin::Export::TweetStream::CSV::csv("Start Time", "End Time", "Number of Tweets Added", "End State"); #headings
		close FILE;
	}

	open FILE, ">>$file"; #handle exception
	print FILE EPrints::Plugin::Export::TweetStream::CSV::csv($start_time, $end_time, $count, $status);
}

sub export_package_filepath
{
	my ($self) = @_;
	my $repo = $self->repository;

	my $repository = @_;

	my $target_dir = $self->export_package_directory;
	my $filename = $self->export_package_filename;

	return "$target_dir$filename";
}

sub render_exports
{
        my( $session , $field , $value , $alllangs , $nolink , $object ) = @_;

	return $object->render_exporters;
}

sub render_top_tag_cloud
{
        my( $session , $field , $value , $alllangs , $nolink , $object ) = @_;

	if (!(scalar @{$value}))
	{
		return $session->html_phrase('top_field_no_data');
	}

	my $fieldname = $field->name;

	my $cloud_min = 80;
	my $cloud_max = 200;
	my $range = $cloud_max - $cloud_min;

	#first find the highest to scale all others
	my $highest = 0;
	my $lowest = undef;
	foreach my $v (@{$value})
	{
		$lowest = $v->{count} unless defined $lowest;
		$lowest = $v->{count} if $v->{count} < $lowest;
		$highest = $v->{count} if $v->{count} > $highest;
	}

	my $tags = [];

	foreach my $single_value (@{$value})
	{
		my $label = render_top_lhs($session, $fieldname, $single_value);
		my $size = int( ( log(1+($single_value->{count} - $lowest)) / log(1+($highest-$lowest)) ) * $range ) + $cloud_min;

		my $span = $session->make_element( "span", class => 'tag-cloud-item', style=>"font-size: $size\%" );
		$span->appendChild( $label );
		push @{$tags}, $span;
	}

	my $cloud = $session->xml->create_document_fragment;
	foreach my $tag (sort { lc(EPrints::Utils::tree_to_utf8($a)) cmp lc(EPrints::Utils::tree_to_utf8($b))} @{$tags})
	{
		$cloud->appendChild($tag);
		$cloud->appendChild($session->xml->create_text_node(' '));
	}
	return $cloud;

}


sub render_top_frequency_values
{
        my( $session , $field , $value , $alllangs , $nolink , $object ) = @_;

	if (!(scalar @{$value}))
	{
		return $session->html_phrase('top_field_no_data');
	}


	#first find the highest to scale all others
	my $highest = 0;
	foreach (@{$value})
	{
		$highest = $_->{value} if $_->{value} > $highest;
	}

	my $table = $session->make_element('table', class=>"tweetstream_graph");

	foreach my $pair (@{$value})
	{
		my $tr = $session->make_element('tr');
		$table->appendChild($tr);
		my $th = $session->make_element('th');
		$tr->appendChild($th);
		$th->appendChild($session->make_text($pair->{label}));
		my $td = $session->make_element('td', class => "tweetstream_bar");

		my $width = int (($pair->{value} / $highest) * 100);
		my $div = $session->make_element('div', style => "width: $width%");
		$td->appendChild($div);
		$tr->appendChild($td);

		$td = $session->make_element('td');
		$td->appendChild($session->make_text($pair->{value}));
		$tr->appendChild($td);
	}
	return $table;
}

sub get_url
{
	my ($self) = @_;

	return $self->uri;
}

sub render_newest_coordinates
{
        my( $repo , $field , $value , $alllangs , $nolink , $object ) = @_;

	my $xml = $repo->xml;
	my $plugin = $repo->plugin('Export::TweetStream::GoogleMap');
	my $url = $plugin->dataobj_export_url($object);

	$url =~ s#^[^/]*//#//#;

	my $iframe = $xml->create_element('iframe', id => 'map-iframe', src => $url);

	return $iframe;
}

sub render_top_field
{
        my( $session , $field , $value , $alllangs , $nolink , $object ) = @_;

	if (!(scalar @{$value}))
	{
		return $session->html_phrase('top_field_no_data');
	}

	my $rows;
	my $fieldname = $field->name;

	foreach my $single_value (@{$value})
	{
		my $tr = $session->make_element('tr');
		my $td = $session->make_element('td');
		$tr->appendChild($td);
		$td->appendChild(render_top_lhs($session, $fieldname, $single_value));
		$td = $session->make_element('td');
		$td->appendChild(render_top_rhs($session, $fieldname, $single_value));
		$tr->appendChild($td);

		push @{$rows}, $tr;
	}

	return columned_table($session, $rows, $session->config('tweetstream_tops',$fieldname,'cols'));
}


sub render_top_lhs
{
	my ($session, $fieldname, $stuff) = @_;

	if ($fieldname eq 'top_hashtags')
	{
		my $value = $stuff->{hashtag}; 
		
		my $max_render_len = $session->config('tweetstream_tops',$fieldname,'max_len'); 

		my $hash_stripped_tag = $value;	
		$hash_stripped_tag =~ s/^#//;
		my $url = 'http://twitter.com/hashtag/' . URI::Escape::uri_escape($hash_stripped_tag);

		my $a = $session->make_element('a', href=>$url, title=>$stuff->{count}); 

		if (length $value > $max_render_len) 
		{ 
			my $chars = $max_render_len - 3; 
			$value = substr($value, 0, $chars) . '...'; 
		} 

		$a->appendChild($session->make_text($hash_stripped_tag)); 
		return $a;       
	};

	if ($fieldname eq 'top_urls_from_text')
	{
		my $value = $stuff->{'url_from_text'}; 
		
		my $max_render_len = $session->config('tweetstream_tops',$fieldname,'max_len'); 
		
		my $a = $session->make_element('a', href=>$value, title=>$value);

		if (length $value > $max_render_len) 
		{ 
			my $chars = $max_render_len - 3; 
			$value = substr($value, 0, $chars) . '...'; 
		} 

		$a->appendChild($session->make_text($value)); 
		return $a;       
	};

	if ($fieldname eq 'top_from_users')
	{
		my $base_url = 'http://twitter.com/';
		my $img_url = $stuff->{profile_image_url};
		my $user = $stuff->{from_user};

		my $a = $session->make_element('a', href=>$base_url . $user, title=> $user);
		$a->appendChild($session->make_element('img', height=>"48", width=>"48",src=>$img_url, onerror=>"this.onerror=''; this.src='/images/tweepository/egg.png';"));
		return $a;
	}

	if ($fieldname eq 'top_tweetees')
	{
		my $base_url = 'http://twitter.com/';
		my $user = $stuff->{tweetee};

		my $a = $session->make_element('a', href=>$base_url . $user, title=> $stuff->{count});
		$a->appendChild($session->make_text($user));
		return $a;
	}

	if (
		$fieldname eq 'top_reply_tos'
		|| $fieldname eq 'top_retweeted'
	)
	{
		my $twitterid = $stuff->{reply_to};
		$twitterid = $stuff->{retweeted} if $fieldname eq 'top_retweeted';

		my $tweet = EPrints::DataObj::Tweet::tweet_with_twitterid($session, $twitterid);
		if ($tweet)
		{
			return $tweet->render_span;
		}
		else
		{
			my $xml = $session->xml;
			my $frag = $xml->create_document_fragment;
			my $url = 'http://twitter.com/statuses/' . $twitterid;
			my $link = $session->render_link($url);
			$link->appendChild($xml->create_text_node($twitterid));
			$frag->appendChild($xml->create_text_node('Uncollected tweet with id '));
			$frag->appendChild($link);
			return $frag;
		}

	}

	#we should never get here
	return $session->make_text("$fieldname unhandled in render_top_lhs\n");
}

sub render_top_rhs
{
	my ($session, $fieldname, $stuff) = @_;

	if ($fieldname eq 'top_from_users')
	{
		my $frag = $session->make_doc_fragment;

		my $base_url = 'http://twitter.com/';
		my $img_url = $stuff->{profile_image_url};
		my $user = $stuff->{from_user};

		my $a = $session->make_element('a', href=>$base_url . $user, title=> $user);
		$a->appendChild($session->make_text($user));
		$frag->appendChild($a);
		$frag->appendChild($session->make_element('br'));
		$frag->appendChild($session->make_text($stuff->{count} . ' tweets'));
		return $frag;
	}
	else
	{
		return $session->make_text($stuff->{count});
	}
}

sub columned_table
{
	my ($session, $rows, $ncols ) = @_;

	my $nitems = scalar @{$rows};
	my $col_len = POSIX::ceil( $nitems / $ncols );

	my $table = $session->make_element('table');
	my $tr = $session->make_element('tr');
	$table->appendChild($tr);

	my $inside_table;
	for( my $i=0; $i < $nitems; ++$i )
        {

                if( $i % $col_len == 0 )
		{
			my $td = $session->make_element('td', valign => 'top');
			$tr->appendChild($td);

			$inside_table = $session->make_element('table');
			$td->appendChild($inside_table);

		}
		$inside_table->appendChild($rows->[$i]);
	}
	return $table;
}


######################################################################
=pod

=item $tweet = EPrints::DataObj::Tweet->new( $session, $tweetid )

Load the tweet with the ID of $tweetid from the database and return
it as an EPrints::DataObj::Tweet object.

=cut
######################################################################

sub new
{
	my( $class, $session, $tweetid ) = @_;

#bugfix
$session->{xhtml} = $session->xhtml;

	return $session->get_database->get_single( 
		$session->get_repository->get_dataset( "tweetstream" ),
		$tweetid );
}


######################################################################
=pod

=item $tweet = EPrints::DataObj::Tweet->new_from_data( $session, $data )

Construct a new EPrints::DataObj::Tweet object based on the $data hash 
reference of metadata.

Used to create an object from the data retrieved from the database.

=cut
######################################################################

sub new_from_data
{
	my( $class, $session, $known ) = @_;

	return $class->SUPER::new_from_data(
			$session,
			$known,
			$session->get_repository->get_dataset( "tweetstream" ) );
}


######################################################################
# =pod
# 
# =item $dataobj = EPrints::DataObj->create_from_data( $session, $data, $dataset )
# 
# Create a new object of this type in the database. 
# 
# $dataset is the dataset it will belong to. 
# 
# $data is the data structured as with new_from_data.
# 
# =cut
######################################################################

sub create_from_data
{
	my( $class, $session, $data, $dataset ) = @_;

	my $new_tweetstream = $class->SUPER::create_from_data( $session, $data, $dataset );

	$new_tweetstream->update_triggers();
	
	if( scalar( keys %{$new_tweetstream->{changed}} ) > 0 )
	{
		# Remove empty slots in multiple fields
		$new_tweetstream->tidy;

		# Write the data to the database
		$session->get_database->update(
			$new_tweetstream->{dataset},
			$new_tweetstream->{data},
			$new_tweetstream->{changed} );
	}

	$session->get_database->counter_minimum( "tweetstreamid", $new_tweetstream->get_id );

	return $new_tweetstream;
}

######################################################################
=pod

=item $dataset = EPrints::DataObj::Tweet->get_dataset_id

Returns the id of the L<EPrints::DataSet> object to which this record belongs.

=cut
######################################################################

sub get_dataset_id
{
	return "tweetstream";
}

######################################################################
=pod

=item $defaults = EPrints::DataObj::Tweet->get_defaults( $session, $data )

Return default values for this object based on the starting data.

=cut
######################################################################

# inherits


######################################################################
=pod

=item $tweet->commit( [$force] )

Write this object to the database.

If $force isn't true then it only actually modifies the database
if one or more fields have been changed.

=cut
######################################################################

sub commit
{
	my( $self, $force ) = @_;

	$self->update_triggers();

	$self->set_value('status', 'active') if !$self->is_set('status');
	$self->set_value('tweet_count', 0) if !$self->is_set('tweet_count');

	if (!$self->is_set('title') && $self->is_set('search_string'))
	{
		#sensible default
		$self->set_value('title', 'Twitter Feed for ' . $self->value('search_string'));
	}

	if ($self->is_set('oldest_tweets'))
	{
		my $ids = $self->value('oldest_tweets');
		my $oldest_tweet = $self->repository->dataset('tweet')->dataobj($ids->[0]);
		if ($oldest_tweet)
		{
			my $t = $oldest_tweet->value('created_at');
			$self->set_value('start_date', $t); #I checked the code, you can set a Date field with a Time type, it strips the time off.
		}
	}

	if( !defined $self->{changed} || scalar( keys %{$self->{changed}} ) == 0 )
	{
		# don't do anything if there isn't anything to do
		return( 1 ) unless $force;
	}

	my $success = $self->SUPER::commit( $force );
	
	return( $success );
}


sub highest_twitterid
{
	my ($self) = @_;

	return 0 unless $self->is_set('newest_tweets');

	my $repo = $self->repository;
	my $tweet_ds = $repo->dataset('tweet');

	my $newest_tweetids = $self->value('newest_tweets');
	my $newest_tweet = $tweet_ds->dataobj($newest_tweetids->[-1]);

	return $newest_tweet->value('twitterid') if $newest_tweet;
	return 0;
}

sub highest_tweetid
{
	my ($self) = @_;

	my $repo = $self->repository;
	my $db = $repo->database;

	my $sql = 'SELECT tweetid FROM tweet_tweetstreams WHERE tweetstreams = ' .
		$self->value('tweetstreamid') . ' ORDER BY tweetid DESC LIMIT 1';

        my $sth = $db->prepare( $sql );
        $sth->execute;

	return $sth->fetchrow_arrayref->[0];
}

#how many tweets in this tweetstream.  Optionally, specify a tweetid (not a twitterid) and we'll only count up to there
#Note that this took a minute to count up to 3 million!
sub count_with_query
{
	my ($self, $highest_tweetid) = @_;

	#if it's archived, don't count from the database -- there may be no data there
	if ($self->value('status') eq 'archived')
	{
		return $self->value('tweet_count');
	}

	my $repo = $self->repository;
	my $db = $repo->database;

	my $sql = 'SELECT COUNT(*) ';
	$sql .=   'FROM tweet JOIN tweet_tweetstreams ON tweet.tweetid = tweet_tweetstreams.tweetid ';
	$sql .=   'WHERE tweet_tweetstreams.tweetstreams = ' . $self->value('tweetstreamid');

	if ($highest_tweetid)
	{
		$sql .= " AND tweet.tweetid <= $highest_tweetid";
	}

        my $sth = $db->prepare( $sql );
        $sth->execute;

	return $sth->fetchrow_arrayref->[0];
}

#returns a page of tweets, or all of them if args not supplied
#order_field enables the retrieval of tweets by tweetid -- much faster for larger twitter feeds
sub tweets
{
	my ($self, $limit, $lowest_id, $order_field) = @_;

	$order_field = 'twitterid' unless $order_field;

	my $ds = $self->repository->dataset('tweet');

	my $search = $ds->prepare_search(custom_order => $order_field);
	$search->add_field($ds->get_field('tweetstreams'), $self->id);

	$search->set_property('limit', $limit) if $limit;
	$search->add_field($ds->get_field($order_field), "$lowest_id-") if $lowest_id;

	return $search->perform_search;
}

#returns the csv columns of a *Tweet* object, and the max multiplicity for this stream for each field
sub csv_cols
{
	my ($self) = @_;

	return
	[
		{ fieldname => "twitterid", ncols => 1 },
		{ fieldname => "from_user", ncols => 1 },
		{ fieldname => "from_user_id", ncols => 1 },
		{ fieldname => "created_at", ncols => 1 },
		{ fieldname => "text", ncols => 1 },
		{ fieldname => "profile_image_url", ncols => 1 },
		{ fieldname => "iso_language_code", ncols => 1 },
		{ fieldname => "source", ncols => 1 },
		{ fieldname => "text_enriched", ncols => 1 },
		{ fieldname => "coordinates", ncols => 2, },
		{ fieldname => "tweetees", ncols => ( $self->get_value('tweetees_ncols') ? $self->get_value('tweetees_ncols') : 1 ) },
		{ fieldname => "hashtags", ncols => ( $self->get_value('hashtags_ncols') ? $self->get_value('hashtags_ncols') : 1 ) },
		{ fieldname => "urls_from_text", ncols => ( $self->get_value('urls_from_text_ncols') ? $self->get_value('urls_from_text_ncols') : 1 ) },
	];
}

######################################################################
=pod

=item $success = $tweetstream->remove

Remove this tweetstream from the database. 

=cut
######################################################################

sub remove
{
	my( $self ) = @_;
	
	my $success = 1;

	my $page_size = 1000;
	while (1)
	{
		my $tweets = $self->tweets($page_size, 0, 'tweetid');
		last unless $tweets->count; #exit if there are no results returned
		$tweets->map( sub
		{
			my ($repo, $ds, $tweet, $tweetstream) = @_;
			$tweet->remove_from_tweetstream($self);
		}, $self);
	}

	# remove tweetstream record
	my $tweetstream_ds = $self->{session}->get_repository->get_dataset( "tweetstream" );
	$success = $success && $self->{session}->get_database->remove(
		$tweetstream_ds,
		$self->get_value( "tweetstreamid" ) );
	
	return( $success );
}

#a parallel list of tweet ids (due to a utf8 issue) will be rendered as the number of tweets.
sub render_tweetcount
{
        my( $session , $field , $value , $alllangs , $nolink , $object ) = @_;

        my $xml = $session->xml;
	my $frag = $xml->create_document_fragment;
	$frag->appendChild($xml->create_text_node(scalar @{$value} . ' tweets'));

	return $frag;
}

sub render_tweet_field
{
        my( $session , $field , $value , $alllangs , $nolink , $object ) = @_;

        my $xml = $session->xml;
	my $tweet_ds = $session->dataset('tweet');
	my $frag = $xml->create_document_fragment;

	my $ol = $xml->create_element('ol', class => 'tweets');
	$frag->appendChild($ol);

	foreach my $tweetid (@{$value})
	{
		my $tweet = $tweet_ds->dataobj($tweetid);
		next unless $tweet;
		$ol->appendChild($tweet->render_li);
	}
	return $frag;
}


sub render_tweet_list
{
        my( $repository , $field , $value , $alllangs , $nolink , $object ) = @_;

        my $xml = $repository->xml;
	my $tweet_ds = $repository->dataset('tweet');
	my $frag = $xml->create_document_fragment;

	$frag->appendChild($object->render_value('oldest_tweets'));

	if ($object->is_set('newest_tweets')) #will only be set if weh have more than n_oldest + n_newest tweets
	{
		my $n_oldest = $repository->config('tweetstream_tweet_renderopts','n_oldest');
		my $n_newest = $repository->config('tweetstream_tweet_renderopts','n_newest');

		my $span = $xml->create_element('span', style => "margin-top: 1em; margin-bottom: 1em;");
		$span->appendChild($xml->create_element('img', src=>"/images/tweepository/paper_tear-top.png", style=>"width: 480px"));
		$span->appendChild($xml->create_element('br'));
		$span->appendChild($repository->html_phrase('DataObj::Tweet/unshown_items', n=>$xml->create_text_node(($object->value('tweet_count') - ($n_oldest+$n_newest)))));
		$span->appendChild($xml->create_element('br'));
		$span->appendChild($xml->create_element('img', src=>"/images/tweepository/paper_tear-bottom.png", style=>"width: 480px"));
		$frag->appendChild($span);
		$frag->appendChild($object->render_value('newest_tweets'));
	}

	return $frag;
}

sub render_tools
{
        my( $session , $field , $value , $alllangs , $nolink , $object ) = @_;

	my $processor = EPrints::ScreenProcessor->new(
		session => $session,
		dataobj => $object,
		dataset => $object->dataset,
	);
	my $some_plugin = $session->plugin( "Screen", processor=>$processor );

	my $table = $session->make_element('table', class => 'tweepository_summary_page_actions' );
	my $icons_tr = $session->make_element('tr');
	my $buttons_tr = $session->make_element('tr');
	$table->appendChild($icons_tr);
	$table->appendChild($buttons_tr);

	my $tools = $session->config('tweepository_tools_on_summary_page');

	foreach my $screenid (@{$tools})
	{
		my $screen = $session->plugin(
			$screenid,
			processor => $processor,
		);
#		my $screen = $item->{screen};
#print STDERR '::::';
#print STDERR join(', ',keys %{$item}), "\n";


		next unless $screen;
		next unless $screen->can_be_viewed;

		my $params = {
			screen_id => $screen->get_id,
			screen => $screen,
			hidden => {
				dataobj => $object->id,
				dataset => 'tweetstream'
			}
		};

		my $td = $session->make_element('td');
		$icons_tr->appendChild($td);
		$td->appendChild($screen->render_action_icon($params));

		$td = $session->make_element('td');
		$buttons_tr->appendChild($td);
		$td->appendChild($screen->render_action_button($params));
	}
	return $table;
}

sub _screenid_to_url
{
	my ($self, $screenid) = @_;

	return $self->{session}->get_repository->get_conf( "http_cgiurl" ).
		'/users/home?screen=' . $screenid .
		'&dataset=tweetstream' .
		'&dataobj=' . $self->id;
}


sub render_exporters
{
	my ($self) = @_;

	my $repository = $self->repository;
	my $xml = $repository->xml;

	my $tweet_count = $self->value('tweet_count');
	$tweet_count = 0 unless $tweet_count;
	my $threshold = $repository->config('tweepository_export_threshold');
	$threshold = 100000 unless $threshold;

	my $plugin_list = 'tweepository_exports_on_summary_page';
	my $phrase_id = 'TweetStream/export_menu';

	if ($self->value('tweet_count') > $threshold)
	{
		$plugin_list = 'tweepository_exports_on_summary_page_too_many';
		$phrase_id = 'TweetStream/export_too_many';
	}

	if ($self->value('status') eq 'archived')
	{
		$plugin_list = 'tweepository_exports_on_summary_page_arhived';
		$phrase_id = 'TweetStream/export_archived';
		return $repository->html_phrase('TweetStream/export_archived');
	}
	

	my $pluginids = $repository->config($plugin_list);

	my $export_ul = $xml->create_element('ul');
	foreach my $pluginid (@{$pluginids})
	{
		my $plugin = $repository->plugin($pluginid);
		next unless $plugin;

		my $li = $xml->create_element( "li" );
		my $url = $plugin->dataobj_export_url( $self );
		my $a = $repository->render_link( $url );
		$a->appendChild( $plugin->render_name );
		$li->appendChild( $a );
		$export_ul->appendChild( $li );

	}
	return ($repository->html_phrase($phrase_id, export_list => $export_ul));

	
}

sub has_owner
{
	my( $self, $possible_owner ) = @_;

	if( $possible_owner->get_value( "userid" ) == $self->get_value( "userid" ) )
	{
		return 1;
	}

	return 0;
}

sub data_for_export
{
	my ($self) = @_;

	my $data;

	foreach my $fieldname (qw/ search_string top_hashtags top_from_users top_tweetees top_urls_from_text /)
	{
		$data->{$fieldname} = $self->value($fieldname) if $self->is_set($fieldname);
	}

	return $data;
}

1;
}



{
package EPrints::DataObj::TweetStreamExport;

our @ISA = ( 'EPrints::DataObj' );

use strict;

sub get_dataset_id
{
	return "tsexport";
}


sub get_defaults
{
        my( $class, $session, $data, $dataset ) = @_;

        $class->SUPER::get_defaults( $session, $data, $dataset );

	$data->{datestamp} = EPrints::Time::get_iso_timestamp();
	$data->{status} = 'pending';

	my $user = $session->current_user;
	my $userid;
	$data->{userid} = $user->id if defined $user;

        return $data;
}

1;
}

