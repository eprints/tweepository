#!/usr/bin/perl -w

#this needs to be moved into a locking event so that it can give way

use strict;
use warnings;

use EPrints;
use Archive::Zip qw( :ERROR_CODES :CONSTANTS );
use JSON;
use Archive::Zip::MemberRead;
#bugfix
#sub Archive::Zip::MemberRead::opened { 1 }

use Data::Dumper;

my $STATE = {tweets_created => 0, tweets_existing => 0};

my ($repoid, $userid, $filename) = @ARGV;
die "export_tweetstream_packages.pl *repositoryid* *username/userid* *file*\n" unless $filename;
chomp $repoid;
chomp $filename;

die "file $filename doesn't exist\n" unless -e $filename;

my $ep = EPrints->new;
my $repo = $ep->repository($repoid);
die "couldn't create repository for '$repoid'\n" unless $repo;

my $user;
if( $userid =~ m/^\d+/ )
{
	$user = $repo->dataset('user')->dataobj($userid);
}
else
{
	$user = EPrints::DataObj::User::user_with_username( $repo, $userid );
}
if( !defined $user )
{
	die "Can't find user with userid/username [$userid]\n";
}



my $files;

# Read a Zip file
my $zip = Archive::Zip->new();
unless ( $zip->read( $filename ) == AZ_OK ) {
	die 'Unable to read $filename as ZIP';
}

foreach my $member ($zip->members)
{
	next if $member->isDirectory;
	my $filename = $member->fileName;

	$filename =~ m/\.([^\.]*)$/;
	my $extension = $1;

	next unless $extension;

	push @{$files->{$extension}}, $filename;
}

#validate presence of correctly named XML file:
die "no object XML file in zip file\n" unless $files->{xml}->[0] =~ m/tweetstream[0-9]+\/tweetstream\.xml/;

#check that we have some json files
die unless $files->{json} and scalar @{$files->{json}} > 0;

my $ds = $repo->dataset('tweetstream');

my $plugin = $repo->plugin('Import::XML');

die "Couldn't load import plugin\n" unless $plugin;

if( $plugin->broken )
{
	print STDERR "Plugin Import::XML could not run because:\n";
	print STDERR $plugin->error_message."\n";
	$repo->terminate;
	exit 1;
}

my $ts = create_tweetstream_dataobj($zip, $files, $repo);
die "problem creating tweetstream object\n" unless $ts;

$STATE->{new_tweetstreamid} = $ts->id;

add_tweets_to_tweetstream($zip, $files, $ts);

print "Tweetstream package imported successfully:\n";
print "\tTweetstream ID: " . $STATE->{new_tweetstreamid} . "\n";

print $STATE->{tweets_created} . " tweets created\n";
print $STATE->{tweets_existing} . " existing tweets in this stream\n";

	print "Now run update_tweetstream_abstracts to generate the new tweetstream's abstract page\n";
if ($STATE->{tweets_existing})
{
	print "Note that some tweets were already existing.  You may need to run update_tweetstream_abstracts with the 'update_from_zero' option, though be aware this may take some time as it removes all cached data and regenerates all tweetstream abstracts from scratch.";
}

sub add_tweets_to_tweetstream
{
	my ($zip, $files, $ts) = @_;
	my $repo = $ts->repository;

	my $json = JSON->new->allow_nonref;

	foreach my $filename (sort sort_json_filenames @{$files->{json}})
	{
		my $fh = file_in_zip_to_fh($filename, $zip);
		my @json_txt = <$fh>;

                my $tweets = eval { $json->utf8()->decode(join('',@json_txt)); };
		if ($@)
		{
			print STDERR "problem parsing $filename in zip file:\n$@";
			print STDERR "rolling back changes (deleting tweetstream object)\n";
			$ts->remove;
			print STDERR "rollback successful\n";
			exit;
		}	

		my $summary_data = {};
		foreach my $json_tweet (@{$tweets->{tweets}})
		{
			my $twitterid = $json_tweet->{id};

			#We might want to do something with the added value fields, but for now
			#  we won't.
			#In future it will be particularly important to make a sensible decision about this
			#  if we have some fully resolved URLs (i.e. we've followed the short URL redirects)
			#In any case, we need to delete it from the hash before creating the tweet object
			#  and do something sensible with the data on the created (or already existing) object
			my $eprints_value_added = delete $json_tweet->{eprints_value_added};

                        #check to see if we already have a tweet with this twitter id in this repository
                        my $tweetobj = EPrints::DataObj::Tweet::tweet_with_twitterid($repo,$twitterid);
                        if (!defined $tweetobj)
                        {
                                $tweetobj = EPrints::DataObj::Tweet->create_from_data(
                                        $repo,
                                        {
                                                twitterid => $twitterid,
                                                json_source => $json_tweet,
                                                tweetstreams => [$ts->id],
                                        }
                                );
				$STATE->{tweets_created} += 1;
                        }
			else
			{
				$tweetobj->add_to_tweetstream($ts);
				$STATE->{tweets_existing} += 1;
			}
			$tweetobj->commit;
		}

	}
}

sub sort_json_filenames
{

	$a =~ m/([0-9]*)\.json/;
	my $a_int = $1;
	$b =~ m/([0-9]*)\.json/;
	my $b_int = $1;

	return $a_int <=> $b_int;
}

sub create_tweetstream_dataobj
{
	my ($zip, $files, $repo) = @_;

	my $fh = file_in_zip_to_fh($files->{xml}->[0], $zip);
	$fh = wrap_with_tag('tweetstreams', $fh);

	my $ds = $repo->dataset('tweetstream');

	my $list = $plugin->input_fh( dataset => $ds, fh => $fh );
	return undef unless $list;

	my $ts = $list->item(0);
	return undef unless $ts;

	$ts->set_value('userid', $user->id);
	$ts->commit;
	return $ts;
}

##now create the tweet objects from the json data, rebuilding the abstract with the data gleaned from that.


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

#the plugin is expecting a list, so make the XML look like it's a list
sub wrap_with_tag
{
	my ($tagname, $fh) = @_;

	my $tmp_fh = File::Temp->new( TEMPLATE => "ep-ts-import_unzipXXXXX", TMPDIR => 1 );

	print $tmp_fh "<$tagname>";
	print $tmp_fh $_ while (<$fh>);
	print $tmp_fh "</$tagname>";

	#move to start of file
	seek($tmp_fh, 0, 0);

	return $tmp_fh;
}
