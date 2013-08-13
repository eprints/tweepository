#!/usr/bin/perl -w

#options:
#
#	--verbose -- output messages
#	--update_from_zero -- remove the cache and reprocess all tweets
# 	--recommit_tweets -- trigger regeneration of tweet objects from stored JSON

use strict;
use warnings;

use EPrints;
use Getopt::Long;

my $verbose = 0;
my $update_from_zero = 0;
my $recommit_tweets = 0;

Getopt::Long::Configure("permute");

GetOptions(
        'verbose' => \$verbose,
	'update_from_zero' => \$update_from_zero,
	'recommit_tweets' => \$recommit_tweets,
); 


my ($repoid) = @ARGV;
die "update_tweetstream_abstracts.pl *repositoryid* [--verbose] [--update_from_zero] [--recommit_tweets]\n" unless $repoid;


my $ep = EPrints->new;
my $repo = $ep->repository($repoid);
die "couldn't create repository for '$repoid'\n" unless $repo;

my $plugin = $repo->plugin('Event::UpdateTweetStreamAbstracts');

my %opts;

$opts{update_from_zero} =  $update_from_zero;
$opts{verbose} = $verbose;
$opts{recommit_tweets} = $recommit_tweets;

$plugin->action_update_tweetstream_abstracts(%opts);

