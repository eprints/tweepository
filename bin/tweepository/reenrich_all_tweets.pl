#!/usr/bin/perl -w

use strict;
use warnings;

use EPrints;

my ($repoid) = @ARGV;
die "reenrich_all_tweets.pl *repositoryid*\n" unless $repoid;

chomp $repoid;

my $ep = EPrints->new;
my $repo = $ep->repository($repoid);
die "couldn't create repository for '$repoid'\n" unless $repo;

my $ds = $repo->dataset('tweet');

my $page_size = 10000;
my $high_id = 0; 

while (1)
{
	sleep(2); #let's be nice to anything else that the system might be doing -- we don't want to interrupt harvesting
	my $search = $ds->prepare_search(limit => $page_size, custom_order => 'tweetid' );
	$search->add_field($ds->get_field('tweetid'), $high_id . '-');

	my $results = $search->perform_search;
	print STDERR scalar localtime time, ": $high_id (".$results->count.")\n";

	last unless $results->count > 1;

        $results->map(sub {
                my ($repo, $ds, $tweet, $data) = @_;

		$high_id = $tweet->value('tweetid');
		$tweet->enrich_text();
		$tweet->commit;
        });

	$results->DESTROY;
}

