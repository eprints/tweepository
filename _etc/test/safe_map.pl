#!/usr/bin/perl -I/opt/eprints3/perl_lib

use strict;
use warnings;
use EPrints;

my $ep = EPrints->new;
my $repo = $ep->repository('tweets');
my $ds = $repo->dataset('tweet');

my $page_size = 10000;
my $high_id = 181414840982716418;


while (1)
{
	my $search = $ds->prepare_search(limit => $page_size, custom_order => 'twitterid' );
	$search->add_field($ds->get_field('twitterid'), $high_id . '-');

	my $results = $search->perform_search;
	print STDERR scalar localtime time, ": $high_id (".$results->count.")\n";

	last unless $results->count > 1;

        $results->map(sub {
                my ($repo, $ds, $tweet, $data) = @_;

		$high_id = $tweet->value('twitterid');
		$tweet->enrich_text();
		$tweet->commit;
        });

	$results->DESTROY;
}

