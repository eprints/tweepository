#!/usr/bin/perl -I/opt/eprints3/perl_lib

use strict;
use warnings;
use EPrints;

my $page_size = 5; #need to page through the tweet objects

my $ep = EPrints->new;
my $repo = $ep->repository('tweets', noise => 0);
my $ds = $repo->dataset('tweetstream');

my $tweets_processed = 0; #count the number of records we process
my $page_number = 0;

while (1)
{
#get all unenriched tweets
	my $search = $ds->prepare_search( limit => $page_size, offset => ($page_size * $page_number), );

	my $objects = $search->perform_search;

#find all URIs on all unenriched tweets
	$objects->map(sub {
		my ($repo, $ds, $object, $data) = @_;
	
		print STDERR $object->id,'-';
	});

	#exit if we are on the last page
	last if $objects->count < $page_size;
	$page_number++;
	$tweets_processed++;
	print STDERR "processing page $page_number\n";
}

print STDERR "$tweets_processed processed\n";

