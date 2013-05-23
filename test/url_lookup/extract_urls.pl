#!/usr/bin/perl -I/opt/eprints3/perl_lib

use strict;
use warnings;
use EPrints;

use DB_File;

my $page_size = 50000; #need to page through the tweet objects because there may be millions of them

my $ep = EPrints->new;
my $repo = $ep->repository('tweets', noise => 3);
my $ds = $repo->dataset('tweet');

my $filename = 'URI_CACHE';
my $cache_file = $repo->config('archiveroot') . '/var/' . $filename; 

my $db_cache_file = $repo->config('archiveroot') . '/var/' . $filename . '.dbfile';
my $tmp_uri_list = $repo->config('archiveroot') . '/var/' . $filename . '.tmp'; #write all URLs to this before processing.

#create a new file if one already exists;
while (-e $tmp_uri_list)
{
	$tmp_uri_list .= 'x';
}


my %cache;
tie %cache, "DB_File", $db_cache_file, O_RDWR|O_CREAT, 0666, $DB_HASH 
        or die "Cannot open file '$db_cache_file': $!\n";

open FILE, ">$tmp_uri_list" or die "cannot open $tmp_uri_list for writing: $!\n";

#function for URI finder.  Fill $uris with new urls
my $finder = URI::Find->new(sub {
	my($uri) = shift;
	return if $cache{$uri}; #we've already looked this one up
	print FILE $uri, "\n";
});

my $tweets_processed = 0; #count the number of records we process
my $page_number = 0;

while (1)
{
#get all unenriched tweets
	my $search = $ds->prepare_search( limit => $page_size, offset => ($page_size * $page_number), custom_order => 'twitterid' );
	$search->add_field(
		$ds->get_field( "text_is_enriched" ),
		'FALSE' );

	my $tweets = $search->perform_search;

#find all URIs on all unenriched tweets
	$tweets->map(sub {
		my ($repo, $ds, $tweet, $data) = @_;

print STDERR '.';
	
		my $text = $tweet->value('text');
		$finder->find(\$text);
	});

	#exit if we are on the last page
	last if $tweets->count < $page_size;
	$page_number++;
	$tweets_processed++;
	print STDERR "processing page $page_number\n";
}

print STDERR "$tweets_processed processed\n";

close FILE;

