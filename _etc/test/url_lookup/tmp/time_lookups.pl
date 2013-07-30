#!/usr/bin/perl -I/opt/eprints3/perl_lib

use strict;
use warnings;
use EPrints;
use LWP::UserAgent;


        my $ua = LWP::UserAgent->new(timeout => 10);

open FILE, "random_order_urls" or die;

my $i = 0;

my $t = time;
while (<FILE>)
{
	$i++;
print STDERR '.';
		chomp;
                my $response = $ua->head($_);
                my @redirects = $response->redirects;	


	last if $i >= 500;
}
my $t2 = time;

print STDERR "$i in " . ($t2 - $t) . " seconds\n";

