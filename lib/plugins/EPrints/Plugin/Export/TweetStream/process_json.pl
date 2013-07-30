#!/usr/bin/perl -I/opt/eprints3/perl_lib

use strict;
use warnings;

use EPrints;

my $ep = EPrints->new;

my $repo = $ep->repository('epmtweepository');
die "could not create repository\n" unless $repo;

my $ds = $repo->dataset('tweet');
$ds->search->map(sub{
	my ($repo, $ds, $tweet) = @_;
	print STDERR '.';
	$tweet->process_json;
	$tweet->commit;
});

