#!/usr/bin/perl -I/opt/eprints3/perl_lib

use strict;
use warnings;
use EPrints;

my $ep = EPrints->new;
my $repo = $ep->repository('tweets');
my $ds = $repo->dataset('event_queue');

$ds->search->map(sub {
  my ($repo, $ds, $obj) = @_;
	$obj->remove;
});

