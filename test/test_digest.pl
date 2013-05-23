#!/usr/bin/perl -I/opt/eprints3/perl_lib

use strict;
use warnings;
use EPrints;

my $ep = EPrints->new;
my $repo = $ep->repository('tweets');
my $ds = $repo->dataset('tweetstream');
my $stream = $ds->dataobj('8');

$stream->generate_tweet_digest;

