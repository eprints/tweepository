#!/usr/bin/perl -I/opt/eprints3/perl_lib

use strict;
use warnings;
use EPrints;

my $ep = EPrints->new;
my $repo = $ep->repository('tweets', noise => 3);
my $ds = $repo->dataset('tweet');

print STDERR 'a';

#get all unenriched tweets
my $search = $ds->prepare_search( );
$search->add_field(
                $ds->get_field( "text_is_enriched" ),
                'FALSE' );

print STDERR 'b';
my $tweets = $search->perform_search;

print STDERR 'c';
