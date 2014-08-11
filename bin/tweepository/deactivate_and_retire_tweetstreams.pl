#!/usr/bin/perl -w

#options:
#
#	--verbose -- output messages

use strict;
use warnings;

use EPrints;
use Getopt::Long;

my $verbose = 0;

Getopt::Long::Configure("permute");

GetOptions(
        'verbose' => \$verbose,
); 

my ($repoid) = @ARGV;
die "deactivate_and_retire_tweetstreams.pl *repositoryid* [--verbose]\n" unless $repoid;

my $ep = EPrints->new;
my $repo = $ep->repository($repoid);
die "couldn't create repository for '$repoid'\n" unless $repo;

my $plugin = $repo->plugin('Event::DeactivateTweetStreams');

$plugin->action_deactivate_tweetstreams(verbose => $verbose);

$plugin = $repo->plugin('Event::ArchiveTweetStreams');

$plugin->action_archive_tweetstreams(verbose => $verbose);

