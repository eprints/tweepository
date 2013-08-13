#!/usr/bin/perl -w

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
die "update_tweetstreams.pl *repositoryid* [--verbose]\n" unless $repoid;
chomp $repoid;

my $ep = EPrints->new;
my $repo = $ep->repository($repoid);
die "couldn't create repository for '$repoid'\n" unless $repo;

my $plugin = $repo->plugin('Event::UpdateTweetStreams');

if ($verbose)
{
	$plugin->action_update_tweetstreams(1);
}
else
{
	$plugin->action_update_tweetstreams;
}
