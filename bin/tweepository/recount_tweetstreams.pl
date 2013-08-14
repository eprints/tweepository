#!/usr/bin/perl -w

use strict;
use warnings;

use EPrints;
use Getopt::Long;

my $verbose = 0;

Getopt::Long::Configure("permute");

GetOptions(
        'silent' => \$silent,
);

my ($repoid) = @ARGV;
die "recount_tweetstreams.pl *repositoryid* [--silent]\n" unless $repoid;
chomp $repoid;

my $ep = EPrints->new;
my $repo = $ep->repository($repoid);
die "couldn't create repository for '$repoid'\n" unless $repo;

my $plugin = $repo->plugin('Event::RecountTweetStreams');

if ($silent)
{
	$plugin->action_update_tweetstreams;
}
else
{
	$plugin->action_update_tweetstreams(1);
}
