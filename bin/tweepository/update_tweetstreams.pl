#!/usr/bin/perl -w

use strict;
use warnings;

use EPrints;

my ($repoid) = @ARGV;
die "update_tweetstreams.pl *repositoryid*\n" unless $repoid;
chomp $repoid;

my $ep = EPrints->new;
my $repo = $ep->repository($repoid);
die "couldn't create repository for '$repoid'\n" unless $repo;

my $plugin = $repo->plugin('Event::UpdateTweetStreams');

$plugin->action_update_tweetstreams;
