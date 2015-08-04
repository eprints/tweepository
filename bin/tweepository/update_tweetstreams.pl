#!/usr/bin/perl -w

use strict;
use warnings;

use EPrints;
use Getopt::Long;

my $verbose = 0;
my $status_log_file = '';

Getopt::Long::Configure("permute");

GetOptions(
        'verbose' => \$verbose,
	'status-log-file=s' => \$status_log_file
);

my ($repoid) = @ARGV;
die "update_tweetstreams.pl *repositoryid* [--verbose] [--status-log-file=file]\n" unless $repoid;
chomp $repoid;

my $ep = EPrints->new;
my $repo = $ep->repository($repoid);
die "couldn't create repository for '$repoid'\n" unless $repo;

my $plugin = $repo->plugin('Event::UpdateTweetStreams');

$plugin->action_update_tweetstreams(verbose => $verbose, status_log_file => $status_log_file);

