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

my ($repoid, @ids) = @ARGV;
die "export_tweetstream_packages.pl *repositoryid* [*tweetstreamid*] [*tweetstreamid*] [...] [--verbose]\n" unless $repoid;
chomp $repoid;

my $ep = EPrints->new;
my $repo = $ep->repository($repoid);
die "couldn't create repository for '$repoid'\n" unless $repo;

my $plugin = $repo->plugin('Event::ExportTweetStreamPackage');

if ($verbose)
{
	$plugin->set_verbose(1);
}

if (scalar @ids)
{
	$plugin->action_export_tweetstream_packages(@ids);
}
else
{
	$plugin->action_export_queued_tweetstream_packages;
}

