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
die "web_observatory_push.pl *repositoryid* [--verbose]\n" unless $repoid;
chomp $repoid;

my $ep = EPrints->new;
my $repo = $ep->repository($repoid);
die "couldn't create repository for '$repoid'\n" unless $repo;

my $plugin = $repo->plugin('Event::WebObservatoryPush');

if ($verbose)
{
	$plugin->set_verbose(1);
}

$plugin->action_web_observatory_push();

