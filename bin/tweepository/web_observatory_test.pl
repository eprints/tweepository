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

my ($repoid, $wo_id) = @ARGV;
die "web_observatory_push.pl *repository_id* *web_observatory_id*\n" unless $wo_id;
chomp $repoid;

my $ep = EPrints->new;
my $repo = $ep->repository($repoid);
die "couldn't create repository for '$repoid'\n" unless $repo;

my $plugin = $repo->plugin('Event::WebObservatoryPush');

$plugin->set_verbose(1);

$plugin->action_test_web_observatory($wo_id);

