#!/usr/bin/perl -w

use strict;
use warnings;

use EPrints;

my ($repoid, $update_from_zero) = @ARGV;
die "update_tweetstream_abstracts.pl *repositoryid* [update_from_zero]\n" unless $repoid;

#remove newline from last arg
if ($update_from_zero)
{
	chomp $update_from_zero;
}
else
{
	chomp $repoid;
}

if ($update_from_zero && $update_from_zero ne 'update_from_zero')
{
	die "malformed argument: '$update_from_zero' (should be 'update_from_zero')\n";
}

my $ep = EPrints->new;
my $repo = $ep->repository($repoid);
die "couldn't create repository for '$repoid'\n" unless $repo;

my $plugin = $repo->plugin('Event::UpdateTweetStreamAbstracts');

my %opts;

$opts{update_from_zero} = 1 if $update_from_zero;

$plugin->action_update_tweetstream_abstracts(%opts);

