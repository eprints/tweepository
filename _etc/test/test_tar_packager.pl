#!/usr/bin/perl -I/usr/share/eprints3/perl_lib

use strict;
use warnings;
use EPrints;

#you should probably fiddle with the tweepository_archive_tar_threshold setting.

my $ep = EPrints->new;
my $repo = $ep->repository('tweets', noise => 0);

my $tsid = 105;

my $plugin = $repo->plugin('Event::ExportTweetStreamPackage');
$plugin->set_verbose(1);

$plugin->output_status('hi');

#$plugin->action_export_tweetstream_packages($tsid);

$plugin = $repo->plugin('Event::ArchiveTweetStreams');
$plugin->set_verbose(1);
$plugin->output_status('ho');

my $ts = $repo->dataset('tweetstream')->dataobj($tsid);

$plugin->verify_package($ts);



