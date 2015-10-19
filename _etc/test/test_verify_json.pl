#!/usr/bin/perl -I/usr/share/eprints3/perl_lib

use strict;
use warnings;
use EPrints;

#you should probably fiddle with the tweepository_archive_tar_threshold setting.

my $ep = EPrints->new;
my $repo = $ep->repository('tweets', noise => 0);

my $tsid = 171;
my $unpacked_package_path = '/usr/share/eprints3/lib/epm/tweepository/_etc/test/tmp/tweetstream171';

my $ts = $repo->dataset('tweetstream')->dataobj($tsid);
die "Couldn't create tweetstream $tsid\n" unless $ts;


my $plugin = $repo->plugin('Event::ArchiveTweetStreams');
$plugin->set_verbose(1);

$plugin->verify_package($ts);



