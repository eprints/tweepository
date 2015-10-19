#!/usr/bin/perl

use strict;
use warnings;

use Archive::Tar;

my $base_dir = '/tmp/tmp/ep-ts-export-tempSF4Gv';

my $t = Archive::Tar->new;

my $files = [];
all_files_in_dir($base_dir, $files);

print STDERR "got files\n";

$t->add_files(@{$files});

print STDERR "added files\n";

$t->write('tmp/out.tar.gz', COMPRESS_GZIP, 'foo');

print STDERR "written file\n";

sub all_files_in_dir
{
        my ($path, $json_files) = @_;

        if (-f $path)
        {
                push @{$json_files}, $path;
        }
        if (-d $path)
        {
                opendir(DIR, $path);
                my @files = grep { !/^\.{1,2}$/ } readdir (DIR); #ignore . and ..
                closedir(DIR);
                @files = map { $path . '/' . $_ } @files; #full paths
                foreach (sort @files)
                {
                        all_files_in_dir($_, $json_files);
                }
        }
}
