#!/usr/bin/perl

use strict;
use warnings;
use threads;
use threads::shared;
use AnyEvent::Twitter::Stream;

my $user = 'gobfrey';
my $password = '*****';
my $filter_file = '/opt/eprints3/lib/epm/tweepository/test/filters';

my %statuses :shared; #the statuses of the running harvesters.  Used for syncing switchover when updating filters.
my $filters :shared; #the filters currently bing used.

my $active_thread; #the stream harvester
my $tmp_thread; #used when updating the filters

my $thread_counter = 1;

$filters = get_filters();
$active_thread = threads->create(\&harvest, $thread_counter);

while (1)
{
	sleep 10;
	my $new_filters = get_filters();

	if ($new_filters ne $filters)
	{
		$filters = $new_filters;

		$thread_counter++;
		$tmp_thread = threads->create(\&harvest, $thread_counter);

		while ($statuses{$thread_counter-1}) #wait for the old thread to kill itself
		{
			sleep 1;
		}

		$active_thread = $tmp_thread;
	}
}



sub get_filters
{
	open FILE, $filter_file or die "Couldn't open $filter_file for reading\n";
	my @filters;
	while (<FILE>)
	{
		my $s = $_;
		chomp $s;
		next unless $s =~ m/./;
		push @filters, $s;
	}
	my $f = join(',',@filters);

	return $f;
}

sub harvest
{
	my ($id) = @_; #the ID of this item.  Used for monitoring whether this thread has successfully started harvesting

	my $done = AnyEvent->condvar;


	my $guard = AnyEvent::Twitter::Stream->new(
		username => $user,
		password => $password,
		method   => "filter",
		track    => $filters,
		on_tweet => sub {
			my $tweet = shift;
			$statuses{$id} = 1; #running

			warn "$id -- $tweet->{user}{screen_name}: $tweet->{text}\n";
		},
		on_error => sub { #this thread will timeout when another connection opens
			if ($statuses{$id+1}) #if the next thread has taken over
			{
				delete $statuses{$id}; #remove my status to avoid memory leak;
				$done->send; #exit
			}
		},
		timeout => 3,
	);

	$done->recv;

}



