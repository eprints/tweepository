#!/usr/bin/perl -I/opt/eprints3/perl_lib

use strict;
use warnings;
use EPrints;

use Parallel::ForkManager;
use DB_File;

my $ep = EPrints->new;
my $repo = $ep->repository('tweets', noise => 3);


my $number_of_forks = 10;
my $uris_per_fork = 100;

my $filename = 'URI_CACHE';
my $cache_file = $repo->config('archiveroot') . '/var/' . $filename; 

my $db_cache_file = $repo->config('archiveroot') . '/var/' . $filename . '.dbfile';
my $tmp_uri_list = $repo->config('archiveroot') . '/var/' . $filename . '.tmp'; #the file containing URLs to lookup

my %cache;
tie %cache, "DB_File", $db_cache_file, O_RDWR|O_CREAT, 0666, $DB_HASH 
        or die "Cannot open file '$db_cache_file': $!\n";

my @uris;
tie @uris, "DB_File", $tmp_uri_list, O_RDWR, 0666, $DB_RECNO
	or die "Cannot open file $tmp_uri_list: $!\n";

my $uri_count = scalar @uris;

my $pm = new Parallel::ForkManager($number_of_forks);

# data structure retrieval and handling
$pm->run_on_finish ( # called BEFORE the first call to start()
	sub {
		my ($pid, $exit_code, $ident, $exit_signal, $core_dump, $data_structure_reference) = @_;
		# retrieve data structure from child
print STDERR "$pid,";
		if (defined($data_structure_reference)) {  # children are not forced to send anything
			foreach my $urichain (@{$data_structure_reference})
			{
				$cache{$urichain->[0]} = join("\t",@{$urichain});
			}
		} else {  # problems occuring during storage or retrieval will throw a warning
			print STDERR qq|No message received from child process $pid!\n|;
		}
	}
);

for (my $i = 0; $i < $uri_count; $i += $uris_per_fork)
{
	$pm->start() and next;

        my $ua = LWP::UserAgent->new(timeout => 10);

	my $return = [];
	foreach my $j ($i..($i+$uris_per_fork))
	{
		my $uri = $uris[$j];
		next unless $uri;
		next if $cache{$uri}; 
print STDERR '.';
		my $response = $ua->head($uri);
		my @redirects = $response->redirects;

		my $urichain;
		if (scalar @redirects)
		{
			foreach my $redirect (@redirects)
			{
				push @{$urichain}, $redirect->request->uri->as_string;
			}
			push @{$urichain}, $response->request->uri->as_string;
		}
		else
		{
			$urichain = [$uri];
		}
		push @{$return}, $urichain;
	}

# send it back to the parent process
	$pm->finish(0, $return);  # note that it's a scalar REFERENCE, not the scalar itself
}
$pm->wait_all_children;
