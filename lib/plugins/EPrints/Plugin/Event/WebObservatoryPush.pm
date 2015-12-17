package EPrints::Plugin::Event::WebObservatoryPush;

use EPrints::Plugin::Event::LockingEvent;
@ISA = qw( EPrints::Plugin::Event::LockingEvent );

use strict;

use JSON;

sub new
{
        my( $class, %params ) = @_;

        my $self = $class->SUPER::new(%params);

        return $self;
}

sub export_dir
{
	my ($self) = @_;

	my $dir = join('/',
		$self->repository->config('archiveroot'),
		'var',
		'tweepository_web_observatory_exports'
	);

	mkdir $dir unless -d $dir;

	return $dir;
}

sub action_web_observatory_push
{
	my ($self) = @_;

        $self->{log_data}->{start_time} = scalar localtime time;
	if ($self->is_locked)
	{
		$self->repository->log( (ref $self) . " is locked.  Unable to run.\n");
		return;
	}
	$self->create_lock;

	$self->wait; #in case UpdateTweetstreamAbstracts is currently running -- we want today's files

	my $repo = $self->repository;
	my $tweetstream_ds = $repo->dataset('tweetstream');

	#scan directory for files to go
	my $files = $self->get_file_list(); #get list of files that are ready to send

	my $files_by_web_obs = {};

	foreach my $tweetstreamid (keys %{$files})
	{
		my $tweetstream = $tweetstream_ds->dataobj($tweetstreamid);
		next unless $tweetstream;
		next unless $tweetstream->is_set('web_observatory_export');
		next unless $tweetstream->value('web_observatory_export') eq 'yes';
		my $v = $tweetstream->validate_web_observatory_meta;
		next unless $v->{valid};

		foreach my $f (@{$files->{$tweetstreamid}})
		{
			push @{$files_by_web_obs->{$tweetstream->value('web_observatory_id')}->{$tweetstream->value('web_observatory_collection')}}, $f;
		}
	}

	foreach my $web_obs (keys %{$files_by_web_obs})
	{
		my $type = $repo->get_conf('web_observatories',$web_obs,'type');

		my $files_to_send = 0;
		foreach my $collection (keys %{$files_by_web_obs->{$web_obs}})
		{
			$files_to_send = 1 if scalar @{$files_by_web_obs->{$web_obs}->{$collection}};
		}
		next unless $files_to_send;

		if ($type eq 'mysql')
		{
			$self->send_mysql($web_obs, $files_by_web_obs->{$web_obs});
		}
		elsif ($type eq 'mongodb')
		{
			$self->send_mongodb($web_obs, $files_by_web_obs->{$web_obs});
		}
		else
		{
			#raise exception -- unexpected db type
		}
	}

        $self->{log_data}->{end_time} = scalar localtime time;
	$self->write_log;
	$self->remove_lock;

}

sub generate_log_string
{
	my ($self) = @_;

	my $l = $self->{log_data};

	my @r;

        push @r, '===========================================================================';
        push @r, '';
        push @r, "Export started at:        " . $l->{start_time};
        push @r, '';
	if ($l->{obs})
	{
		foreach my $obs (keys %{$l->{obs}})
		{
			push @r, "*** Observatory $obs";
			push @r, "\tStatus: " . $l->{obs}->{$obs}->{status};
			foreach my $col (keys %{$l->{obs}->{$obs}->{cols}})
			{
				my $c = $l->{obs}->{$obs}->{cols}->{$col};

				foreach my $k (qw/ insert_count duplicate_count error_count /)
				{
					$c->{$k} = 0 unless $c->{$k};
				}

				push @r, "\tCollection $col activity: "
				. $c->{insert_count} . " inserts,"
				. $c->{duplicate_count} . " duplicates,"
				. $c->{error_count} . " errors.";
				if ($c->{files_removed} && scalar @{$c->{files_removed}})
				{
					push @r, "\tFiles Unlinked:";
					foreach my $f (@{$c->{files_removed}})
					{
						push @r, "\t\t$f";
					}
				}
			}
		}
	}
	else
	{
		push @r, "No Work Done.";
	}
        push @r, '';
        push @r, "Export finished at:       " . $l->{end_time};
        push @r, '';
        push @r, '===========================================================================';


	return join("\n", @r);
}

sub connect_to_mongodb
{
	my ($self, $wo_name) = @_;

	my $repo = $self->repository;
	my $wo_conf = $repo->config('web_observatories',$wo_name);

	eval "use MongoDB; use MongoDB::MongoClient;";

	if ($@)
	{
		$repo->log("MongoDB driver not installed\n");
		return undef;
	}

	foreach my $arg (qw/ host port username password db_name /)
	{
		if (!$wo_conf->{$arg})
		{
			$self->log("Missing $arg in database config for web observatory '$wo_name'\n");
			return undef;
		}
	}

	my $client;
	my $db;
	eval
	{
		$client = MongoDB::MongoClient->new(
			host => $wo_conf->{host} . ':' . $wo_conf->{port},
			username => $wo_conf->{username},
			password => $wo_conf->{password},
			db_name => $wo_conf->{db_name}
		);
		$db = $client->get_database( $wo_conf->{db_name} );
	};
	if (my $err = $@)
	{
		my $msg = "Connection Problem for Observatory $wo_name: $err";
		$self->{log_data}->{obs}->{$wo_name}->{status} = $msg;
		$self->output_status($msg);
		return undef;
	}
	$self->{log_data}->{obs}->{$wo_name}->{status} = "Connected OK";
	$self->output_status("Connected");
	return $db;
}

sub send_mongodb
{
	my ($self, $wo_name, $files_to_send) = @_;
	my $repo = $self->repository;

	$self->output_status("Sending files to $wo_name");

	my $db = $self->connect_to_mongodb($wo_name);
	return unless $db;

	foreach my $collection (keys %{$files_to_send})
	{
		my $collection_id = $repo->config('base_url') . '/' . $collection;
		$self->output_status("Getting collection: $collection_id");
		my $mongo_collection = $db->get_collection($collection_id);
		foreach my $file (@{$files_to_send->{$collection}})
		{
			$self->output_status("Pushing $file");

			open FILE, $file || next; #we don't expect a problem -- handle exceptions better if there are
			my @json_text = <FILE>;
			close FILE;

			my $json = JSON->new->utf8;
			my $tweets = $json->decode(join('',@json_text));

			my $counts = { duplicate => 0, insert => 0, error => 0 };
			foreach my $tweet (@{$tweets})
			{
				$tweet->{_id} = $tweet->{id};
				my $thing;
				eval
				{
					$mongo_collection->insert($tweet);
				};
				if (my $err = $@)
				{
					if ($err =~ m/^E11000 duplicate key error index:/)
					{
						$counts->{duplicate}++;
						$self->{log_data}->{obs}->{$wo_name}->{cols}->{$collection}->{duplicate_count}++;
						#we don't care -- the tweet is alread in the collection
					}
					else
					{
						$repo->log("Error on MongoDB insert: $err\n");
						$counts->{error}++;
						$self->{log_data}->{obs}->{$wo_name}->{cols}->{$collection}->{error_count}++;
					}
				}
				else
				{
					$counts->{insert}++;
					$self->{log_data}->{obs}->{$wo_name}->{cols}->{$collection}->{insert_count}++;
				}
			}
			if (!$counts->{error})
			{
				unlink $file;
				push @{$self->{log_data}->{obs}->{$wo_name}->{cols}->{$collection}->{files_removed}}, $file;
			}
			$self->output_status('File Pushed. Inserts: '.$counts->{insert}.', Duplicates: '.$counts->{duplicate}.' Errors: '.$counts->{error});
		}
	}


}


sub send_mysql
{
	my ($self, $wo_conf, $files_to_send);

}

#returns a hash, keyed on tweetstream ID, with a list of files for each tweetstream.
sub get_file_list
{
	my ($self) = @_;

	my $files = {};

	my $dir = $self->export_dir;

 	opendir(my $dh, $dir) || return; #better exception handling?
	while (my $file = readdir($dh))
	{
		if ($file =~ m/^([0-9]+)-[0-9+]\.json$/)
		{
			push @{$files->{$1}}, "$dir/$file";
		}
	}

	closedir($dh);
	return $files;
}

1;
