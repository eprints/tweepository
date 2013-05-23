package EPrints::Plugin::Export::TweetStream::GraphML;

@ISA = ( "EPrints::Plugin::Export" );

use strict;
use warnings;

sub new
{
	my( $class, %opts ) = @_;

	my( $self ) = $class->SUPER::new( %opts );

	$self->{name} = "NodeXL (GraphML)";
	$self->{accept} = [ 'dataobj/tweetstream' ];
	$self->{visible} = "all"; 
	$self->{suffix} = ".graphml";
	$self->{mimetype} = "application/graphml+xml";

	return $self;
}

sub output_dataobj
{
	my( $plugin, $dataobj, %opts ) = @_;

	my $repository = $dataobj->repository;

	#this is only for little sets of tweets
	if
	(
		$dataobj->value('tweet_count') > $repository->config('tweepository_export_threshold')
	)
	{
		my $msg =
			"Disallowing GraphML export of Tweetstream " . $dataobj->id .
			" with " . $dataobj->value('tweet_count') . " tweets ";
		$repository->log($msg);
		if( defined $opts{fh} )
		{
			print {$opts{fh}} $msg;
			return;
		}
		return $msg;
	}


	my $data = {
		max_tweet_count => 0,
		max_mention_count => 0,
		tweeters => {},
	};

	$dataobj->tweets->map(sub
	{
		my ($repository, $dataset, $tweet, $data) = @_;
		aggregate_tweet($tweet, $data); 
	}, $data);

	my $r = graphML_head();

	foreach my $handle (keys %{$data->{tweeters}})
	{
		$r .= tweeter_data_to_graphML($data->{tweeters}->{$handle}, $data->{max_tweet_count}, $data->{max_mention_count});
	}

	$r .= graphML_tail();

	if( defined $opts{fh} )
	{
		print {$opts{fh}} $r;
		return;
	}

	return $r;
}

sub tweeter_data_to_graphML
{
	my ($tweeter, $max_tweet_count, $max_mention_count) = @_;

	my $r;

	$r .= '<node id="' . $tweeter->{handle} . '">';

	my $data_elements =
	{
		'V-Size' => generate_size($tweeter->{tweet_count}, $max_tweet_count, 100),
		'V-Image File' => ($tweeter->{profile_image_url} ? $tweeter->{profile_image_url} : '') ,
		'V-Custom Menu Item Text' => 'Open Twitter Page for This Person',
		'V-Custom Menu Item Action' => 'http://twitter.com/'.$tweeter->{handle},
		'V-Label' => $tweeter->{handle},
		'V-Tooltip' => generate_tweeter_tooltip($tweeter)
	};

	foreach my $k (keys %{$data_elements})
	{
		next unless $data_elements->{$k}; #don't create empty tags
		$r .= "<data key = \"$k\">";
		$r .= $data_elements->{$k};
		$r .= '</data>';
	}

	$r .= "</node>\n";

	foreach my $handle (keys %{$tweeter->{mentions}})
	{
		$r .= '<edge source="' . $tweeter->{handle} . '" target="' . $handle . '">';
		$r .= '<data key="E-Width">' . generate_size($tweeter->{mentions}->{$handle}, $max_mention_count, 5)  . '</data>';
		$r .= "</edge>\n";
	}
	return $r;
}

sub generate_tweeter_tooltip
{
	my ($tweeter_data) = @_;

	my @parts;

	push @parts, '@' . $tweeter_data->{handle};
	push @parts, ' - author of ' . $tweeter_data->{tweet_count} . ' tweets';
	push @parts, ' - mentioned in ' . $tweeter_data->{mention_count} . ' tweets';
	
	return join("\n",@parts);
}


sub generate_size
{
	my ($n, $max_n, $return_max) = @_;

	return 1 if $n == 0;

	my $percentage = $n / $max_n;
	my $return = int ($return_max * $percentage);

	$return++ unless $return; #we don't want to return 0

	return $return;
}

sub aggregate_tweet
{
	my ($tweet, $data) = @_;

	my $tweeter_handle = $tweet->value('from_user');
	return unless $tweeter_handle;

	my $tweeter = $data->{tweeters}->{$tweeter_handle};

	if (
		!defined $tweeter or #uninitialised -- we know nothing about this tweeter
		$tweeter->{tweet_count} == 0 #we have very little data - previously mentioned, but no tweets yet
	)
	{
		$tweeter = tweet_to_tweeter_data($tweet, $tweeter);
		$data->{tweeters}->{$tweeter_handle} = $tweeter;
	}

	$tweeter->{tweet_count}++;

	if ($tweeter->{tweet_count} > $data->{max_tweet_count})
	{
		$data->{max_tweet_count} = $tweeter->{tweet_count};
	}

	my $tweetees = $tweet->value('tweetees');
	foreach my $tweetee (@{$tweetees})
	{
		$tweetee =~ s/^\@//;
		$tweeter->{mentions}->{$tweetee}++;
		if ($tweeter->{mentions}->{$tweetee} > $data->{max_mention_count})
		{
			$data->{max_mention_count} = $tweeter->{mentions}->{$tweetee};
		}

		if (!defined $data->{tweeters}->{$tweetee})
		{
			$data->{tweeters}->{$tweetee} =
			{
				'handle' => $tweetee,
				'tweet_count' => 0,
				'mention_count' => 0,
			}
		}
		$data->{tweeters}->{$tweetee}->{mention_count}++;
	}

}

sub tweet_to_tweeter_data
{
	my ($tweet, $tweeter) = @_;

	$tweeter = {} unless $tweeter;

	$tweeter->{handle} = $tweet->value('from_user');
	$tweeter->{profile_image_url} = $tweet->value('profile_image_url');
	$tweeter->{mention_count} = 0 unless $tweeter->{mention_count};
	$tweeter->{tweet_count} = 0 unless $tweeter->{tweet_count};

	return $tweeter;
}


sub graphML_head
{
	return <<END;
<?xml version="1.0" encoding="UTF-8"?>
<graphml xmlns="http://graphml.graphdrawing.org/xmlns">
  <key id="V-Color" for="node" attr.name="Color" attr.type="string" />
  <key id="V-Shape" for="node" attr.name="Shape" attr.type="string" />
  <key id="V-Size" for="node" attr.name="Size" attr.type="string" />
  <key id="V-Opacity" for="node" attr.name="Opacity" attr.type="string" />
  <key id="V-Image File" for="node" attr.name="Image File" attr.type="string" />
  <key id="V-Visibility" for="node" attr.name="Visibility" attr.type="string" />
  <key id="V-Label" for="node" attr.name="Label" attr.type="string" />
  <key id="V-Label Fill Color" for="node" attr.name="Label Fill Color" attr.type="string" />
  <key id="V-Label Position" for="node" attr.name="Label Position" attr.type="string" />
  <key id="V-Tooltip" for="node" attr.name="Tooltip" attr.type="string" />
  <key id="V-Layout Order" for="node" attr.name="Layout Order" attr.type="string" />
  <key id="V-X" for="node" attr.name="X" attr.type="string" />
  <key id="V-Y" for="node" attr.name="Y" attr.type="string" />
  <key id="V-Locked?" for="node" attr.name="Locked?" attr.type="string" />
  <key id="V-Polar R" for="node" attr.name="Polar R" attr.type="string" />
  <key id="V-Polar Angle" for="node" attr.name="Polar Angle" attr.type="string" />
  <key id="V-Degree" for="node" attr.name="Degree" attr.type="string" />
  <key id="V-In-Degree" for="node" attr.name="In-Degree" attr.type="string" />
  <key id="V-Out-Degree" for="node" attr.name="Out-Degree" attr.type="string" />
  <key id="V-Betweenness Centrality" for="node" attr.name="Betweenness Centrality" attr.type="string" />
  <key id="V-Closeness Centrality" for="node" attr.name="Closeness Centrality" attr.type="string" />
  <key id="V-Eigenvector Centrality" for="node" attr.name="Eigenvector Centrality" attr.type="string" />
  <key id="V-PageRank" for="node" attr.name="PageRank" attr.type="string" />
  <key id="V-Clustering Coefficient" for="node" attr.name="Clustering Coefficient" attr.type="string" />
  <key id="V-Reciprocated Vertex Pair Ratio" for="node" attr.name="Reciprocated Vertex Pair Ratio" attr.type="string" />
  <key id="V-ID" for="node" attr.name="ID" attr.type="string" />
  <key id="V-Dynamic Filter" for="node" attr.name="Dynamic Filter" attr.type="string" />
  <key id="V-Add Your Own Columns Here" for="node" attr.name="Add Your Own Columns Here" attr.type="string" />
  <key id="V-Custom Menu Item Text" for="node" attr.name="Custom Menu Item Text" attr.type="string" />
  <key id="V-Custom Menu Item Action" for="node" attr.name="Custom Menu Item Action" attr.type="string" />
  <key id="E-Color" for="edge" attr.name="Color" attr.type="string" />
  <key id="E-Width" for="edge" attr.name="Width" attr.type="string" />
  <key id="E-Style" for="edge" attr.name="Style" attr.type="string" />
  <key id="E-Opacity" for="edge" attr.name="Opacity" attr.type="string" />
  <key id="E-Visibility" for="edge" attr.name="Visibility" attr.type="string" />
  <key id="E-Label" for="edge" attr.name="Label" attr.type="string" />
  <key id="E-Label Text Color" for="edge" attr.name="Label Text Color" attr.type="string" />
  <key id="E-Label Font Size" for="edge" attr.name="Label Font Size" attr.type="string" />
  <key id="E-Reciprocated?" for="edge" attr.name="Reciprocated?" attr.type="string" />
  <key id="E-ID" for="edge" attr.name="ID" attr.type="string" />
  <key id="E-Dynamic Filter" for="edge" attr.name="Dynamic Filter" attr.type="string" />
  <key id="E-Add Your Own Columns Here" for="edge" attr.name="Add Your Own Columns Here" attr.type="string" />
  <key id="E-Relationship" for="edge" attr.name="Relationship" attr.type="string" />
  <key id="E-Relationship Date (UTC)" for="edge" attr.name="Relationship Date (UTC)" attr.type="string" />
  <key id="E-Edge Weight" for="edge" attr.name="Edge Weight" attr.type="string" />
  <graph edgedefault="directed">
END
}

sub graphML_tail
{
	return <<TAIL;
  </graph>
</graphml>
TAIL

}


1;

