package EPrints::Plugin::Export::TweetStream::GoogleMap;

use EPrints::Plugin::Export;

@ISA = ( "EPrints::Plugin::Export" );

use strict;

sub new
{
	my( $class, %opts ) = @_;

	my( $self ) = $class->SUPER::new( %opts );

	my $n = $self->repository->config('tweepository_newest_coordinates_n');
	$n = 'ERR' unless $n;

	$self->{name} = "Google Map (most recent $n geolocations)";
	$self->{accept} = [ 'dataobj/tweetstream' ];
	$self->{visible} = "all"; 
	$self->{suffix} = ".html";
	$self->{mimetype} = "text/html";

	return $self;
}

sub output_dataobj
{
	my( $plugin, $dataobj, %opts ) = @_;

	my $repo = $dataobj->repository;

	my $title = $dataobj->value('title') . ' Most Recent Geolocated Tweets';

	my $coordinates = $dataobj->value('newest_coordinates');

	my $c = [];
	foreach my $coordinate (@{$coordinates})
	{
		push @{$c}, join('', '{ lat: ', $coordinate->{lat}, ', lng: ', $coordinate->{lon}, '}');
	}

	my $r;
	if ($coordinates && scalar @{$coordinates})
	{
		$r = $plugin->_page($title, join(', ',@{$c}));
	}
	else
	{
		$r = $plugin->_page_empty($title);
	}

	if( defined $opts{fh} )
	{
		print {$opts{fh}} $r;
		return;
	}

	return $r;
}

sub _page_empty
{
	my ($self, $title) = @_;

	return <<END;
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<title>$title</title>
  </head>
  <body>
    <p>Sorry, this tweetstream has no geolocation data at this time.  Please check back later.</p>
  </body>
</html>


END

}

sub _page
{
	my ($self, $title, $coordinate_string) = @_;
	return <<END;
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<title>$title</title>
    <style>
      #map {
        width: 100%;
        height: 100%;
      }
    </style>
    <script src="https://maps.googleapis.com/maps/api/js">// <!-- No script --></script>
    <script>
      function initialize() {
        var mapCanvas = document.getElementById('map');
        var mapOptions = {
          center: new google.maps.LatLng(30, 10),
          zoom: 1,
          mapTypeId: google.maps.MapTypeId.ROADMAP
        }
        var map = new google.maps.Map(mapCanvas, mapOptions);

	var latlongs = [ $coordinate_string ];
	for (latlong of latlongs)
	{
		var marker = new google.maps.Marker({
			position: latlong,
			map: map
		});

	}

      }
      google.maps.event.addDomListener(window, 'load', initialize);
    </script>
  </head>
  <body>
    <div id="map"></div>
  </body>
</html>


END



}



1;

