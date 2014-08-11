package EPrints::Plugin::Screen::ManageTweetstreamsLink;

use EPrints::Plugin::Screen;
@ISA = qw( EPrints::Plugin::Screen );

sub new
{
	my( $class, %params ) = @_;

	my $self = $class->SUPER::new(%params);

	$self->{appears} = [
	{
		place => "key_tools",
		position => 150,
	}
	];

	return $self;
}

sub can_be_viewed
{
	my( $self ) = @_;

	return $self->allow( "tweetstream/view" );
}

sub from
{
	my( $self ) = @_;

	my $url = URI->new( $self->{session}->current_url( path => "cgi", "users/home" ) );
	$url->query_form(	
		screen => "Listing",
		dataset => "tweetstream",
	);

	$self->{session}->redirect( $url );
	exit;
}


1;
