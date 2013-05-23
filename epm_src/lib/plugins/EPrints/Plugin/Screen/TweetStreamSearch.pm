package EPrints::Plugin::Screen::TweetStreamSearch;

@ISA = ( 'EPrints::Plugin::Screen::AbstractSearch' );

use strict;

sub new
{
	my( $class, %params ) = @_;

	my $self = $class->SUPER::new(%params);
	
	$self->{appears} = [
		{
			place => "admin_actions_editorial",
			position => 600,
		},
	];

	return $self;
}

sub search_dataset
{
	my( $self ) = @_;

	return $self->{session}->get_repository->get_dataset( "tweetstream" );
}

sub search_filters
{
	my( $self ) = @_;

	return;
}

sub allow_export { return 1; }

sub allow_export_redir { return 1; }

sub can_be_viewed
{
	my( $self ) = @_;

	return $self->allow( "tweetstream/view" );
}

sub from
{
	my( $self ) = @_;

	my $sconf = $self->{session}->get_repository->get_conf( "search", "tweetstream" );
		
	$self->{processor}->{sconf} = $sconf;

	$self->SUPER::from;
}

1;
