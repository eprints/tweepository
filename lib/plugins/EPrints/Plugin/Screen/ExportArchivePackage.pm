package EPrints::Plugin::Screen::ExportArchivedPackage;

use EPrints::Plugin::Screen::RequestTweetStreamExport;

@ISA = ( 'EPrints::Plugin::Screen::RequestTweetStreamExport' );

use strict;

sub new
{
	my( $class, %params ) = @_;

	my $self = $class->SUPER::new(%params);

	$self->{actions} = [qw/ export export_redir /];

	$self->{icon} = "tweetstream_package.png";

	$self->{appears} = [
		{
			place => "dataobj_actions",
			position => 1600,
		},
		{
			place => "tweepository_tools_on_summary_page",
			position => 300,
		},
	];
	
	return $self;
}

sub can_be_viewed
{
	my( $self ) = @_;

	return 0 unless $self->{processor}->{dataset}->id eq 'tweetstream';

	return 0 unless $self->{processor}->{dataobj}->value('status') eq 'archived';

	return $self->allow( "tweetstream/export" );
}


1;
