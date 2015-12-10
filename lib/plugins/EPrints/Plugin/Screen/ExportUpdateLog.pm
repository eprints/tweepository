package EPrints::Plugin::Screen::ExportUpdateLog;

use EPrints::Plugin::Screen::RequestTweetStreamExport;
use Text::CSV::Simple;

@ISA = ( 'EPrints::Plugin::Screen::RequestTweetStreamExport' );

use strict;

sub new
{
	my( $class, %params ) = @_;

	my $self = $class->SUPER::new(%params);

	$self->{actions} = [qw/ export export_redir /];

	$self->{icon} = "tweetstream_logfile.png";

	$self->{appears} = [
		{
			place => "dataobj_actions",
			position => 1700,
		},
		{
			place => "tweepository_tools_on_summary_page",
			position => 300,
		},
	];

	return $self;
}

sub properties_from
{
	my( $self ) = @_;

	$self->SUPER::properties_from;
	$self->{processor}->{file_to_export} = $self->{processor}->{dataobj}->update_log_filepath;
}


sub can_be_viewed
{
	my( $self ) = @_;

	return 0 unless $self->{processor}->{dataset}->id eq 'tweetstream';

	return $self->allow( "tweetstream/export" );
}

sub render
{
	my ($self) = @_;

	my $session = $self->{session};
	my $ts = $self->{processor}->{dataobj};

	my $div = $session->make_element( "div", class=>"ep_block" );

	$div->appendChild($self->html_phrase('download_log_preamble'));

	my $file = $self->{processor}->{file_to_export};

	if (-e $file)
	{
		$div->appendChild($self->_render_log_details);
	}
	else
	{
		$div->appendChild($self->html_phrase('download_log_absent'));
	}

	return $div;
}

sub _render_log_details
{
	my ($self) = @_;

	my $session = $self->{session};
	my $ts = $self->{processor}->{dataobj};
	my $file = $self->{processor}->{file_to_export};

	#sanity check
	return $self->repository->xml->create_document_fragment unless -e $file;

	my $csv = Text::CSV::Simple->new;
	my @rows = $csv->read_file($file);

	my $table = $session->xml->create_element('table', style => 'border: thin solid black;');
	my $cell_type = 'th';
	foreach my $row (@rows)
	{
		my $tr = $session->xml->create_element('tr');
		$table->appendChild($tr);
		foreach my $cell (@{$row})
		{
			my $td = $session->xml->create_element($cell_type);
			$tr->appendChild($td);
			$td->appendChild($session->xml->create_text_node($cell));
		}
		$cell_type = 'td'; #only the first row is 'th'
	}

	my $size = -s $file;
	if ($size >= (1024 * 1024))
	{
		$size = sprintf("%.1f", ($size / (1024 * 1024))) . ' MB';
	}
	elsif ($size >= 1024)
	{
		$size = sprintf("%.1f", ($size / 1024 )) . ' KB';
	}

	$size = $session->make_text($size);

	my $mtime = (stat( $file ))[9];
	my $date = $session->make_text(scalar localtime($mtime));

	return $self->html_phrase('update_log_details',
		logfile => $table,
		filesize => $size,
		datestamp => $date,	
		downloadbutton => $self->form_with_buttons( export_redir => $self->phrase('export_redir') )
	);
}




1;
