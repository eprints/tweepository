package EPrints::Plugin::Screen::RequestTweetStreamExport;

@ISA = ( 'EPrints::Plugin::Screen::Workflow' );

use strict;

sub new
{
	my( $class, %params ) = @_;

	my $self = $class->SUPER::new(%params);

	$self->{actions} = [qw/ queue_export export_redir export /];

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

	return 0 if $self->{processor}->{dataobj}->value('status') eq 'archived';

	return $self->allow( "tweetstream/export" );
}

sub allow_export_redir
{
	my ($self) = @_;

	return 0 unless (-e $self->{processor}->{dataobj}->export_package_filepath);

	return $self->can_be_viewed;
}

sub allow_export
{
	my ($self) = @_;

	return 0 unless (-e $self->{processor}->{dataobj}->export_package_filepath);

	return $self->can_be_viewed;
}

sub allow_queue_export
{
	my ($self) = @_;

	return 0 unless ($self->{processor}->{dataobj}->value('status') eq 'active');
	return $self->can_be_viewed;
}

sub render
{
	my( $self ) = @_;

	my $session = $self->{session};
	my $ts = $self->{processor}->{dataobj};

	if ($ts->value('status') eq 'active')
	{
		return $self->render_active;
	}
	else
	{
		return $self->render_non_active;
	}
}	

sub render_active
{
	my( $self ) = @_;

	my $session = $self->{session};
	my $ts = $self->{processor}->{dataobj};

	my $div = $session->make_element( "div", class=>"ep_block" );

	$div->appendChild($self->html_phrase('preamble'));

	if ($ts->pending_package_request || $ts->running_package_request)
	{
		$div->appendChild($self->html_phrase('package_pending'));
	}
	else
	{
		my $file = $ts->export_package_filepath;
		if (-e $file)
		{
			$div->appendChild($self->_render_package_details);
		}
		else
		{
			$div->appendChild($self->html_phrase('package_absent'));
		}

		if ($self->allow_queue_export)
		{
			$div->appendChild( $self->html_phrase('generate_preamble'));
			$div->appendChild( $self->form_with_buttons(queue_export => $self->phrase('queue_export')));
		}
	}

#	$div->appendChild( $self->html_phrase("sure_delete",
#		title=>$self->{processor}->{dataobj}->render_description() ) );
#

	return( $div );
}

sub render_non_active
{
	my ($self) = @_;

	my $session = $self->{session};
	my $ts = $self->{processor}->{dataobj};

	my $div = $session->make_element( "div", class=>"ep_block" );

	$div->appendChild($self->html_phrase('non_active_preamble'));

	my $file = $ts->export_package_filepath;
	if (-e $file)
	{
		$div->appendChild($self->_render_package_details);
	}
	else
	{
		$div->appendChild($self->html_phrase('non_active_package_absent'));
	}

	return $div;
}

sub _render_package_details
{
	my ($self) = @_;

	my $session = $self->{session};
	my $ts = $self->{processor}->{dataobj};
	my $file = $ts->export_package_filepath;

	#sanity check
	return $self->repository->xml->create_document_fragment unless -e $file;

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

	return $self->html_phrase('package_exists',
		filesize => $size,
		datestamp => $date,	
		downloadbutton => $self->form_with_buttons( export_redir => $self->phrase('export_redir') )
	);
}


sub form_with_buttons
{
	my ($self, %buttons) = @_;

	my $form= $self->render_form;
	$form->appendChild( $self->{session}->render_action_buttons( %buttons) );
	return $form;
}


#Queue up the export of the tweetstream
sub action_queue_export
{
	my( $self ) = @_;

	my $ds = $self->repository->dataset('tsexport');
	my $tweetstreamid = $self->{processor}->{dataobj}->id;
	$ds->create_object( $self->repository, { tweetstream => $tweetstreamid } );


	$self->{processor}->add_message( "message",
                $self->html_phrase( "export_queued" ) );

	#$self->{processor}->{screenid} = $self->{processor}->{dataobj}->view_screen;
}



sub action_export
{
	my( $self ) = @_;

	$self->{processor}->{tweetstream_subscreen} = "export";

	return;
}

sub wishes_to_export
{
	my( $self ) = @_;
	return 0 unless $self->{processor}->{tweetstream_subscreen} eq "export";
	return 1;
}

#Send the file
sub export 
{
	my( $self ) = @_;

	my $tweetstream = $self->{processor}->{dataobj};
	my $filepath = $tweetstream->export_package_filepath;
	
	return unless -e $filepath;

	my $buffer;
	open ZIP, $filepath || return;

	binmode ZIP;
	binmode STDOUT;

	while (
		read (ZIP, $buffer, 655536)
		and print STDOUT $buffer
	) {};

	close ZIP;

}

sub export_mimetype
{
	my ($self) = @_;

	my $tweetstream = $self->{processor}->{dataobj};

	my $export_package_filename = $tweetstream->export_package_filename;

	if ($export_package_filename =~ m/\.zip$/)
	{
		return 'application/zip';
	}
	if ($export_package_filename =~ m/\.tar\.gz$/)
	{
		return 'application/gzip';
	}


	return "text/plain";
}

sub action_export_redir
{
	my( $self ) = @_;

	$self->{processor}->{redirect} = $self->export_url();
}

sub export_url
{
	my( $self ) = @_;

	my $tweetstream = $self->{processor}->{dataobj};
	my $tweetstreamid = $tweetstream->id;
	my $filename = $tweetstream->export_package_filename;

	my $url = URI->new( $self->{session}->get_uri() . "/export_tweetstream_package/" . $filename );

	$url->query_form(
		screen => $self->{processor}->{screenid},
		dataset => "tweetstream",
		dataobj => $tweetstreamid,
		_action_export => 1,
	);

	return $url;
}


1;

=head1 COPYRIGHT

=for COPYRIGHT BEGIN

Copyright 2000-2011 University of Southampton.

=for COPYRIGHT END

=for LICENSE BEGIN

This file is part of EPrints L<http://www.eprints.org/>.

EPrints is free software: you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published
by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

EPrints is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
License for more details.

You should have received a copy of the GNU Lesser General Public
License along with EPrints.  If not, see L<http://www.gnu.org/licenses/>.

=for LICENSE END

