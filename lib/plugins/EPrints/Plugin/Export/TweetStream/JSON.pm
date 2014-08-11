=head1 NAME

EPrints::Plugin::Export::JSON

=cut

package EPrints::Plugin::Export::TweetStream::JSON;

use EPrints::Plugin::Export::JSON;
use JSON;

@ISA = ( "EPrints::Plugin::Export::JSON" );

use strict;

sub new
{
	my( $class, %opts ) = @_;

	my( $self ) = $class->SUPER::new( %opts );

	$self->{name} = "JSON TweetStream";
	$self->{accept} = [ 'dataobj/tweetstream' ];
	$self->{visible} = "all"; 
	$self->{suffix} = ".js";
	$self->{mimetype} = "application/json; charset=utf-8";

	return $self;
}

sub tweet_to_json
{
	my ($tweet, $depth, $in_hash, %opts) = @_;

	my $json = JSON->new->allow_nonref;

	my $data = $tweet->data_for_export;

	my $json_data = $json->pretty->encode($data);

	return _pad_json($json_data, $depth, $in_hash);
}

sub _pad_json
{
	my ($json_string, $depth, $in_hash) = @_;

	my $pad = "   " x $depth;
	my $pre_pad = $in_hash ? "" : $pad;

	chomp $json_string;
	$json_string =~ s/^/$pre_pad/g;
	$json_string =~ s/\n/\r$pre_pad/g;

	return $json_string;
}

sub _epdata_to_json
{
	my( $self, $epdata, $depth, $in_hash, %opts ) = @_;

	if( ref ($epdata) eq 'EPrints::DataObj::Tweet' )
	{
		return tweet_to_json($epdata, $depth, $in_hash, %opts);
#		my $data = $epdata->data_for_export;
#
#		my $json_data = $json->pretty->encode($data);
#		chomp $json_data;
#		$json_data =~ s/^/$pre_pad/g;
#		$json_data =~ s/\n/\r$pre_pad/g;
#
#		return $json_data;
	}

	my $pad = "   " x $depth;
	my $pre_pad = $in_hash ? "" : $pad;

	my $json = JSON->new->allow_nonref;

	my $r = "{\n";

	my $data = $epdata->data_for_export;
	my $json_data = $json->pretty->encode($data);
	chomp $json_data;

	$json_data =~ s/^\s*{\n//;
	$json_data =~ s/\n\s*}$//g;
	$r .= $json_data . ",\n";

	$r .= $pre_pad."\"tweets\" : \[\n";

	my $first = 1;

	$epdata->tweets->map(sub
	{
		my ($repo, $ds, $tweet) = @_;

		$r .= ",\n"if !$first;
		$first = 0;
		$r .= $self->_epdata_to_json($tweet, $depth + 1, 0, %opts )
	});
	$r .= "\n$pad\]\n}\n";
	return $r;
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

