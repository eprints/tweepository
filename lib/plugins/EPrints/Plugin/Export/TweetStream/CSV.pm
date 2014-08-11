package EPrints::Plugin::Export::TweetStream::CSV;

use EPrints::Plugin::Export;
use EPrints::Plugin::Export::Grid;

@ISA = ( "EPrints::Plugin::Export::Grid" );

use strict;

sub new
{
	my( $class, %opts ) = @_;

	my $self = $class->SUPER::new( %opts );

	$self->{name} = "CSV";
	$self->{accept} = [ 'dataobj/tweetstream' ];
	$self->{visible} = "all";
	$self->{suffix} = ".csv";
	$self->{mimetype} = "text/csv";
	
	return $self;
}

sub output_dataobj
{
	my( $plugin, $dataobj, %opts ) = @_;

	my $repository = $dataobj->repository;

	my $r = [];
	push @{$r}, csv_headings($dataobj); 

	my $cols = $dataobj->csv_cols;

	#and now the data
	$dataobj->tweets->map(sub
	{
		my ($repository, $dataset, $tweet, $cols) = @_;
		push @{$r}, tweet_to_csvrow($tweet, $cols); 
	}, $cols);

	if( defined $opts{fh} )
	{
		print {$opts{fh}} join( "", @{$r} );
		return;
	}

	return join( "", @{$r} );
}

sub csv_headings
{
	my ($tweetstream) = @_;
	my $repository = $tweetstream->repository;

	my $cols = $tweetstream->csv_cols;

	my @headings;
	foreach my $col (@{$cols})
	{
		if ($col->{ncols} == 1)
		{
			push @headings, $repository->phrase('tweet_fieldname_' . $col->{fieldname});
		}
		else
		{
			my $n = $col->{ncols};
			foreach my $i (1..$n)
			{
				push @headings, $repository->phrase('tweet_fieldname_' . $col->{fieldname}) . ' ' . $i;
			}
		}
	}
	return csv(@headings); 
}

sub tweet_to_csvrow
{
	my ($tweet, $cols) = @_;

	my @data;
	foreach my $col (@{$cols})
	{
		my $val = $tweet->value($col->{fieldname});
		$val = [ $val ] if (not ref $val);

		my $n = $col->{ncols};
		foreach my $i (1..$n)
		{
			push @data, (defined $val->[$i-1] ? $val->[$i-1] : undef);
		}
	}
	return csv(@data); 
}


sub csv
{
        my( @row ) = @_;

        my @r = ();
        foreach my $item ( @row )
        {
                if( !defined $item )
                {
                        push @r, '""';
                        next;
                }
                $item =~ s/"/""/g;
                $item =~ s/\r?\n//g;
                push @r, '"'.$item.'"';
        }
        return join( ",", @r )."\n";
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

