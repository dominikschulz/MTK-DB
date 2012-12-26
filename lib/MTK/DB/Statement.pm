package MTK::DB::Statement;
# ABSTRACT: a wrapped DBI statement

use 5.010_000;
use mro 'c3';
use feature ':5.10';

use Moose;
use namespace::autoclean;

# use IO::Handle;
# use autodie;
# use MooseX::Params::Validate;

has '_sth' => (
    'is'      => 'ro',
    'isa'     => 'Object',
    'lazy'    => 1,
    'builder' => '_init_sth',
);

has '_dbh' => (
    'is'       => 'ro',
    'isa'      => 'DBI::db',
    'required' => 1,
);

has 'parent' => (
    'is'       => 'ro',
    'isa'      => 'MTK::DB',
    'required' => 1,
);

has 'sqlstr' => (
    'is'       => 'ro',
    'isa'      => 'Str',
    'required' => 1,
);

sub _init_sth {
    my $self = shift;

    my $dbi_sth = $self->_dbh()->prepare( $self->sqlstr() );
    if ( !$dbi_sth ) {
        $self->logger()->log( message => 'Prepare failed on Query ' . $self->sqlstr() . ' with error ' . $self->_dbh(), level => 'error', );
        return;
    }

    return $dbi_sth;
}

=method logger

Return the parents instance of the logger.

=cut
sub logger { my $self = shift; return $self->parent()->logger(); }

=method valid

See L<DBI>.

=cut
sub valid {
    my $self = shift;
    if ( $self->_sth() ) {
        return 1;
    }
    else {
        return;
    }
}

=method err

See L<DBI>.

=cut
## no critic (ProhibitBuiltinHomonyms)
sub err {
    ## use critic
    my $self = shift;

    return $self->_sth()->err();
}

=method errstr

See L<DBI>.

=cut
sub errstr {
    my $self = shift;

    return $self->_sth()->errstr();
}

=method state

See L<DBI>.

=cut
## no critic (ProhibitBuiltinHomonyms)
sub state {
    ## use critic
    my $self = shift;

    return $self->_sth()->state;
}

=method set_err

See L<DBI>.

=cut
sub set_err {
    my ( $self, @args ) = @_;

    return $self->_sth()->set_err(@args);
}

=method trace

See L<DBI>.

=cut
sub trace {
    my ( $self, @args ) = @_;

    return $self->_sth()->trace(@args);
}

=method trace_msg

See L<DBI>.

=cut
sub trace_msg {
    my ( $self, @args ) = @_;

    return $self->_sth()->trace_msg(@args);
}

=method func

See L<DBI>.

=cut
sub func {
    my ( $self, @args ) = @_;

    return $self->_sth()->func(@args);
}

# 'can' is not implemented

=method parse_trace_flags

See L<DBI>.

=cut
sub parse_trace_flags {
    my ( $self, @args ) = @_;

    return $self->_sth()->parse_trace_flags(@args);
}

=method parse_trace_flag

See L<DBI>.

=cut
sub parse_trace_flag {
    my ( $self, @args ) = @_;

    return $self->_sth()->parse_trace_flag(@args);
}

=method private_attribute_info

See L<DBI>.

=cut
sub private_attribute_info {
    my $self = shift;

    return $self->_sth()->private_attribute_info();
}

# 'swap_inner_handle' is not implemented

=method visit_child_handles

See L<DBI>.

=cut
sub visit_child_handles {
    my ( $self, @args ) = @_;

    return $self->_sth()->visit_child_handles(@args);
}

=method bind_param

See L<DBI>.

=cut
sub bind_param {
    my ( $self, @args ) = @_;

    return $self->_sth()->bind_param(@args);
}

=method bind_param_inout

See L<DBI>.

=cut
sub bind_param_inout {
    my ( $self, @args ) = @_;

    return $self->_sth()->bind_param_inout(@args);
}

=method bind_param_array

See L<DBI>.

=cut
sub bind_param_array {
    my ( $self, @args ) = @_;

    return $self->_sth()->bind_param_array(@args);
}

=method execute

See L<DBI>.

=cut
sub execute {
    my ( $self, @args ) = @_;

    return $self->_sth()->execute(@args);
}

=method execute_array

See L<DBI>.

=cut
sub execute_array {
    my ( $self, @args ) = @_;

    return $self->_sth()->execute_array(@args);
}

=method execute_for_fetch

See L<DBI>.

=cut
sub execute_for_fetch {
    my ( $self, @args ) = @_;

    return $self->_sth()->execute_for_fetch(@args);
}

=method fetchrow_arrayref

See L<DBI>.

=cut
sub fetchrow_arrayref {
    my $self = shift;

    return $self->_sth()->fetchrow_arrayref();
}

=method fetchrow_array

See L<DBI>.

=cut
sub fetchrow_array {
    my $self = shift;

    return $self->_sth()->fetchrow_array();
}

=method fetchrow_hashref

See L<DBI>.

=cut
sub fetchrow_hashref {
    my ( $self, @args ) = @_;

    return $self->_sth()->fetchrow_hashref(@args);
}

=method fetchall_arrayref

See L<DBI>.

=cut
sub fetchall_arrayref {
    my ( $self, @args ) = @_;

    return $self->_sth()->fetchall_arrayref(@args);
}

=method fetchall_hashref

See L<DBI>.

=cut
sub fetchall_hashref {
    my ( $self, @args ) = @_;

    return $self->_sth()->fetchall_hashref(@args);
}

=method finish

See L<DBI>.

=cut
sub finish {
    my $self = shift;

    return $self->_sth()->finish();
}

=method rows

See L<DBI>.

=cut
sub rows {
    my $self = shift;

    return $self->_sth()->rows();
}

=method bind_col

See L<DBI>.

=cut
sub bind_col {
    my ( $self, @args ) = @_;

    return $self->_sth()->bind_col(@args);
}

=method bind_columns

See L<DBI>.

=cut
sub bind_columns {
    my ( $self, @args ) = @_;

    return $self->_sth()->bind_columns(@args);
}

=method dump_results

See L<DBI>.

=cut
sub dump_results {
    my ( $self, @args ) = @_;

    return $self->_sth()->dump_results(@args);
}

=method NUM_OF_FIELDS

See L<DBI>.

=cut
sub NUM_OF_FIELDS {
    my $self = shift;

    return $self->_sth()->NUM_OF_FIELDS;
}

=method NUM_OF_PARAMS

See L<DBI>.

=cut
sub NUM_OF_PARAMS {
    my $self = shift;

    return $self->_sth()->NUM_OF_PARAMS;
}

=method NAME

See L<DBI>.

=cut
sub NAME {
    my $self = shift;

    return $self->_sth()->NAME;
}

=method NAME_lc

See L<DBI>.

=cut
sub NAME_lc {
    my $self = shift;

    return $self->_sth()->NAME_lc;
}

=method NAME_uc

See L<DBI>.

=cut
sub NAME_uc {
    my $self = shift;

    return $self->_sth()->NAME_uc;
}

=method NAME_hash

See L<DBI>.

=cut
sub NAME_hash {
    my $self = shift;

    return $self->_sth()->NAME_hash;
}

=method NAME_lc_hash

See L<DBI>.

=cut
sub NAME_lc_hash {
    my $self = shift;

    return $self->_sth()->NAME_lc_hash;
}

=method NAME_uc_hash

See L<DBI>.

=cut
sub NAME_uc_hash {
    my $self = shift;

    return $self->_sth()->NAME_uc_hash;
}

=method TYPE

See L<DBI>.

=cut
sub TYPE {
    my $self = shift;

    return $self->_sth()->TYPE;
}

=method PRECISION

See L<DBI>.

=cut
sub PRECISION {
    my $self = shift;

    return $self->_sth()->PRECISION;
}

=method SCALE

See L<DBI>.

=cut
sub SCALE {
    my $self = shift;

    return $self->_sth()->SCALE;
}

=method NULLABLE

See L<DBI>.

=cut
sub NULLABLE {
    my $self = shift;

    return $self->_sth()->NULLABLE;
}

=method CursorName

See L<DBI>.

=cut
sub CursorName {
    my $self = shift;

    return $self->_sth()->CursorName;
}

=method Database

See L<DBI>.

=cut
sub Database {
    my $self = shift;

    return $self->_sth()->Database;
}

=method Statement

See L<DBI>.

=cut
sub Statement {
    my $self = shift;

    return $self->_sth()->Statement;
}

=method ParamValues

See L<DBI>.

=cut
sub ParamValues {
    my $self = shift;

    return $self->_sth()->ParamValues;
}

=method ParamTypes

See L<DBI>.

=cut
sub ParamTypes {
    my $self = shift;

    return $self->_sth()->ParamTypes;
}

=method ParamArrays

See L<DBI>.

=cut
sub ParamArrays {
    my $self = shift;

    return $self->_sth()->ParamArrays;
}

=method RowsInCache

See L<DBI>.

=cut
sub RowsInCache {
    my $self = shift;

    return $self->_sth()->RowsInCache;
}

no Moose;
__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

MTK::DB::Statement - DBI statement wrapper

=head1 ACKNOWLEDGEMENT

This module was originally developed for eGENTIC Systems. With approval from eGENTIC Systems,
this module was generalized and published, for which the authors would like to express their
gratitude.

=cut
