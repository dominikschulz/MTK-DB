package MTK::DB::Credentials;
# ABSTRACT: mysql credential detection

use 5.010_000;
use mro 'c3';
use feature ':5.10';

use Moose;
use namespace::autoclean;

# use IO::Handle;
# use autodie;
# use MooseX::Params::Validate;

use Carp;
use Config::Tiny;
use Try::Tiny;
use MTK::DB;
use List::MoreUtils;

has 'hostname' => (
    'is'       => 'ro',
    'isa'      => 'Str',
    'required' => 1,
);

has 'database' => (
    'is'  => 'rw',
    'isa' => 'Maybe[Str]',
);

has 'keys' => (
    'is'      => 'rw',
    'isa'     => 'ArrayRef[Str]',
    'default' => sub { [] },
);

has 'creds' => (
    'is'      => 'rw',
    'isa'     => 'HashRef',
    'default' => sub { {} },
);

has 'num_tries' => (
    'is'      => 'rw',
    'isa'     => 'Int',
    'default' => 1,
);

has '_username' => (
    'is'  => 'rw',
    'isa' => 'Str',
);

has '_password' => (
    'is'  => 'rw',
    'isa' => 'Str',
);

has 'candidates' => (
    'is'      => 'ro',
    'isa'     => 'ArrayRef',
    'default' => sub { [] },
);

has 'tries' => (
    'is'      => 'ro',
    'isa'     => 'ArrayRef',
    'default' => sub { [] },
);

has 'sleep' => (
    'is'      => 'ro',
    'isa'     => 'Int',
    'default' => 10,
);

has 'raise_error' => (
    'is'      => 'rw',
    'isa'     => 'Bool',
    'default' => 1,
);

has 'port' => (
    'is'      => 'rw',
    'isa'     => 'Int',
    'default' => 3306,
);

has 'socket' => (
    'is'  => 'rw',
    'isa' => 'Str',
);

has 'timeout' => (
    'is'      => 'rw',
    'isa'     => 'Int',
    'default' => 15,
);

has 'dbh' => (
    'is'  => 'rw',
    'isa' => 'MTK::DB',
);

with qw(Config::Yak::RequiredConfig Log::Tree::RequiredLogger);

=method BUILD

Detect the credentials.

=cut
sub BUILD {
    my $self = shift;

    if ( $self->_detect_credentials() ) {
        return 1;
    }
    else {
        my $msg = 'No valid credentials found for host ' . $self->hostname() . '. Failed creds: ' . $self->_format_creds( $self->tries() );
        if ( $self->raise_error() ) {
            confess $msg;
        }
        else {
            carp $msg;
        }
    }

    return;
}

sub _format_creds {
    my $self  = shift;
    my $creds = shift;

    my $str = q{};
    foreach my $try ( @{$creds} ) {
        my $db = $try->{'database'} || q{};
        $str .= $try->{'username'} . q{:} . $try->{'password'} . q{/} . $db . q{,};
    }

    return $str;
}

sub _detect_credentials {
    my $self = shift;

    # detect credentials and set _username and _password
    # first try user-supplied creds
    foreach my $username ( keys %{ $self->creds() } ) {
        my $password = $self->creds()->{$username};
        next unless $username && $password;
        $self->logger()->log( message => 'Adding user supplied credentials for checking ('.$username.q{:}.$password.q{)}, level => 'debug', );
        my $arg_ref = {
            'username' => $username,
            'password' => $password,
            'database' => 'mysql',
        };
        push( @{ $self->candidates() }, $arg_ref, );
    }

    # next try user-supplied keys
    foreach my $key ( @{ $self->keys() } ) {
        my $username = $self->config()->get_scalar( $key . '::Username' );
        my $password = $self->config()->get_scalar( $key . '::Password' );
        my $database = $self->config()->get_scalar( $key . '::Database', { Default => 'mysql', }, );
        my $socket   = $self->config()->get_scalar( $key . '::Socket', );
        next unless $username && $password;
        $self->logger()->log( message => 'Adding user supplied key for checking ('.$username.q{:}.$password.q{/}.$database.q{)}, level => 'debug', );
        my $arg_ref = {
            'username' => $username,
            'password' => $password,
            'database' => $database,
        };
        $arg_ref->{'socket'} = $socket if ( $socket && -e $socket );
        push( @{ $self->candidates() }, $arg_ref );
    }

    # last try the hard-coded defaults
    # try all known mysql users, but try some before all others
    my @try_first = qw(DBA DebianSysMaint Replication Monitoring User);
    my @all_keys = $self->config()->get_array('MTK::MySQL::User');
    my @keys = List::MoreUtils::uniq (@try_first, @all_keys);
    # TODO let MTK::MySQL::User::USERNAME be an array
    foreach my $key (@keys) {
        my $username = $self->config()->get_scalar( 'MTK::MySQL::User::' . $key . '::Username' );
        my @passwords = $self->config()->get_array( 'MTK::MySQL::User::' . $key . '::Password' );
        my $database = $self->config()->get_scalar( 'MTK::MySQL::User::' . $key . '::Database', { Default => 'mysql', }, );
        my $socket   = $self->config()->get_scalar( $key . '::Socket', );
        foreach my $password (@passwords) {
            next unless $username && $password;
            $database ||= 'mysql';
            $self->logger()
              ->log( message => 'Adding hard-coded key for checking (MTK::MySQL::User::' . $key . q{ -> }.$username.q{:}.$password.q{/}.$database.q{)}, level => 'debug', );
            my $arg_ref = {
                'username' => $username,
                'password' => $password,
                'database' => $database,
            };
            $arg_ref->{'socket'} = $socket if ( $socket && -e $socket );
            push( @{ $self->candidates() }, $arg_ref, );
        }
    }

    # try .my.cnf and debian.cnf, too
    my $mycnf = $ENV{'HOME'} . '/.my.cnf';
    if ( my ( $username, $password, $socket ) = $self->_read_mycnf($mycnf) ) {
        $self->logger()->log( message => 'Adding auto-detected credentials for checking from .my.cnf ('.$username.q{:}.$password.q{)}, level => 'debug', );
        my $arg_ref = {
            'username' => $username,
            'password' => $password,
            'database' => 'mysql',
        };
        $arg_ref->{'socket'} = $socket if ( $socket && -e $socket );
        push( @{ $self->candidates() }, $arg_ref, );
    }
    else {
        $self->logger()->log( message => 'No credentials found in '.$mycnf, level => 'debug', );
    }
    my $debiancnf = '/etc/mysql/debian.cnf';
    if ( my ( $username, $password, $socket ) = $self->_read_mycnf($debiancnf) ) {
        $self->logger()->log( message => 'Adding auto-detected credentials for checking from /etc/debian.cnf ('.$username.q{:}.$password.q{)}, level => 'debug', );
        my $arg_ref = {
            'username' => $username,
            'password' => $password,
            'database' => 'mysql',
        };
        $arg_ref->{'socket'} = $socket if ( $socket && -e $socket );
        push( @{ $self->candidates() }, $arg_ref, );
    }
    else {
        $self->logger()->log( message => 'No credentials found in '.$debiancnf, level => 'debug', );
    }

    $self->logger()->log( message => 'Prepared search. Candidates: ' . $self->_format_creds( $self->candidates() ), level => 'debug', );

    foreach my $try ( 1 .. $self->num_tries() ) {
        foreach my $creds ( sort_root_up( @{ $self->candidates() } ) ) {
            my $username = $creds->{'username'};
            my $password = $creds->{'password'};
            my $database = $creds->{'database'};

            if ( !$username || !$password || !$database ) {
                $self->logger()->log( message => 'Invalid set of credentials! Missing either Username, Password or Database!', level => 'error', );
                next;
            }

            push( @{ $self->tries() }, $creds );

            $self->logger()->log( message => 'Trying '.$username.q{:}.$password.' ...', level => 'debug', );

            if ( $self->_test_connection( $username, $password, $database, $creds->{'socket'} ) ) {
                $self->_username($username);
                $self->_password($password);
                $self->database($database);
                $self->logger()->log( message => 'Search finished. Found valid credentials ('.$username.q{:}.$password.q{/}.$database.q{)}, level => 'debug', );
                return 1;
            }
            else {
                $self->logger()->log( message => 'Continuing search ...', level => 'debug', );
            }
        }

        if ( $try < $self->num_tries() ) {
            sleep $self->sleep();
        }
    }

    # nothing found ...
    $self->logger()->log( message => 'Search exhausted. No valid credentials found.', level => 'debug', );
    return;
}

=method sort_root_up

Custom sort order, pushing root to the top.

=cut
# DGR: speeeeed
## no critic (RequireArgUnpacking)
sub sort_root_up {
    my @first = grep { $_->{'username'} eq 'root' } @_;
    my @remainder = grep { $_->{'username'} ne 'root' } @_;

    return ( @first, @remainder );
}
## use critic

# Read a mysql style my.cnf/debian.cnf/whatever
sub _read_mycnf {
    my $self     = shift;
    my $filename = shift;

    my $Cnf = Config::Tiny::->read($filename);
    if ( -e $filename && $Cnf && $Cnf->{'client'} && $Cnf->{'client'}->{'user'} && $Cnf->{'client'}->{'password'} ) {
        my $username = $Cnf->{'client'}->{'user'};
        my $password = $Cnf->{'client'}->{'password'};
        my $socket   = $Cnf->{'client'}->{'socket'} || q{};
        $self->logger()->log( message => "Read credentials from $filename. Username: $username, Password: $password", level => 'debug', );
        return ( $username, $password, $socket );
    }
    return;
}

sub _test_connection {
    my $self     = shift;
    my $username = shift;
    my $password = shift;
    my $database = shift;
    my $socket   = shift;

    $socket ||= $self->socket();

    my $arg_ref = {
        'username'       => $username,
        'password'       => $password,
        'logger'         => $self->logger(),
        'display_errors' => 0,
    };

    if($self->hostname()) {
        $arg_ref->{'hostname'} = $self->hostname();
        if($self->port()) {
            $arg_ref->{'port'} = $self->port();
        } else {
            $arg_ref->{'port'} = 3306;
        }
    } elsif($socket) {
        $arg_ref->{'socket'} = $socket;
    }

    my $DB;
    try {
        $DB = MTK::DB::->new($arg_ref);
    };

    if ( $DB && $DB->valid() ) {
        $DB->database($database) if $database;
        $self->logger()->log( message => 'Valid Credentials! Successfully connected to ' . $self->hostname() . " as $username:$password.", level => 'debug', );

        # the currently active db connection can be retrieved with $obj->dbh() and will be detroyed by
        # the garbage collection if it is no longer referenced
        $self->dbh($DB);
        return 1;
    }
    else {
        $self->logger()->log( message => 'Invalid Credentials! Failed to connect to ' . $self->hostname() . " as $username:$password.", level => 'debug', );
        return;
    }
}

=method username

Returns the detected username.

=cut
sub username {
    my $self = shift;

    return $self->_username();
}

=method password

Returns the detected password.

=cut
sub password {
    my $self = shift;

    return $self->_password();
}

no Moose;
__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

MTK::DB::Credentials - mysql credential detection

=head1 SYNOPSIS

 use MTK::DB::Credentials;
 my $Creds = MTK::DB::Credentials::->new({
 	'hostname'	=> 'localhost',
 	'config'	=> $Config,
 	'keys'		=> [qw(MTK::RepliSnap MTK::DB)],
 	'creds'		=> {
 		'john'	=> 'doe',
 	},
 });
 $Creds->username();

=head1 ACKNOWLEDGEMENT

This module was originally developed for eGENTIC Systems. With approval from eGENTIC Systems,
this module was generalized and published, for which the authors would like to express their
gratitude.

=cut
