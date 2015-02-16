package MTK::DB;

# ABSTRACT: MTK DBI Wrapper

use 5.010_000;
use mro 'c3';
use feature ':5.10';

use Moose;
use namespace::autoclean;

# use IO::Handle;
# use autodie;
# use MooseX::Params::Validate;
use English qw( -no_match_vars );

use Carp;
use File::Temp qw/tempfile/;
use File::Blarf;
use DBI qw(:sql_types);
use List::MoreUtils;

use MTK::DB::Statement;
use Sys::Run;

=attr username

The username used to connect to the DB

=cut

has 'username' => (
  'is'       => 'ro',
  'isa'      => 'Str',
  'required' => 1,
);

=attr password

The password used to connect to the DB

=cut

has 'password' => (
  'is'  => 'ro',
  'isa' => 'Str',
);

=attr hostname

The hostname to connect to

=cut

has 'hostname' => (
  'is'       => 'ro',
  'isa'      => 'Str',
  'required' => 1,
);

=attr database

Current database

=cut

has 'database' => (
  'is'      => 'rw',
  'isa'     => 'Str',
  'trigger' => \&_trigger_database,
);

=attr port

Database port, defaults to 3306

=cut

has 'port' => (
  'is'      => 'ro',
  'isa'     => 'Int',
  'default' => 3306,
);

=attr socket

Database socket, default is empty

=cut

has 'socket' => (
  'is'  => 'ro',
  'isa' => 'Maybe[Str]',
);

=attr raise_error

When set errors will be thrown, not caught

=cut

has 'raise_error' => (
  'is'      => 'rw',
  'isa'     => 'Bool',
  'default' => 0,
);

=attr display_errors

When set errors will be displayed, not hidden

=cut

has 'display_errors' => (
  'is'      => 'rw',
  'isa'     => 'Bool',
  'default' => 1,
);

has '_dbh' => (
  'is'        => 'rw',
  'isa'       => 'DBI::db',
  'lazy'      => 1,
  'builder'   => '_init_dbh',
  'predicate' => 'has_dbh',
);

=attr connect_timeout

The timeout for connecting to the DB

=cut

has 'connect_timeout' => (
  'isa'     => 'Num',
  'is'      => 'rw',
  'default' => 30,
);

=attr utf8

When true the connection is set to UTF-8

=cut

has 'utf8' => (
  'is'      => 'ro',
  'isa'     => 'Bool',
  'default' => 1,
);

has 'sys' => (
  'is'      => 'rw',
  'isa'     => 'Sys::Run',
  'lazy'    => 1,
  'builder' => '_init_sys',
);

has '_pid' => (
  'is'      => 'ro',
  'isa'     => 'Int',
  'lazy'    => 1,
  'builder' => '_init_pid',
);

with qw(Log::Tree::RequiredLogger);

sub _init_pid {
  return $PID;
}

sub _trigger_database {
  my ( $self, $new_db, $old_db ) = @_;

  $self->do( 'USE `' . $new_db . q{`} );

  return 1;
} ## end sub _trigger_database

sub _init_sys {
  my $self = shift;

  my $Sys = Sys::Run::->new( { 'logger' => $self->logger(), } );

  return $Sys;
} ## end sub _init_sys

=method BUILD

Initialize pid for fork check

=cut

sub BUILD {
  my $self = shift;

  # IMPORTANT: initialize our pid!
  $self->_pid();

  if ( $self->valid() ) {
    return 1;
  }
  else {
    my $msg = 'Connect check at BUILD time failed.';
    $self->logger()->log( message => $msg, level => 'error', );
    confess($msg);
  }
} ## end sub BUILD

sub _init_dbh {
  my $self = shift;

  my $dbh          = undef;
  my $prev_timeout = 0;
  my $success      = eval {
    local $SIG{ALRM} = sub { die "alarm-mtk-db-connect\n" };
    $prev_timeout = alarm $self->connect_timeout();
    $dbh          = DBI->connect(
      $self->dsn(),
      undef, undef,
      {
        RaiseError        => 0,
        PrintError        => 0,
        mysql_enable_utf8 => $self->utf8(),
      }
    );
    if ( $DBI::VERSION >= 1.614 ) {
      $dbh->{AutoInactiveDestroy} = 1;
    }
    1;
  };
  alarm $prev_timeout;
  if ( $EVAL_ERROR && $EVAL_ERROR eq "alarm-mtk-db-connect\n" ) {
    my $msg = 'Connection w/ DSN ' . $self->dsn() . ' timed out after ' . $self->connect_timeout() . 's.';
    $self->logger()->log( message => $msg, level => 'error', ) if $self->display_errors();
    confess($msg);
  }
  elsif ( !$success || $EVAL_ERROR ) {
    my $msg = 'Unknown error during connection: ' . $EVAL_ERROR;
    $self->logger()->log( message => $msg, level => 'error', );
    confess($msg);
  }
  elsif ( $dbh && ref($dbh) eq 'DBI::db' && $dbh->ping() ) {

    #$self->logger()->log( message => 'Connected to DB w/ DSN ' . $self->dsn(), level => 'debug', );
    return $dbh;
  }
  else {
    my $msg = 'Connection to DB failed with DSN ' . $self->dsn() . q{: } . DBI->errstr;
    $self->logger()->log( message => $msg, level => 'error', ) if $self->display_errors();
    confess($msg);
  }
} ## end sub _init_dbh

=method fork_check

test if we were forked and act accordingly

see http://www.perlmonks.org/?node_id=594175

=cut

sub fork_check {
  my $self = shift;

  if ( $self->_pid() != $PID ) {
    my $child_dbh = $self->_dbh()->clone();
    $self->_dbh()->{InactiveDestroy} = 1;
    $self->_dbh($child_dbh);    # this should also destroy our copy of the parent's dbh!
  }

  return 1;
} ## end sub fork_check

=method clone

NOT YET IMPLEMENTED!

=cut

sub clone {
  my $self = shift;

  my $child_dbh = $self->_dbh()->clone();

  # TODO should be implemented ...

  return;
} ## end sub clone

=method ping

See DBI::ping

=cut

sub ping {
  my $self = shift;

  return $self->_dbh()->ping();
}

=method DEMOLISH

Disconnect from DB.

=cut

sub DEMOLISH {
  my $self = shift;

  # we MUST NOT call disconnect if we're not already connected
  # or the call to disconnect will (try to) connect to the DBH first!
  # This will create nasty stacktraces for invalid credentials (which may be
  # perfectly valid during connection probing!)
  if ( $self->has_dbh() ) {
    return $self->disconnect();
  }
  return 1;
} ## end sub DEMOLISH

=method unlock_tables

Issue UNLOCK TABLES to the DB

=cut

sub unlock_tables {
  my $self = shift;

  my $query = 'UNLOCK TABLES';
  my $sth   = $self->prepexec($query);
  if ( !$sth ) {
    return;
  }
  $sth->finish();
  return 1;
} ## end sub unlock_tables

=method flush_tables

Issue FLUSH TABLES to the DB

=cut

sub flush_tables {
  my $self               = shift;
  my $no_write_to_binlog = shift;

  my $query = 'FLUSH ';
  if ($no_write_to_binlog) {
    $query .= 'NO_WRITE_TO_BINLOG ';
  }
  $query .= 'TABLES';
  my $sth = $self->prepexec($query);
  if ( !$sth ) {
    return;
  }
  $sth->finish();
  return 1;
} ## end sub flush_tables

=method flush_logs

Issue FLUSH LOGS to the DBH.

=cut

sub flush_logs {
  my $self               = shift;
  my $no_write_to_binlog = shift;

  my $query = 'FLUSH ';
  if ($no_write_to_binlog) {
    $query .= 'NO_WRITE_TO_BINLOG ';
  }
  $query .= 'LOGS';
  my $sth = $self->prepexec($query);
  if ( !$sth ) {
    return;
  }
  $sth->finish();
  return 1;
} ## end sub flush_logs

=method flush_tables_with_read_lock

Flush all tables and acquire a global read lock.

=cut

sub flush_tables_with_read_lock {
  my $self               = shift;
  my $no_write_to_binlog = shift;

  my $query = 'FLUSH ';
  if ($no_write_to_binlog) {
    $query .= 'NO_WRITE_TO_BINLOG ';
  }
  $query .= 'TABLES WITH READ LOCK';
  my $sth = $self->prepexec($query);
  if ( !$sth ) {
    return;
  }
  $sth->finish();
  return 1;
} ## end sub flush_tables_with_read_lock

=method list_tables

List all tables known to MySQL

Returns    : Either an ARRAY or HASHREF

Parameters in $opts:

Opts/ion      : Type      : Value
OnlyDB        : STRING    : only return tables from this DB
ReturnHashRef : BOOL      : return a hash with extended information instead of the default array of `DB`.`TABLE`?
Excludes      : ARRAY     : filter out tables matching these patterns
IncludeOnly   : ARRRAY    : only return tables matching these patterns
AddHost       : STRING    : add a hostname in front of the table name
OnlyEngine    : STRING    : return only tables of this engine type (MyISAM, InnoDB, etc.)
NotOnlyBaseTables : BOOL  : return NOT only base tables (i.e. views too)

=cut

sub list_tables {
  my $self = shift;
  my $opts = shift || {};

  my $host = undef;
  if ( $opts->{'AddHost'} ) {
    $host = $opts->{'AddHost'};
  }

  # Get a list of all Tables in this DBMS
  my $query = 'SELECT TABLE_SCHEMA,TABLE_NAME,ENGINE,TABLE_ROWS,DATA_LENGTH,INDEX_LENGTH,DATA_FREE,';
  $query .= 'UNIX_TIMESTAMP(CREATE_TIME),UNIX_TIMESTAMP(UPDATE_TIME) FROM information_schema.TABLES WHERE 1';
  my @params = ();

  # only from this db
  if ( $opts->{'OnlyDB'} ) {
    $query .= ' AND TABLE_SCHEMA = ?';
    push( @params, $opts->{'OnlyDB'} );
  }

  # only report tables with these engine
  if ( $opts->{'OnlyEngine'} ) {

    # Corrupted MyISAM tables are sometimes reported as NULL.
    $query .= ' AND (ENGINE = ?  OR ENGINE IS NULL)';
    push( @params, $opts->{'OnlyEngine'} );
  } ## end if ( $opts->{'OnlyEngine'...})

  # also include non-base tables (views ...)?
  if ( !$opts->{'NotOnlyBaseTables'} ) {
    $query .= q{ AND TABLE_TYPE = 'BASE TABLE'};
  }
  my $sth = $self->prepare($query)
    or confess q{Can't prepare statement};
  my @tables = ();
  my %tables = ();
  $sth->execute(@params)
    or confess q{Can't execute prepared statement};
TWHILE: while ( my ( $schema, $table, $engine, $rows, $data_length, $index_length, $data_free, $create_time, $update_time ) = $sth->fetchrow_array() ) {

    # we're asked to only process include_tables
    if ( $opts->{'IncludeOnly'} ) {
      my $found = 0;
      foreach my $pattern ( @{ $opts->{'IncludeOnly'} } ) {
        my $match_pattern = $pattern;
        my $dbtable       = $schema . q{.} . $table;
        if ($host) {
          $dbtable = $host . q{.} . $dbtable;
        }

        # handle wildcards
        # replace shell style glob by proper regexp matches
        # i.e. LOG_* => LOG_[^.]*
        # or LOG_MED?_XX => LOG_MED[^.]?_XX
        $match_pattern =~ s/[*]/[^.]*/g;
        $match_pattern =~ s/[?]/[^.]?/g;
        $self->logger()->log( message => 'Checking table ' . $dbtable . ' against include_table w/ rule: ' . $pattern, level => 'debug', );
        if ( $dbtable =~ /$match_pattern$/gi ) {
          $self->logger()->log( message => 'Found Table ' . $dbtable . ' due for include rule: ' . $pattern, level => 'debug', );
          $found = 1;
        }
      } ## end foreach my $pattern ( @{ $opts...})

      # skip this table if it is not in the include_list
      if ($found) {
        $self->logger()
          ->log( message => 'Found table ' . $schema . q{.} . $table . ' in include list. Now checking against exclude rules (if present)', level => 'debug', );
      }
      else {
        $self->logger()->log( message => 'IncludeOnly TRUE but table ' . $schema . q{.} . $table . ' not found in include list. Skipping.', level => 'debug', );
        next TWHILE;
      }
    } ## end if ( $opts->{'IncludeOnly'...})

    # exclude excludes
    if ( $opts->{'Excludes'} ) {
      foreach my $pattern ( @{ $opts->{Excludes} } ) {
        my $match_pattern = $pattern;
        my $dbtable       = $schema . q{.} . $table;
        if ($host) {
          $dbtable = $host . q{.} . $dbtable;
        }

        # handle wildcards
        # replace shell style glob by proper regexp matches
        # i.e. LOG_* => LOG_[^.]*
        # or LOG_MED?_XX => LOG_MED[^.]?_XX
        $match_pattern =~ s/[*]/[^.]*/g;
        $match_pattern =~ s/[?]/[^.]?/g;
        $self->logger()->log( message => 'Checking table ' . $dbtable . ' against exclude_table ' . $match_pattern . ' (' . $pattern . ')', level => 'debug', );
        if ( $dbtable =~ /$match_pattern/gi ) {
          $self->logger()->log( message => 'Skipping Table ' . $dbtable . ' due to exclude rule: ' . $pattern, level => 'debug', );
          next TWHILE;
        }
        else {
          $self->logger()->log( message => 'Table ' . $dbtable . ' not excluded.', level => 'debug', );
        }
      } ## end foreach my $pattern ( @{ $opts...})
    } ## end if ( $opts->{'Excludes'...})

    # only add if statefile is older than 1 hour and not in excludes list,
    # also skip pre-defined excludes.
    # information_schema: A "virtual" table defined by SQL99 (?)
    # lost+found: Present if the data-directory (/var/lib/mysql) is on a partition of its own
    # REPLICHECK: use to store replication information by this (and other) script. Not vital.
    if ( $schema ne 'information_schema' && $schema ne 'lost+found' ) {
      if ($host) {
        $self->logger()->log( message => 'Adding table: ' . $host . q{.} . $schema . q{.} . $table, level => 'debug', );
      }
      else {
        $self->logger()->log( message => 'Adding table: ' . $schema . q{.} . $table, level => 'debug', );
      }
      push( @tables, q{`} . $schema . q{`.`} . $table . q{`} );
      $engine ||= 1;
      $tables{$schema}{$table}{'engine'}       = uc($engine);
      $tables{$schema}{$table}{'rows'}         = $rows;
      $tables{$schema}{$table}{'data_length'}  = $data_length;
      $tables{$schema}{$table}{'index_length'} = $index_length;
      $tables{$schema}{$table}{'data_free'}    = $data_free;
      $tables{$schema}{$table}{'create_time'}  = $create_time;
      $tables{$schema}{$table}{'update_time'}  = $update_time;
    } ## end if ( $schema ne 'information_schema'...)
  } ## end TWHILE: while ( my ( $schema, $table...))
  $sth->finish();
  if ( $opts->{ReturnHashRef} ) {
    return \%tables;
  }
  else {
    return @tables;
  }
} ## end sub list_tables

=method get_columns

Return all columns of a given table.

=cut

sub get_columns {
  my $self   = shift;
  my $schema = shift;
  my $table  = shift;
  my $opts   = shift || {};

  my @cols = ();

  # return all columns from the given table
  my $sql = q{DESC `} . $schema . q{`.`} . $table . q{`};
  my $sth = $self->prepexec($sql);
  if ( !$sth ) {
    warn 'Could not get columns for table ' . $schema . q{.} . $table . "\n";
    return @cols;
  }

ROW: while ( my ( $Field, $Type, $Null, $Key, $Default, $Extra ) = $sth->fetchrow_array() ) {
    if ( $opts->{'SkipCols'} && List::MoreUtils::any { $Field =~ m/^$_$/i } @{ $opts->{'SkipCols'} } ) {
      next ROW;
    }
    if ( $opts->{'Extended'} ) {
      push(
        @cols,
        {
          'Field'   => $Field,
          'Type'    => $Type,
          'Null'    => $Null,
          'Key'     => $Key,
          'Default' => $Default,
          'Extra'   => $Extra,
        }
      );
    } ## end if ( $opts->{'Extended'...})
    else {
      push( @cols, $Field );
    }
  } ## end ROW: while ( my ( $Field, $Type...))
  $sth->finish();

  return \@cols;
} ## end sub get_columns

=method attribute

Get a named attribute from the DBH.

=cut

sub attribute {
  my $self   = shift;
  my $attrib = shift;

  return $self->_dbh()->{$attrib};
} ## end sub attribute

=method lock_tables

$dbh->lock_tables({ 'table1' => 'WRITE', 'table2' => 'READ', });

=cut

sub lock_tables {
  my $self      = shift;
  my $table_ref = shift;
  my $opts      = shift || {};

  my $sql = 'LOCK TABLES ';
  foreach my $table ( %{$table_ref} ) {
    my $type = $table_ref->{$table};
    next unless $type;
    ## no critic (ProhibitFixedStringMatches)
    if ( $type !~ m/^(?:READ|WRITE)$/i ) {
      ## use critic
      $type = 'READ';
    }
    $sql .= $table . ' ' . $type . ' ';
  } ## end foreach my $table ( %{$table_ref...})

  my $sth = $self->prepexec($sql);
  if ($sth) {
    $sth->finish();
    return 1;
  }
  else {
    return;
  }
} ## end sub lock_tables

=method get_db_size

get the size of a DB in MB

=cut

sub get_db_size {
  my $self = shift;
  my $db   = shift;
  my $opts = shift || {};

  my $sql    = q{SELECT SUM(DATA_LENGTH)/(1024*1024) AS SIZEMB FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA = ?};
  my $sth    = $self->prepexec( $sql, $db );
  my $sizemb = $sth->fetchrow_array();
  $sth->finish();
  return $sizemb;
} ## end sub get_db_size

=method show_child_handles

Recursively prints all child handles of the given handle.


See Also http://search.cpan.org/~timb/DBI-1.611/DBI.pm#ChildHandles_%28array_ref%29

=cut

sub show_child_handles {
  my $self  = shift;
  my $h     = shift;
  my $level = shift || 0;

  $self->fork_check();

  printf "%sh %s %s\n", $h->{Type}, "\t" x $level, $h;
  foreach my $child ( grep { defined } @{ $h->{ChildHandles} } ) {
    $self->show_child_handles( $child, $level + 1 );
  }
  return;
} ## end sub show_child_handles

=method finish_child_handles

Recursivly (up to 10 levels of nesting) visit all child handles an call finish() on them.

See Also http://search.cpan.org/~timb/DBI-1.611/DBI.pm#ChildHandles_%28array_ref%29

=cut

sub finish_child_handles {
  my $self   = shift;
  my $handle = shift;
  my $level  = shift || 0;

  $self->fork_check();

  # prevent infinite loops
  return if $level > 10;
  foreach my $child ( grep { defined } @{ $handle->{ChildHandles} } ) {

    # st -> (prepared) Statement
    if ( $child->{Type} eq 'st' ) {
      $child->finish();
    }
    else {
      $self->finish_child_handles( $child, $level + 1 );
    }
  } ## end foreach my $child ( grep { ...})
  return;
} ## end sub finish_child_handles

=method disconnect

Disconnect from the DB.

=cut

sub disconnect {
  my $self = shift;

  $self->fork_check();

  $self->finish_child_handles();
  return $self->_dbh()->disconnect();
} ## end sub disconnect

=method do

Prepare and execute a given SQL string.

=cut

## no critic (ProhibitBuiltinHomonyms)
sub do {
  my $self   = shift;
  my $sqlstr = shift;

  my $sth = $self->prepexec($sqlstr);
  if ($sth) {
    $sth->finish();
    return 1;
  }
  else {
    $self->logger()->log( message => 'Execution of Query ' . $sqlstr . ' failed w/ error: ' . $self->dbh()->errstr(), level => 'error', );
    return;
  }
} ## end sub do
## use critic

=method dsn

Return the active DSN.

=cut

sub dsn {
  my $self = shift;

  my $dsn = 'DBI:mysql:user=' . $self->username();
  if ( $self->password() ) {
    $dsn .= ';password=' . $self->password();
  }
  if ( $self->socket() ) {
    $dsn .= ';mysql_socket=' . $self->socket();
  }
  else {
    $dsn .= ';host=' . $self->hostname() . ';port=' . $self->port();
  }
  if ( $self->database() ) {
    $dsn .= ';database=' . $self->database();
  }

  return $dsn;
} ## end sub dsn

=method drop_table

Drop (delete) a given table.

=cut

sub drop_table {
  my $self   = shift;
  my $schema = shift;
  my $table  = shift;

  my $sql = 'DROP TABLE `' . $schema . '`.`' . $table . '`';
  my $sth = $self->prepexec($sql);

  if ($sth) {
    $sth->finish();
    return 1;
  }
  else {
    return;
  }
} ## end sub drop_table

=method prepexec

Prepare and execute a statement

=cut

sub prepexec {
  my ( $self, $sqlstr, @params ) = @_;

  my $sth;
  $sth = $self->prepare( $sqlstr, { RaiseError => 1, } );
  if ( $sth->execute(@params) ) {

    #$self->logger()->log( message => 'Executed Query ('.$sqlstr.') w/o error.', level => 'debug', );
  }
  else {
    my $msg = q{Couldn't execute statement: } . $sqlstr . ' - Error: ' . $sth->errstr;
    $self->logger()->log( message => $msg, level => 'error', );
    if ( $self->raise_error() ) {
      confess($msg);
    }
    else {
      return;
    }
  } ## end else [ if ( $sth->execute(@params...))]
  return $sth;
} ## end sub prepexec

=method check_connection

Return true if the DB connection is ok.

=cut

sub check_connection {
  my $self = shift;

  $self->fork_check();

  if ( !$self->_dbh()->ping() ) {
    $self->{'_dbh'} = $self->_init_dbh();
  }

  if ( $self->_dbh()->ping() ) {
    return 1;
  }
  else {
    return;
  }
} ## end sub check_connection

=method prepare

Prepare a given SQL string into a statement and return the stmt object.

=cut

sub prepare {
  my $self   = shift;
  my $sqlstr = shift;

  $self->check_connection();

  my $sth = MTK::DB::Statement::->new(
    {
      'sqlstr' => $sqlstr,
      '_dbh'   => $self->_dbh(),    # TODO HIGH here lies danger ... Statement should access the dbh only via parent!
      'parent' => $self,
    }
  );

  if ( $sth && $sth->valid() ) {
    return $sth;
  }
  else {
    return;
  }
} ## end sub prepare

=method repair_table_and_restart

Try (very hard) to repair a given tabel and restart the slave threads
afterwards.

=cut

sub repair_table_and_restart {
  my $self     = shift;
  my $database = shift;
  my $table    = shift;
  my $opts     = shift || {};

  $self->stop_slave();

  $opts->{NoBinlog} = 1;
  if ( !$self->repair_table( $database, $table, $opts ) ) {
    $self->logger()->log( message => 'Could not repair ' . $database . q{.} . $table . ' with a simple repair!', level => 'notice', );
    $opts->{Extended} = 1;
    if ( !$self->repair_table( $database, $table, $opts ) ) {
      $self->logger()->log( message => 'Could not repair ' . $database . q{.} . $table . ' with a extended repair!', level => 'notice', );
      $opts->{UseFrm} = 1;
      if ( !$self->repair_table( $database, $table, $opts ) ) {
        $self->logger()->log( message => 'Could not repair ' . $database . q{.} . $table . ' with a extended/use_frm repair! Aborting.', level => 'notice', );
        return;
      }
    } ## end if ( !$self->repair_table...)
  } ## end if ( !$self->repair_table...)
  if ( $self->start_slave($opts) ) {
    $self->logger()->log( message => 'Repaired table ' . $database . q{.} . $table . ' and re-started slave.', level => 'notice', );
    return 1;
  }
  else {
    $self->logger()->log( message => 'Repaired table ' . $database . q{.} . $table . ' but could not re-start slave.', level => 'error', );
    return;
  }
} ## end sub repair_table_and_restart

=method has_repair_running

Return true if some repair table statement is already running.

=cut

sub has_repair_running {
  my ( $self, $schema, $table, $opts ) = @_;

  my $sql = 'SHOW FULL PROCESSLIST';
  my $sth = $self->prepexec($sql);

  while ( my ( $id, $user, $host, $db, $cmd, $time, $state, $info ) = $sth->fetchrow_array() ) {
    next unless $db;
    next unless $db eq $schema;

    # DGR: it's perfectly readable that way
    ## no critic (ProhibitComplexRegexes)
    if ( $info =~ m/REPAIR\s+(?:(?:NO_WRITE_TO_BINLOG|LOCAL)\s+)?TABLE(?:\s+.|\s+|\s+.*\..?)\Q$table\E\.?/i ) {
      ## use critic
      $sth->finish();
      return $id;
    }
  } ## end while ( my ( $id, $user, ...))
  $sth->finish();

  return;
} ## end sub has_repair_running

=method repair_table

Repair a given table.

=cut

sub repair_table {
  my ( $self, $schema, $table, $opts ) = @_;

  if ( my $tid = $self->has_repair_running( $schema, $table ) ) {
    $self->logger()->log( message => 'Repair blocked by another repair on the same table: ' . $tid, level => 'warning', );
    return;
  }

  my $sql = 'REPAIR ';
  $sql .= 'NO_WRITE_TO_BINLOG ' if $opts->{NoBinlog};
  $sql .= 'TABLE `' . $schema . '`.`' . $table . '` ';
  $sql .= 'QUICK '              if $opts->{Quick};
  $sql .= 'EXTENDED '           if $opts->{Extended};
  $sql .= 'USE_FRM '            if $opts->{UseFrm};

  $self->logger()->log( message => 'Trying to repair table ' . $schema . q{.} . $table . ' with Query ' . $sql, level => 'debug', );
  my $sth = $self->prepexec($sql);
  if ($sth) {
    my ( $op, $msg_type, $msg_text ) = $sth->fetchrow_array();
    if ( $msg_type =~ m/error/i ) {

      # repair failed
      $self->logger()->log( message => 'Repair failed on table ' . $schema . q{.} . $table . ' with error: ' . $msg_text, level => 'error', );
      return;
    } ## end if ( $msg_type =~ m/error/i)
    else {
      $self->logger()->log( message => 'Repaired table ' . $schema . q{.} . $table . '. Message: ' . $msg_text, level => 'debug', );
      return 1;
    }
  } ## end if ($sth)
  else {
    $self->logger()->log( message => 'Repair failed. Prepare statement failed.', level => 'error', );
    return;
  }
} ## end sub repair_table

=method get_sec_behind_master

convenience function, retrieve the
(mysql reported) lag behind the master

=cut

sub get_sec_behind_master {
  my $self = shift;
  my $opts = shift || {};

  my $slave_status = $self->get_slave_status();

  if ( $slave_status && defined( $slave_status->{'Seconds_Behind_Master'} ) ) {
    return $slave_status->{'Seconds_Behind_Master'};
  }
  else {
    $self->logger()->log( message => 'Replication not running', level => 'debug', );
    return -1;
  }
} ## end sub get_sec_behind_master

=method purge_binary_logs_to

Remove all binary (master) log files up to a given file.

=cut

sub purge_binary_logs_to {
  my $self     = shift;
  my $log_name = shift;

  my $query = 'PURGE BINARY LOGS TO ?';
  my $sth = $self->prepexec( $query, $log_name );
  return unless $sth;

  $sth->finish();
  return 1;
} ## end sub purge_binary_logs_to

=method is_master

Return true if this host if configured as a master (it may have no slaves).

=cut

sub is_master {
  my $self = shift;

  # master only if:
  # - read only == off
  # - not slave
  my ( $file, $pos ) = $self->get_master_status();

  if ( $file && $pos ) {
    return 1;
  }
  return;
} ## end sub is_master

=method is_slave

Return true if this host if configured as a slave (is may be not running).

=cut

sub is_slave {
  my $self = shift;

  my $ss = $self->get_slave_status();

  if ($ss) {

    # these keys are requried for a valid slave
    foreach my $key (qw(Master_User Master_Host Master_Log_File Read_Master_Log_Pos)) {
      if ( !$ss->{$key} ) {
        return;
      }
    }
    return 1;
  } ## end if ($ss)
  return;
} ## end sub is_slave

=method slave_is_running

Return true if this host is running as a slave.

=cut

sub slave_is_running {
  my $self = shift;

  my $ss = $self->get_slave_status();

  if ( $ss && $ss->{'Slave_SQL_Running'} eq 'Yes' && $ss->{'Slave_IO_Running'} eq 'Yes' ) {
    return 1;
  }
  return;
} ## end sub slave_is_running

=method get_master_status

Return the master log file and the master log position as a list.

=cut

sub get_master_status {
  my $self = shift;
  my $opts = shift || {};

  $self->check_connection();

  my $query = 'SHOW MASTER STATUS';
  if ( my $status = $self->_dbh()->selectrow_hashref($query) ) {
    return ( $status->{'File'}, $status->{'Position'} );
  }
  else {
    my $msg = 'Could not get Master status';
    if ( $self->_dbh()->errstr ) {
      $msg .= q{: } . $self->_dbh()->errstr;
    }
    $msg .= "\n";
    $self->logger()->log( message => $msg, level => 'debug', );
    return;
  } ## end else [ if ( my $status = $self...)]
} ## end sub get_master_status

=method get_slave_status

Return the slave status hashref of this host.

=cut

sub get_slave_status {
  my $self = shift;
  my $opts = shift || {};

  $self->check_connection();

  my $query  = 'SHOW SLAVE STATUS';
  my $status = $self->_dbh()->selectrow_hashref($query);
  return $status;
} ## end sub get_slave_status

=method master_pos_wait

Wait until this host has reached a given master position or the timeout expired.

=cut

sub master_pos_wait {
  my $self     = shift;
  my $log_file = shift;
  my $log_pos  = shift;
  my $timeout  = shift || 60;

  $self->check_connection();

  my $query = q{SELECT MASTER_POS_WAIT('} . $log_file . q{',} . $log_pos, q{ . } . $timeout . q{)};
  my @row = $self->_dbh()->selectrow_array($query);

  if ( $row[0] == 0 ) {
    return 1;    # slave up to date
  }
  else {
    return 0;    # slave not up to date
  }
} ## end sub master_pos_wait

=method let_replication_catch_up

Wait until this hosts replication thread has caught up with its
master or the given timeout has expired.

=cut

sub let_replication_catch_up {
  my $self = shift;
  my $opts = shift || {};

  my $sec_behind = $self->get_sec_behind_master($opts);
  $self->logger()->log( message => "Slave is now $sec_behind s behind master.", level => 'debug', );
  my $starttime = time();
  $opts->{ReplicationTimeout} ||= 3600;

  # give some time for the replication to initially start
  while ( $sec_behind < 0 ) {
    if ( $opts->{ReplicationTimeout} && ( time() - $starttime > $opts->{ReplicationTimeout} ) ) {
      $self->logger()->log( message => "Timeout of $opts->{ReplicationTimeout} s reached. [I]", level => 'debug', );
      return;
    }

    my $sleep = 10;
    if ( $opts->{InitialReplicationSleep} ) {
      $sleep = $opts->{InitialReplicationSleep};
    }
    $self->logger()->log( message => "Sleeping $sleep s to allow the replication to start up.", level => 'debug', );
    sleep $sleep;
    $sec_behind = $self->get_sec_behind_master($opts);
    $self->logger()->log( message => "Slave is now $sec_behind s behind master. [I]", level => 'debug', );
  } ## end while ( $sec_behind < 0 )

  while ( $sec_behind > 2 ) {
    if ( $opts->{ReplicationTimeout} && ( time() - $starttime > $opts->{ReplicationTimeout} ) ) {
      $self->logger()->log( message => "Timeout of $opts->{ReplicationTimeout} s reached. [II]", level => 'debug', );
      return;
    }

    sleep 10;
    $sec_behind = $self->get_sec_behind_master($opts);
    $self->logger()->log( message => "Slave is now $sec_behind s behind master. [II]", level => 'debug', );
  } ## end while ( $sec_behind > 2 )

  if ( $sec_behind < 0 ) {
    $self->logger()->log( message => "Replication not running! (Sec_behind: $sec_behind)", level => 'debug', );
    return;
  }

  my $duration = time() - $starttime;

  $self->logger()->log( message => 'Replication up to date after ' . $duration . ' s.', level => 'debug', );

  return 1;
} ## end sub let_replication_catch_up

=method start_slave

Start this hosts slave threads.

=cut

sub start_slave {
  my $self = shift;
  my $opts = shift || {};

  my $query = 'START SLAVE';

  if ( $opts->{UntilMasterLogPos} && $opts->{UntilMasterLogFile} ) {
    $query .= ' UNTIL MASTER_LOG_POS=' . $opts->{UntilMasterLogPos};
    $query .= q{, MASTER_LOG_FILE='} . $opts->{UntilMasterLogFile} . q{'};
  }

  if ( $self->prepexec($query) ) {
    $self->logger()->log( message => 'Query: ' . $query . ' - OK', level => 'debug', );
    return 1;
  }
  else {
    $self->logger()->log( message => 'Query: ' . $query . ' - ERROR', level => 'debug', );
    return;
  }
} ## end sub start_slave

=method stop_slave

Stop this hosts slave threads.

=cut

sub stop_slave {
  my $self = shift;
  my $opts = shift || {};

  my $query = 'STOP SLAVE';

  $self->logger()->log( message => 'Query: ' . $query, level => 'debug', );

  return $self->prepexec($query);
} ## end sub stop_slave

=method reset_slave

Reset this hosts slave information.

=cut

sub reset_slave {
  my $self = shift;
  my $opts = shift || {};

  my $query = 'RESET SLAVE';

  $self->logger()->log( message => 'Query: ' . $query, level => 'debug', );

  return $self->prepexec($query);
} ## end sub reset_slave

=method reset_master

Reset this hosts master information.

=cut

sub reset_master {
  my $self = shift;
  my $opts = shift || {};

  my $query = 'RESET MASTER';

  $self->logger()->log( message => 'Query: ' . $query, level => 'debug', );

  return $self->prepexec($query);
} ## end sub reset_master

=method start_replication

Attach this host to the given master and start the slave.

=cut

# DGR: can't change arguments passing w/o breaking a lot of stuff
## no critic (ProhibitManyArgs)
sub start_replication {
  my $self            = shift;
  my $master_host     = shift;
  my $repli_user      = shift;
  my $repli_pass      = shift;
  my $master_log_file = shift;
  my $master_log_pos  = shift;
  my $opts            = shift || {};

  $self->stop_slave($opts);

  if ( $self->change_master_to( $master_host, $repli_user, $repli_pass, $master_log_file, $master_log_pos, $opts )
    && $self->start_slave($opts) )
  {

    $self->logger()->log( message => 'OK', level => 'debug', );
    return 1;
  } ## end if ( $self->change_master_to...)
  else {

    $self->logger()->log( message => 'ERROR', level => 'error', );
    return;
  }
} ## end sub start_replication
## use critic

=method change_master_to

Change this hosts master (i.e. make it a slave).

=cut

# DGR: can't change arguments passing w/o breaking a lot of stuff
## no critic (ProhibitManyArgs)
sub change_master_to {
  my $self            = shift;
  my $master_host     = shift;
  my $repli_user      = shift;
  my $repli_pass      = shift;
  my $master_log_file = shift;
  my $master_log_pos  = shift;
  my $opts            = shift || {};

  if ( !$repli_user || !$repli_pass ) {

    $self->logger()->log( message => 'No replication user and/or password given. Continuing.', level => 'warning', );
  }

  my $query = 'CHANGE MASTER TO MASTER_HOST=?, MASTER_USER=?, MASTER_PASSWORD=?, MASTER_LOG_FILE=?, MASTER_LOG_POS=?';
  my $prepq = $self->prepare($query);

  # we need integer type here or dbi will convert the
  # logpos to a string and mysql will cry very loud!
  $prepq->bind_param( 5, $master_log_pos, { TYPE => SQL_INTEGER } );

  $self->logger()->log(
    message => 'Query: ' . $query . ", Args: $master_host, $repli_user, $repli_pass, $master_log_file, $master_log_pos",
    level   => 'debug',
  );

  my $rv = $prepq->execute( $master_host, $repli_user, $repli_pass, $master_log_file, int($master_log_pos) );
  $prepq->finish();

  return $rv;
} ## end sub change_master_to
## use critic

=method skip_statement

Set the global SQL Slave skip counter to 1.

=cut

sub skip_statement {
  my $self = shift;
  my $opts = shift || {};

  if ( $self->stop_slave() ) {
    if ( $self->set_global_variable( 'SQL_SLAVE_SKIP_COUNTER', 1, ) ) {
      if ( $self->start_slave($opts) ) {
        $self->logger()->log( message => 'Skipped statement and re-started slave.', level => 'notice', );
        return 1;
      }
      else {
        $self->logger()->log( message => 'Incremented Skip Counter but could not start slave again.', level => 'error', );
        return;
      }
    } ## end if ( $self->set_global_variable...)
  } ## end if ( $self->stop_slave...)
  else {
    $self->logger()->log( message => 'Could not stop slave.', level => 'warning', );
    return;
  }
} ## end sub skip_statement

=method valid

Returns true if this object is in a valid state.

=cut

sub valid {
  my $self = shift;

  if ( $self->_dbh() && $self->_dbh()->ping() ) {
    return 1;
  }
  else {
    return;
  }
} ## end sub valid

=method creds_from_cnf

read username and password from any mysql cnf file

=cut

# TODO HIGH we have this method a trillion times ... DRY!
sub creds_from_cnf {
  my $self = shift;
  my $file = shift;

  my ( $username, $password, $hostname, $socket );

  my @lines = File::Blarf::slurp( $file, { Chomp => 1, } );

  my $ok = 0;
  foreach my $line (@lines) {
    if ($ok) {
      my ( $key, $value ) = split /\s*=\s*/, $line;
      if ( $key eq 'user' ) {
        $username = $value;
      }
      elsif ( $key eq 'password' ) {
        $password = $value;
      }
      elsif ( $key eq 'host' ) {
        $hostname = $value;
      }
      elsif ( $key eq 'socket' ) {
        $socket = $value;
      }
    } ## end if ($ok)
    elsif ( $line =~ m/^\[client\]/ ) {
      $ok = 1;
    }
    elsif ( $line =~ m/^\[/ ) {
      $ok = 0;
    }
    last if ( $username && $password && $hostname && $socket );
  } ## end foreach my $line (@lines)
  return ( $username, $password, $hostname, $socket );
} ## end sub creds_from_cnf

=method check_slave_options

check if mandatory slave options are set

these are for now:
- read_only = 1

=cut

sub check_slave_options {
  my $self = shift;
  my $opts = shift || {};

  return $self->check_options( { 'read_only' => 'ON', 'server_id' => qr{\d+}, }, 0, $opts );
} ## end sub check_slave_options

=method check_master_options

check if mandatory master options are set

these are for now:
- read_only = 0
- log_bin = 1

=cut

sub check_master_options {
  my $self = shift;
  my $opts = shift || {};

  return $self->check_options(
    {
      'read_only' => 'OFF',
      'log_bin'   => 'ON',
      'server_id' => '\d+',
    },
    0, $opts
  );
} ## end sub check_master_options

=method check_options

Check if a given set of options if given

=cut

sub check_options {
  my $self          = shift;
  my $check_options = shift;
  my $optional      = shift // 1;
  my $opts          = shift || {};

  my $query = 'SHOW VARIABLES';
  my $prepq = $self->prepare($query);
  if ( !$prepq ) {
    $self->logger()->log( message => "Could not prepare statement for query $query. Error: " . $self->_dbh()->errstr, level => 'error', );
    return;
  }
  if ( !$prepq->execute() ) {
    $self->logger()->log( message => "Could not execute statement for query $query. Error: " . $prepq->errstr, level => 'error', );
    return;
  }

  my $ok = 1;

  while ( my ( $opt, $val ) = $prepq->fetchrow_array() ) {
    if ( $optional && $check_options->{$opt} && $val !~ m/$check_options->{$opt}/ ) {
      $self->logger()
        ->log( message => 'Optional option ' . $opt . ' failed. Should be ' . $check_options->{$opt} . ' but really is ' . $val, level => 'notice', );
      $ok = 0;
    }
    elsif ( $check_options->{$opt} && $val !~ m/$check_options->{$opt}/ ) {
      $self->logger()
        ->log( message => 'Mandatory option ' . $opt . ' failed. Should be ' . $check_options->{$opt} . ' but really is ' . $val, level => 'notice', );
      return;    # not optional -> fail
    }
  } ## end while ( my ( $opt, $val )...)
  $prepq->finish();
  return $ok;
} ## end sub check_options

=method set_global_value

set a global variable

This method is DEPRECATED and will be removed in a future release. DO NOT USE!

=cut

sub set_global_value {
  my $self     = shift;
  my $variable = shift || return;
  my $value    = shift || return;
  my $opts     = shift || {};

  my ( $package, $filename, $line ) = caller();
  $self->logger()->log(
    message => 'DEPRECATION WARNING: set_global_value is deprecated! Use set_global_variable instead! At: ' . $package . q{, } . $filename . q{:} . $line,
    level   => 'warning',
  );

  return $self->set_global_variable( $variable, $value, $opts );
} ## end sub set_global_value

=method set_global_variable

Set a global mysql variable.

=cut

sub set_global_variable {
  my $self     = shift;
  my $variable = shift || return;
  my $value    = shift || return;
  my $opts     = shift || {};

  my $query = 'SET GLOBAL `' . $variable . '` = ?';

  $self->logger()->log( message => 'Query: ' . $query . ', Value: ' . $value, level => 'debug', );

  my $prepq = $self->prepare($query);

  $value =~ s/^\s+//;
  $value =~ s/\s+$//;
  if ( $value =~ m/^\d+$/ ) {
    $prepq->bind_param( 1, $value, { TYPE => SQL_INTEGER } );
  }
  else {
    $prepq->bind_param( 1, $value );
  }
  unless ( $prepq->execute() ) {
    $self->logger()->log( message => 'Could not execute query: ' . $query . ', errstr: ' . DBI->errstr, level => 'warning', );
    $prepq->finish();
    return;
  }
  else {
    $prepq->finish();
    return 1;
  }
} ## end sub set_global_variable

=method set_persistent_variable

Set a mysql server varialbe in the mysql config.

=cut

sub set_persistent_variable {
  my $self     = shift;
  my $variable = shift;
  my $value    = shift;
  my $opts     = shift || {};

  my $section = $opts->{'MyCnfSection'} || 'mysqld';

  my $remote_filename = '/etc/mysql/conf.d/' . $variable . '.cnf';

  my ( $fh, $filename ) = File::Temp::tempfile( UNLINK => 1, );
  print $fh '[' . $section . ']' . "\n";
  print $fh $variable . ' = ' . $value . "\n";
  close($fh);
  my $cmd = 'scp ' . $filename . ' ' . $self->hostname() . ':' . $remote_filename;
  return $self->sys()->run_cmd($cmd);
} ## end sub set_persistent_variable

=method get_variable

Get a variable from the mysql config

=cut

sub get_variable {
  my $self = shift;
  my $variable = shift || return;

  my $query = 'SHOW VARIABLES LIKE ?';
  my $prepq = $self->prepare($query);
  if ( $prepq->execute($variable) ) {
    my $value = $prepq->fetchrow_array();
    $prepq->finish();
    return $value;
  }
  else {
    $self->logger()->log( message => 'Could not execute query: ' . $query . ', errstr: ' . DBI->errstr, level => 'warning', );
    $prepq->finish();
    return;
  }
} ## end sub get_variable

=method has_slaves

Returns true if their are any slaves connected to this host.

=cut

sub has_slaves {
  my $self = shift;

  return scalar( @{ $self->list_slaves() } );
}

=method list_slaves

List all slaves connected to this host.

=cut

sub list_slaves {
  my $self = shift;

  my $sql = 'SHOW FULL PROCESSLIST';
  my $sth = $self->prepexec($sql);

  if ( !$sth ) {
    return;
  }

  my @slaves = ();
  while ( my ( $id, $user, $host, $db, $command, $time, $state, $info ) = $sth->fetchrow_array() ) {
    if ( $command =~ m/Binlog Dump/i ) {
      if ( $host =~ m/:/ ) {
        my $port;
        ( $host, $port ) = split /:/, $host, 2;
      }
      push( @slaves, $host );
    } ## end if ( $command =~ m/Binlog Dump/i)
  } ## end while ( my ( $id, $user, ...))

  return \@slaves;
} ## end sub list_slaves

=method err

See L<DBI>.

=cut

## no critic (ProhibitBuiltinHomonyms)
sub err {
## use critic
  my $self = shift;

  return $self->_dbh()->err();
} ## end sub err

=method errstr

See L<DBI>.

=cut

sub errstr {
  my $self = shift;

  return $self->_dbh()->errstr();
}

=method state

See L<DBI>.

=cut

## no critic (ProhibitBuiltinHomonyms)
sub state {
## use critic
  my $self = shift;

  return $self->_dbh()->state;
} ## end sub state

=method set_err

See L<DBI>.

=cut

sub set_err {
  my ( $self, @args ) = @_;

  return $self->_dbh()->set_err(@args);
}

=method trace

See L<DBI>.

=cut

sub trace {
  my ( $self, @args ) = @_;

  return $self->_dbh()->trace(@args);
}

=method trace_msg

See L<DBI>.

=cut

sub trace_msg {
  my ( $self, @args ) = @_;

  return $self->_dbh()->trace_msg(@args);
}

=method func

See L<DBI>.

=cut

sub func {
  my ( $self, @args ) = @_;

  return $self->_dbh()->func(@args);
}

# 'can' is not implemented

=method parse_trace_flags

See L<DBI>.

=cut

sub parse_trace_flags {
  my ( $self, @args ) = @_;

  return $self->_dbh()->parse_trace_flags(@args);
}

=method parse_trace_flag

See L<DBI>.

=cut

sub parse_trace_flag {
  my ( $self, @args ) = @_;

  return $self->_dbh()->parse_trace_flag(@args);
}

=method private_attribute_info

See L<DBI>.

=cut

sub private_attribute_info {
  my $self = shift;

  return $self->_dbh()->private_attribute_info();
}

# 'swap_inner_handle' is not implemented

=method visit_child_handles

See L<DBI>.

=cut

sub visit_child_handles {
  my ( $self, @args ) = @_;

  return $self->_dbh()->visit_child_handles(@args);
}

=method last_insert_id

See L<DBI>.

=cut

sub last_insert_id {
  my ( $self, @args ) = @_;

  return $self->_dbh()->last_insert_id(@args);
}

=method selectrow_array

See L<DBI>.

=cut

sub selectrow_array {
  my ( $self, $sql, @args ) = @_;

  my $sth = $self->prepexec( $sql, @args );

  my @data = $sth->fetchrow_array();
  $sth->finish();

  return @data;
} ## end sub selectrow_array

=method selectrow_arrayref

See L<DBI>.

=cut

sub selectrow_arrayref {
  my ( $self, $sql, @args ) = @_;

  my $sth = $self->prepexec( $sql, @args );

  my $data_ref = $sth->fetchrow_arrayref();
  $sth->finish();

  return $data_ref;
} ## end sub selectrow_arrayref

=method selectrow_hashref

See L<DBI>.

=cut

sub selectrow_hashref {
  my ( $self, $sql, @args ) = @_;

  my $sth = $self->prepexec( $sql, @args );

  my $data_ref = $sth->fetchrow_hashref();
  $sth->finish();

  return $data_ref;
} ## end sub selectrow_hashref

=method selectall_arrayref

See L<DBI>.

=cut

sub selectall_arrayref {
  my ( $self, $sql, @args ) = @_;

  my $sth = $self->prepexec( $sql, @args );

  my $data_ref = $sth->fetchall_arrayref();
  $sth->finish();

  return $data_ref;
} ## end sub selectall_arrayref

=method selectall_hashref

See L<DBI>.

=cut

sub selectall_hashref {
  my ( $self, $sql, @args ) = @_;

  my $sth = $self->prepexec( $sql, @args );

  my $data_ref = $sth->fetchall_hashref();
  $sth->finish();

  return $data_ref;
} ## end sub selectall_hashref

no Moose;
__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

MTK::DB - MTK DBI Wrapper

=head1 ACKNOWLEDGEMENT

This module was originally developed for eGENTIC Systems. With approval from eGENTIC Systems,
this module was generalized and published, for which the authors would like to express their
gratitude.

=cut
