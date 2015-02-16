#!perl -T

use Test::More tests => 3;

BEGIN {
  use_ok('MTK::DB::Credentials') || print "Bail out!
";
  use_ok('MTK::DB::Statement') || print "Bail out!
";
  use_ok('MTK::DB') || print "Bail out!
";
} ## end BEGIN

diag("Testing MTK::DB $MTK::DB::VERSION, Perl $], $^X");
