#
#   prove -v t/test.t [ :: [--verbose] [--dry_run] ]
#
# Assumes containers are built and running (use up.sh)
#
# Dependencies:
#   jq
# To install:
#   cpan App::cpanminus
#   # restart shell, then get the dependencies:
#   cpanm --installdeps .
# For more details:
#   http://www.cpan.org/modules/INSTALL.html

use 5.16.3;
use strict;
use warnings;

use Getopt::Long qw(GetOptions);

use Test::More tests => 9;
use Test::File::Contents;

use lib './t';
use Support;

our $verbose = 0;
our $dry_run = 0;

our $OBJID="test_object_id";
our $SUBMITTER_ID='test@email.com';

# read the .env file
use Dotenv;      
Dotenv->load;
Dotenv->load('t/test.env');

our $HOST_PATH = "http://localhost:$ENV{'HOST_PORT'}"; #8082;


GetOptions('dry_run' => \$dry_run,
	   'verbose' => \$verbose) or die "Usage: prove -v t/$0 [ :: [--verbose] ] \n";
if($verbose){
    print("+ dry_run: $dry_run\n");
    print("+ verbose: $verbose\n");
    print("+ HOST_PORT: $ENV{'HOST_PORT'}\n");
}

my $fn ;

cleanup_out();

$fn = "test-1.json";
files_eq(f($fn), cmd("GET",    $fn, "services"),                                                                  "Get all services for fuse-agent");
$fn = "test-2.json";
files_eq(f($fn), cmd("GET",    $fn, "services/providers"),                                                        "Get providers configured for this agent");
$fn = "test-3.json";
files_eq(f($fn), cmd("GET",    $fn, "services/tools"),                                                            "Get tools configured for this agent");
$fn = "test-4.json";
files_eq(f($fn), cmd("POST",    $fn, "submitters/add?submitter_id=${SUBMITTER_ID}", "-H 'accept: application/json' -d ''"),
	                                                                                                          "Create submitter");
$fn = "test-5.json";
files_eq(f($fn), cmd("POST",    $fn, "submitters/add?submitter_id=${SUBMITTER_ID}", "-H 'accept: application/json' -d ''"),
	                                                                                                          "Try to create same submitter again");
$fn = "test-6.json";
files_eq(f($fn), cmd("GET",    $fn, "submitters/search?within_minutes=1"),                                        "Get list of very recently created submitters");
$fn = "test-7.json";
generalize_output($fn, cmd("GET", rawf($fn), "submitters/{$SUBMITTER_ID}"), ["created_time"]);
files_eq(f($fn), "t/out/${fn}",                                                                                   "Get submitter metadata");
$fn = "test-8.json";
files_eq(f($fn), cmd("DELETE",    $fn, "submitters/delete/${SUBMITTER_ID}"),                                       "Delete submitters");
$fn = "test-9.json";
files_eq(f($fn), cmd("DELETE",    $fn, "submitters/delete/${SUBMITTER_ID}"),                                       "Try to delete same submitter again");


