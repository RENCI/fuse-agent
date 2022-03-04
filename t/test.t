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

use Test::More tests => 1;
use Test::File::Contents;

use lib './t';
use Support;

our $verbose = 0;
our $dry_run = 0;

# read the .env file
use Dotenv;      
Dotenv->load;

our $HOST_PATH = "http://localhost:$ENV{'API_PORT'}"; #8082;


GetOptions('dry_run' => \$dry_run,
	   'verbose' => \$verbose) or die "Usage: prove -v t/$0 [ :: [--verbose] ] \n";
if($verbose){
    print("+ dry_run: $dry_run\n");
    print("+ verbose: $verbose\n");
    print("+ API_PORT: $ENV{'API_PORT'}\n");
}

my $fn ;

cleanup_out();

$fn = "test-1.json";
files_eq(f($fn), cmd("GET",    $fn, "config"),                                                                    "Get config for this plugin");

