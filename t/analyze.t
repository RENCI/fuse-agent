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

use Test::More tests => 7;
use Test::File::Contents;

use lib './t';
use Support;

our $verbose = 0;
our $dry_run = 0;

our $DATASET_OBJID="test_dataset_object_id";
our $RESULT_OBJID="test_result_object_id";
our $SUBMITTER_ID='test-xxx@email.com';

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


my $service_id="fuse-provider-upload";
$fn = "analyze-1.json";
files_eq(f($fn), cmd("POST", $fn, "objects/load?requested_object_id=${DATASET_OBJID}",
		     "-F service_id=${service_id} " .
		     "-F submitter_id=${SUBMITTER_ID} " .
		     "-F data_type=class_dataset_expression " .
		     "-F version=1.0 " .
		     "-F 'optional_file_expression=@./t/input/expression.csv;type=application/csv' " .
		     "-H 'Content-Type: multipart/form-data' -H 'accept: application/json'"),
	                                                                                                   "($fn) Submit csv file");
sleep(2); # wait for job queue to catch up


# analyze
$service_id="fuse-tool-pca";
$fn = "analyze-2.json";
files_eq(f($fn), cmd("POST", $fn, "analyze?requested_results_object_id=${RESULT_OBJID}",
		     "-d 'service_id=${service_id}&submitter_id=${SUBMITTER_ID}&number_of_components=3&dataset=${DATASET_OBJID}&description=&results_provider_service_id=fuse-provider-upload' " .
		     "-H 'Content-Type: application/x-www-form-urlencoded' -H 'accept: application/json'"),
	                                                                                                   "($fn) Analyze csv file");
sleep(2); # wait for job queue to catch up

$fn = "analyze-3.json";
generalize_output($fn, cmd("GET", rawf($fn), "objects/{$RESULT_OBJID}"), ["agent","provider"]);
files_eq(f($fn), "t/out/${fn}",                                                                            "($fn) Get info about result DRS object");

$fn = "analyze-4.json";
generalize_output($fn, cmd("GET", rawf($fn), "objects/url/{$RESULT_OBJID}/type/filetype_results_PCATable"), ["url"]);
files_eq(f($fn), "t/out/${fn}",                                                                            "($fn) Get URL for result object's file");

###
# cleanup

# xxx not working
$fn = "analyze-5.json";
generalize_output($fn, cmd("DELETE", rawf($fn), "delete/{$RESULT_OBJID}"), ["stderr"]);
files_eq(f($fn), "t/out/${fn}",                                                                            "($fn) Delete the result object");

$fn = "analyze-6.json";
generalize_output($fn, cmd("DELETE", rawf($fn), "delete/{$DATASET_OBJID}"), ["stderr"]);
files_eq(f($fn), "t/out/${fn}",                                                                            "($fn) Delete the dataset object");

$fn = "analyze-7.json";
files_eq(f($fn), cmd("DELETE",    $fn, "submitters/delete/${SUBMITTER_ID}"),                               "Delete submitter created by post");
