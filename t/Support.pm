### support functions
package Support;

require Exporter;
@ISA = qw(Exporter);
our @EXPORT = qw($verbose $no_download $dry_run $HOST_PATH $EMAIL $GROUPID $APIKEY $dl_taskid $analysis_taskid 
		 cleanup_out f cmd generalize_output rawf dl_poll json_struct);

use Cpanel::JSON::XS;
use Switch;
use Carp qw(croak);

sub f {
    return "./t/expected/$_[0]";
}
sub rawf {  return "raw.$_[0]";}


sub cleanup_out {
    my $dch = "";
    if($dry_run != 0) { $dch="D";}
    if($verbose) {
	print("+$dch [cleanup] rm ./t/out/*\n");
	`rm -f ./t/out/*\n`;
    }
}


sub cmd {
    my ($type, $fn, $endpoint, $post_args) = @_;
    
    my $cmd;
    switch($type) {
    	case "DELETE" { $cmd=sprintf("curl -X 'DELETE'  '${HOST_PATH}/%s' -H 'accept: application/json'", $endpoint); }
	case "GET"    { $cmd=sprintf("curl -X 'GET'     '${HOST_PATH}/%s' -H 'accept: application/json'", $endpoint); }
	case "POST"   { $cmd=sprintf("curl -X 'POST' %s '${HOST_PATH}/%s' -H 'accept: application/json'", $endpoint, $post_args);}
	else { print("+! [cmd] ERROR ${type} not recognized.\n");	}
    }

    my ($ext) = $fn =~ /(\.[^.]+)$/;
    my $outfile = "./t/out/${fn}";
    my $parse_json = "";
    if($ext eq ".json") {
	$parse_json = "| python -m json.tool | jq --sort-keys ";
    }
    my $cmd = ${cmd} . " 2> /dev/null $parse_json > $outfile";
    $dch = "";
    if($dry_run == 0) {
	`$cmd`; 
    } else {
	$dch = "D";
    }
    if($verbose == 1) {
	print("+$dch [cmd] ${cmd}\n");
	print("+$dch [cmd] OUTFILE($ext)=(${outfile})\n");
	if($dry_run == 0) {
	    if($ext eq ".json"){
		print(`python -m json.tool ${outfile}` . "\n");
	    } else {
		print (`awk -F, 'NR<3{print \$1\",\"\$2\",\"\$3,\"...\"}' ${outfile}` ."\n");
	    }
	}
    }
    if($dry_run == 0 ) {
	return ${outfile};
    } else {
	# this is a dry run, return the expected output since an output file isn't generated
	return "./t/expected/${fn}";
    }
}

sub generalize_output {
    # remove ephemeral info from meatadata for comparison
    my ($fn, $raw_outfile, $fields_ref) = @_;
    $json = json_struct($raw_outfile);
    foreach my $field (@$fields_ref) {
	$json->{$field} = "xxx";
    }
    my $fn_raw = rawf($fn);
    open $fh_raw, ">", $raw_outfile;
    print $fh_raw encode_json($json);
    close $fh_raw;
    `cat $raw_outfile |python -m json.tool|jq --sort-keys > t/out/${fn}`;
}

sub json_struct {
    my $outfile = $_[0];
    my $json_text = do {
	open(my $json_fh, "<:encoding(UTF-8)", $outfile)
	    or die("Can't open \"$outfile\": $!\n");
	local $/;
	<$json_fh>
    };
    return decode_json($json_text);
}

1;
