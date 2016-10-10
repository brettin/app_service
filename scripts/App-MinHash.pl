#
# The application.
#

use strict;
use Carp;
use Cwd 'abs_path';
use Data::Dumper;
use File::Temp;
use File::Basename;
use IPC::Run 'run';
use JSON;

use Bio::KBase::AppService::AppConfig;
use Bio::KBase::AppService::AppScript;
use Bio::KBase::AuthToken;

my $script_dir = abs_path(dirname(__FILE__));
my $data_url = Bio::KBase::AppService::AppConfig->data_api_url;
my $script = Bio::KBase::AppService::AppScript->new(\&process_variation_data);
my $rc = $script->run(\@ARGV);
exit $rc;

our $global_ws;
our $global_token;

sub process_variation_data {
    my ($app, $app_def, $raw_params, $params) = @_;

    print "Proc data ", Dumper($app_def, $raw_params, $params);
    my $time1 = `date`;

    $global_token = $app->token();
    $global_ws = $app->workspace;

    my $output_folder = $app->result_folder();

    # my $tmpdir = File::Temp->newdir();
    # my $tmpdir = File::Temp->newdir( CLEANUP => 0 );
    my $tmpdir = "/scratch4/tmp/oIGe_LLBbt";

	print "Localizing params to $tmpdir\n";
	print "Params ", Dumper($params);
    $params = localize_params($tmpdir, $params);

	run_mash($params->{read});

    my @outputs;
    push @outputs, map { [ $_, 'txt' ] } glob("$tmpdir/*.tsv $tmpdir/*.txt");
    push @outputs, map { [ $_, 'vcf' ] } glob("$tmpdir/*.vcf");
    push @outputs, map { [ $_, 'html'] } glob("$tmpdir/*.html");
    push @outputs, map { [ $_, 'bam' ] } glob("$tmpdir/*.bam");
    push @outputs, map { [ $_, 'unspecified' ] } glob("$tmpdir/*.tbi");

    print STDERR '\@outputs = '. Dumper(\@outputs);
    return @outputs;

    for (@outputs) {
		my ($ofile, $type) = @$_;
		if (-f "$ofile") {
            my $filename = basename($ofile);
            print STDERR "Output folder = $output_folder\n";
            print STDERR "Saving $ofile => $output_folder/$filename ...\n";
	    	$app->workspace->save_file_to_file("$ofile", {}, "$output_folder/$filename", $type, 1,
					       (-s "$ofile" > 10_000 ? 1 : 0), # use shock for larger files
					       $global_token);
		} else {
	    	warn "Missing desired output file $ofile\n";
		}
    }

    my $time2 = `date`;
    write_output("Start: $time1"."End:   $time2", "$tmpdir/DONE");
}

sub run_mash {
	my ($query) = @_;
	my $mash = "mash";
	verify_cmd($mash);
	my ($threads, $reference) = (2, "/scratch2/mash/all.msh");
	my $cmd = [$mash, "dist", "-p", $threads, $reference, $query];
	my ($out, $err) = run_cmd($cmd, 1);
	print $out;
}

sub curl_text {
    my ($url) = @_;
    my @cmd = ("curl", curl_options(), $url);
    print STDERR join(" ", @cmd)."\n";
    my ($out) = run_cmd(\@cmd);
    return $out;
}

sub curl_json {
    my ($url) = @_;
    my $out = curl_text($url);
    my $hash = JSON::decode_json($out);
    return $hash;
}

sub curl_options {
    my @opts;
    my $token = get_token()->token;
    push(@opts, "-H", "Authorization: $token");
    push(@opts, "-H", "Content-Type: multipart/form-data");
    return @opts;
}

sub run_cmd {
    my ($cmd, $verbose) = @_;
    my ($out, $err);
    print STDERR "cmd = ", join(" ", @$cmd) . "\n\n" if $verbose;
    run($cmd, '>', \$out, '2>', \$err)
        or die "Error running cmd=@$cmd, stdout:\n$out\nstderr:\n$err\n";
    print STDERR "STDOUT:\n$out\n" if $verbose;
    print STDERR "STDERR:\n$err\n" if $verbose;
    return ($out, $err);
}

# the purpose of this sub is to localize data from the workspace to localhost
sub localize_params {
    my ($tmpdir, $params) = @_;
    die "tmpdir $tmpdir does not exist" unless -e $tmpdir;
	$params->{read} = get_ws_file($tmpdir, $params->{read});
    return $params;
}

sub get_ws {
    return $global_ws;
}

sub get_token {
    return $global_token;
}

sub get_ws_file {
    my ($tmpdir, $id) = @_;
    # return $id; # DEBUG
    my $ws = get_ws();
    my $token = get_token();

    my $base = basename($id);
    my $file = "$tmpdir/$base";
    # return $file; # DEBUG

    my $fh;
    open($fh, ">", $file) or die "Cannot open $file for writing: $!";

print $fh "test";

    print STDERR "GET WS => $tmpdir $base $id\n";
    print STDERR "ws = $ws and is type ", ref($ws), "and can ", $ws->can("copy_files_to_handles"), "\n";
    print STDERR "token = ", Dumper ($token), "\n";
    system("ls -la $tmpdir");

    eval {
    print "calling ws->copy_files_to_handles\n";
	$ws->copy_files_to_handles(1, $token, [[$id, $fh]]);
    print "done calling ws-copy_files_to_handles\n";
    };
    if ($@)
    {
	die "ERROR getting file $id\n$@\n";
    }
    close($fh);
    print "$id $file:\n";
    system("ls -la $tmpdir");

    return $file;
}

sub write_output {
    my ($string, $ofile) = @_;
    open(F, ">$ofile") or die "Could not open $ofile";
    print F $string;
    close(F);
}

sub verify_cmd {
    my ($cmd) = @_;
    print STDERR "verifying executable $cmd ...\n";
    system("which $cmd >/dev/null") == 0 or die "Command not found: $cmd\n";
}

sub sysrun { system(@_) == 0 or confess("FAILED: ". join(" ", @_)); }
