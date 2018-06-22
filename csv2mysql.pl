#!/usr/bin/perl -w
#
# Version 1.0
#
# Usage: csv2mysql.pl file.csv SystemName HeaderColumn[,HeaderColumn] FieldTable[,FieldTable]
#
# The program ... .
#
# 06/2018, Alexey Tarasenko, atarasenko@mail.ru
#
# Red Hat:
# yum install perl-DBD-MySQL
# v2
# yum install perl-Text-CSV_XS*
#

# PERL MODULES WE WILL BE USING
use DBI;
use DBD::mysql;
#v2
#use Text::CSV_XS;

# ./csv2mysql.pl sansw_rep.csv san "Switch Name+,Port#+,WWN,Wave+,Speed,WWPN,Alias" "swname,pnum,wwn,wave,speed,wwpn,host"
# Check for valid number of arguments
if ( ( $#ARGV < 1 ) || ( $#ARGV > 4 ) ) {
    die("Usage: csv2mysql csvfile system headers fields\n");
}

# Open the Comma Separated Variable file
open( CSVFILE, $ARGV[0] ) or die "$ARGV[0]: $!";

#my $columns="Switch Name+,Port#+";
#my $fields="field1,field2";
my $system=$ARGV[1];
my $columns=$ARGV[2];
my $fields=$ARGV[3];
my $values="";

# CONFIG VARIABLES
#$host = "10.100.0.209";
$host = "racktables.bank.srv";
$database="racktables_log";
$platform = "mysql";
$port = "3306";
$user = "stor2rrd";
$pswd = "4FF43435tyty!";

# DATA SOURCE NAME
$dsn = "DBI:$platform:$database:$host:$port";

# PERL DBI CONNECT (RENAMED HANDLE)
$dbh = DBI->connect($dsn, $user, $pswd) or die "Unable to connect: $DBI::errstr\n";

$tablename="kkr_processing_log";
$tablename2="kkr_".$system."_log";

$dbh->do("UPDATE $tablename SET flag=1 WHERE system='$system'");

$sth = $dbh->prepare("SELECT count(*) FROM $tablename2");
$sth->execute();
my ($count) = $sth->fetchrow_array;
$sth->finish;
if ( $count ) {
    $sth = $dbh->prepare("DELETE FROM $tablename2 WHERE 1=1 LIMIT $count");
    $sth->execute();
}

my @var = split( ',', $fields);
foreach my $val (@var) { $values = $values.'?,'; }
if ( length($values) > 1 ) { $values = substr($values,0,-1); }

my %field_hash = ();
my $rows = 0;
$sth = $dbh->prepare("INSERT INTO $tablename2($fields) VALUES ($values)");
#v1
while(<CSVFILE>) {
    chop;
    my @Fld = split /,/;
        
    if ( $rows == 0 ) {
        my $col = 0;
        foreach my $token (@Fld) {
    	    $field_hash{ $token } = $col;
    	    $col++;
    	}
    } else {
    	my $col = 0;
	my @Vals = ();
	my @ColNames = split( ',', $columns );
    	foreach my $token (@ColNames) { 
    	    my $var = $Fld[ $field_hash {$token} ];
    	    if (! defined $var) { $var = ""; }
    	    $Vals[$col] = $var;
	    $col++; 
	}
    	$sth->execute( @Vals );
    }
    $rows++;
}
#v2
# Create a new CSV parsing object
#my $csv = Text::CSV_XS->new;
#while (<CSVFILE>) {
#    if ( $csv->parse($_) ) {
#        my @Fld = $csv->fields;
#
#        if ( $rows == 0 ) {
#    	    my $col = 0;
#    	    foreach my $token (@Fld) {
#    		$field_hash{ $token } = $col;
#    		$col++;
#	    }
#    	} else {
#    	    my $col = 0;
#	    my @Vals = ();
#	    my @ColNames = split( ',', $columns );
#    	    foreach my $token (@ColNames) { 
#    		my $var = $Fld[ $field_hash {$token} ];
#    		#if (! defined $var) { $var = ""; }
#    		$Vals[$col] = $var;
#    		$col++; 
#    	    }
#    	    $sth->execute( @Vals );
#    	}
#    $rows++;
#    } else {
#        my $err = $csv->error_input;
#        print "Text::CSV_XS parse() failed on argument: ", $err, "\n";
#    }
#}

# For view result of work
#$sth = $dbh->prepare("SELECT * FROM $tablename2");
#$sth->execute();
#while ( @row = $sth->fetchrow_array ) {
#    print "@row\n"; 
#}
#$sth->finish;

$dbh->do("UPDATE $tablename SET flag=0 WHERE system='$system'");
$dbh->disconnect;
