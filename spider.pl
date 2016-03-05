#!C:/Perl/bin/perl
#COPYRIGHT Ppctweakies 2003-2015
use Getopt::Std;
use vars qw($opt_i $opt_r);

use Encode::Guess qw/ euc-jp shiftjis utf8 7bit-jis /;

use Encode qw/ from_to /;
use Fcntl ':flock'; 

require WWW::RobotRules;
require LWP::UserAgent;


$| = 1; # Disable STDOUT buffering

my $config_file = "spider.conf";
my $keywords_regexp    = "<meta[^>]+name\s*\=\s*\"keywords\"[^>]+content\s*\=\s*\"([^\">]*)";
my $description_regexp = "<meta[^>]+name\s*\=\s*\"description\"[^>]+content\s*\=\s*\"([^\">]*)";

#============================================
# DEFAULT CONFIG SETTINGS
#============================================
my $seeds_file  = "seeds.txt";
my $db_file     = "spider.dat";
my $db_layout   = "##URL##\t##KEYWORDS##\t##DESCRIPTION##\n";
my $max_description_length = 400;
my $max_keywords_length    = 800;
my $generate_keywords      = 1;
my $domain_max_timeout     = 0;

my $guess_encoding_problems     = 1;
my $encoding_problems_threshold = 0.2;

my $max_iterations = 4;
my $max_db_size    = 100000;

my @exclude_url_rules = ();
my @include_url_rules = ('^http:\/\/.*$');
my @exclude_content_types_rules = ();
my @include_content_types_rules = ('text\/html', 'text\/plain');
my $page_max_size = 200000;
my $ignore_robot_rules = 0;

my $fetch_timeout = 3;
my $user_agent = "Mozilla/5.0 (Windows; U; Windows NT 6.0; ko; rv:1.9.1.3) Gecko/20090824 Firefox/3.5.3 (.NET CLR 3.5.30729)";
my $max_redirects = 7;
my $fetch_pause = 3;

my $use_proxy = 0;
my $proxy_url = 'http://127.0.0.1:8080';

#============================================

# Read configuration file
open (CONFIG, $config_file) or die "Unable to open configuration file: $config_file.";
eval(join ('', <CONFIG>));
close (CONFIG);

# Init RobotRules parser
my $robotsrules = new WWW::RobotRules($user_agent);

# Stores the contents of robots.txt files of any host
my %robots = ();

# Initialize the URL queue from the seeds file
my @url_queue = ();
open (SEEDS, $seeds_file) or die "Unable to open $seeds_file.";
foreach(<SEEDS>){
	chomp;
	push @url_queue, $_;
}
close SEEDS;

# Set of the already known urls
my %known_urls = map {$_ => 1} @url_queue;

# Initialize the URL queue for next step
my @next_url_queue = ();

# Value of current iteration
my $iteration = 1;
my $downloaded_pages = 0;
my $downloaded_bytes = 0;
my $created_records  = 0;
my $processed_this_iteration  = 0;

print "Content-type: text/plain; charset=s-jis\n\n";
open (DB, ">>$db_file")  or die "Unable to open $db_file.";
flock(DB,LOCK_EX);

while ($iteration <= $max_iterations){
	$processed_this_iteration  = 0;
	
	print "-------------------------------------------------------------\n";
	print "Iteration number $iteration started.\n";
	print "Number of pages to fetch: ".($#url_queue + 1)."\n";
	print "-------------------------------------------------------------\n";
	
	# loop on current queue
	foreach my $url (@url_queue){
		$processed_this_iteration ++;
		
		$url = normalize_url($url);
		
		if(should_process_url($url)){
			
			# Check for robot rules if check enabled
			if ($ignore_robot_rules || is_robot_allowed($url)){

				my $page_text = get_page($url);
				
				if ($page_text){
					$page_text = normalize_page($page_text);
					
					if ($guess_encoding_problems && has_encoding_problems($page_text)){
						print "----> Page has unsupported character encoding.\n";
						next;
					}
					

					
					# get keywords and description
					my $description = get_description($page_text);
					
					
					my $guess = Encode::Guess::guess_encoding($description);
					if (ref $guess && $guess->name ne 'shiftjis') {
  					Encode::from_to($description, $guess->name, 'shiftjis');
					}


								
					

					#Figure out the encoding 
					my $keywords    = get_keywords($page_text);
					
					my $guess1 = Encode::Guess::guess_encoding($keywords);
					if (ref $guess1 && $guess1->name ne 'shiftjis') {
  					Encode::from_to($keywords, $guess1->name, 'shiftjis');
					}

									    
				
					if ($description || $keywords) {
						
						
						print DB get_db_record($url, $keywords, $description);
						$created_records++;
						flock(DB,LOCK_UN);
						# Finish if max size reached
						if ($created_records == $max_db_size){
							print "-------------------------------------------------------------\n";
							print "Maximum database size reached ($max_db_size).\n";
							print_iteration_stats();
							print "Process completed.";
							close DB;
							exit (0);
						}
					}
					
					# If we have a next iteration then process also anchors
					if ($iteration < $max_iterations) {
						my @anchors;
						my @frames;
						my @imagemaps;
		   			(@anchors)    = $page_text =~ m/<a[^>]*href\s*=\s*["']([^"'>]*)"/gsxi;
		   			(@frames)     = $page_text =~ m/<i?frame[^>]*src\s*=\s*["']([^"'>]*)"/gsxi;
		   			(@imagemaps)  = $page_text =~ m/<area[^>]*href\s*=\s*["']([^"'>]*)"/gsxi;
						my @all_links = ();
						push @all_links, @anchors;
						push @all_links, @frames;
						push @all_links, @all_links;
						my $added_pages = 0;
						foreach my $anchor (@anchors){
			      	my $new_url = get_fully_quialified_url($url, $anchor);
			      	unless (defined $known_urls{$new_url}) {
			      		push @next_url_queue, $new_url;
			      		$known_urls{$new_url} = 1;
			      		$added_pages++;
			      	}
						}
						print "----> New pages found               : $added_pages\n";
					}
				}
				print "----> Remaining pages for iteration : ". ($#url_queue + 1 - $processed_this_iteration) ."\n\n";
				if ($fetch_pause) {sleep ($fetch_pause);}
			}
		}
	}
	@url_queue = ();
	push @url_queue, @next_url_queue;
	@next_url_queue = ();
	print_iteration_stats();
	$iteration++;
	
}

print "Process completed.";
close DB;



my $dbfile   = "spider.dat";
my $keywords = "keywords.dat";
my $urls     = "urls.dat";
my $descr    = "descriptions.dat";

open (I, "$dbfile") or die "Unable to open: $dbfile";
my @lines = <I>;
close I;

open (K, ">$keywords")   or die "Unable to open: $keywords";
open (U, ">$urls")       or die "Unable to open: $urls";
open (D, ">$descr")      or die "Unable to open: $descr";
foreach (@lines){
	chomp;
	my ($u, $k, $d) = split(/¥t/, $_, 3);
	print K "$k\n";
	print U "$u\n";
	print D "$d\n";
}
close K;
close U;
close D;

@lines = reverse(@lines);
open (K, ">rev_$keywords")   or die "Unable to open: rev_$keywords";
open (U, ">rev_$urls")       or die "Unable to open: rev_$urls";
open (D, ">rev_$descr")      or die "Unable to open: rev_$descr";
foreach (@lines){
	chomp;
	my ($u, $k, $d) = split(/¥t/, $_, 3);
	print K "$k\n";
	print U "$u\n";
	print D "$d\n";
}
close K;
close U;
close D;
#Sort Alphabetically A-Z
@lines = sort(@lines);
open (K, ">sort_$keywords")   or die "Unable to open: rev_$keywords";
open (U, ">sort_$urls")       or die "Unable to open: rev_$urls";
open (D, ">sort_$descr")      or die "Unable to open: rev_$descr";
foreach (@lines){
	chomp;
	my ($u, $k, $d) = split(/¥t/, $_, 3);
	print K "$k\n";
	print U "$u\n";
	print D "$d\n";
}
close K;
close U;
close D; 
#Sort Alphabetically Z-A
@lines = reverse sort(@lines);
open (K, ">revsort_$keywords")   or die "Unable to open: rev_$keywords";
open (U, ">revsort_$urls")       or die "Unable to open: rev_$urls";
open (D, ">revsort_$descr")      or die "Unable to open: rev_$descr";
foreach (@lines){
	chomp;
	my ($u, $k, $d) = split(/¥t/, $_, 3);
	print K "$k\n";
	print U "$u\n";
	print D "$d\n";
}
close K;
close U;
close D; 

exit (0);

##############################################
# Print last iteration statistics
##############################################
sub print_iteration_stats {
	print "-------------------------------------------------------------\n";
	print "Iteration number $iteration completed.\n";
	print "Total pages downloaded : $downloaded_pages\n";
	print "Total bytes downloaded : $downloaded_bytes\n";
	print "Total records created  : $created_records\n";
	print "-------------------------------------------------------------\n\n";
}

##############################################
# Check if a given url should be processed
# according to include/exclude rules
##############################################
sub should_process_url {
	my $url = shift;
	
	if ($domain_max_timeout){
		my ($protocol, $rest) = $url =~ m|^([^:/]*):(.*)$|;
		my ($server_host, $port, $document) = $rest =~ m|^//([^:/]*):*([0-9]*)/*([^:]*)$|;
		$domain_times{$iteration}{$server_host} ||= time();
		return 0 if (time() - $domain_times{$iteration}{$server_host} > $domain_max_timeout);
	}
	
	foreach my $exclude_rule (@exclude_url_rules){
		return 0 if ($url =~ /$exclude_rule/);
	}
	foreach my $include_rule (@include_url_rules){
		return 1 if ($url =~ /$include_rule/);
	}
	
	return 0;
}

##############################################
# Check if a given url should be processed
# according to content type
##############################################
sub check_content_type {
	my $content_type = shift;
	
	foreach my $exclude_content_types_rule (@exclude_content_types_rules){
		return 0 if ($content_type =~ /$exclude_content_types_rule/);
	}
	foreach my $include_content_types_rule (@include_content_types_rules){
		return 1 if ($content_type =~ /$include_content_types_rule/);
	}
	
	return 0;	
}

##############################################
# Check if a given url should be processed
# according to robot rules
##############################################
sub is_robot_allowed {

	my $url = shift;
	
	my ($protocol, $rest) = $url =~ m|^([^:/]*):(.*)$|;
  my ($server_host, $port, $document) = $rest =~ m|^//([^:/]*):*([0-9]*)/*([^:]*)$|;  
  if (!$port) {$port = 80;}
  
  my $key = "$server_host:$port";
  
  # If we haven't yet downloaded robot.txt from this host, donload it now
  unless (defined $robots{$key}){load_robot_rules($server_host, $port);}
  
  # Now check rules
  if ($robots{$key}){
		$robotsrules->parse($url, $robots{$key});  	
		return $robotsrules->allowed($url);
  }
  else {
  	return 1;
  }
}

##############################################
# Load robot.txt rules. Gets an internal
# representation of disallow rules that apply to us
##############################################
sub load_robot_rules {
	
	my $host = shift;
	my $port = shift;

  my $key = "$host:$port";
	
	my $ua = LWP::UserAgent->new;
	setup_ua($ua);
	
	my $url = "http://$host:$port/robots.txt";
	
	print "--> Fetching: robots.txt for: http://".$host.":".$port."...";
	my $response = $ua->get($url);
	if ($response->is_success){
		$downloaded_bytes += length($response->content);
		$robots{$key} = $response->content;
		print "OK.\n";
	}
	else {
		$robots{$key} = '';
		print "Failed, ignoring it. ".$response->status_line."\n";
	}	
}

##############################################
# Gets a page
##############################################
sub get_page {
	my $url = shift;
	
	my $ua = LWP::UserAgent->new;
	setup_ua($ua);

	print "--> Fetching: $url......";
	my $response = $ua->get($url);
		
	if ($response->is_success){
		unless(check_content_type($response->header('content-type'))){
			print "Excluded because of content type (".$response->header('content-type').") .\n";
			return "";
		};
		print "OK.\n";
		$downloaded_pages++;
		$downloaded_bytes += length($response->content);
		return $response->content;
	}
	else {
		print "Error. ".$response->status_line."\n";
		return "";
	}
}

##############################################
# Setup user agent
##############################################
sub setup_ua {
	my $ua = shift;
	$ua->agent($user_agent);
	$ua->timeout($fetch_timeout);
	$ua->max_redirect($max_redirects);
	$ua->max_size($page_max_size) if (defined $page_max_size);
	$ua->proxy('http', $proxy_url) if ($use_proxy);
}

##############################################
# Normalize page content for processing
##############################################
sub normalize_page (){
	my $page_text = shift;
	$page_text =~ s/[\r\n]/ /gsx;
	$page_text =~ s/\s+/ /gsx;
	$page_text =~ s|<!--[^>]*-->||gsx;
	return $page_text;
}

##############################################
# Normalize url. If there is no path, be sure to have a / at the end
##############################################
sub normalize_url (){
	my $url = shift;
	if ($url =~ /^http\:\/\/[^\/]+$/i){$url .= '/';}
	return $url;
}


##############################################
# Build a fully specified URL.
##############################################
sub get_fully_quialified_url {

	my ($thisURL, $anchor) = @_;
	my ($has_proto, $has_lead_slash, $currprot, $currhost, $newURL);

	# Strip anything following a number sign '#', because its
	# just a reference to a position within a page.
	$anchor =~ s|^.*#[^#]*$|$1|;

	# Examine anchor to see what parts of the URL are specified.
	$has_proto = 0;
	$has_lead_slash=0;
	$has_proto = 1 if($anchor =~ m|^[^/:]+:|);
	$has_lead_slash = 1 if ($anchor =~ m|^/|);

	if($has_proto == 1){
	   # If protocol specified, assume anchor is fully qualified.
	   $newURL = $anchor;
	}
	elsif($has_lead_slash == 1){
   # If document has a leading slash, it just needs protocol and host.
   ($currprot, $currhost) = $thisURL =~ m|^([^:/]*):/+([^:/]*)|;
   $newURL = $currprot . "://" . $currhost . $anchor;
	}
	else{
	   ($newURL) = $thisURL =~ m|^(.*)/[^/]*$|;
	   $newURL .= "/" if (! ($newURL =~ m|/$|));
	   $newURL .= $anchor;
	
	}
	return $newURL;
}

######################################################
# Get description from HTML comtent
######################################################
sub get_description {
	my $page_text = shift;
	my $description = '';
	if ($page_text =~ m/$description_regexp/gsxi){$description = $1;}

	if (length ($description) > $max_description_length){
		($description) = $description =~ /^(.{0,$max_description_length})\s/gsx;
	}
	
	return $description;
}

######################################################
# Get keywords from HTML content
######################################################
sub get_keywords {
	my $page_text = shift;
	my $keywords = '';
	if ($page_text =~ m/$keywords_regexp/gsxi){$keywords = $1;}

	if (length ($keywords) > $max_keywords_length){
		($keywords) = $keywords =~ /^(.{0,$max_keywords_length})\s/gsx;
	}

	if (!$keywords && $generate_keywords){
		$keywords = generate_keywords($page_text);
	}
	
	return $keywords;
}

######################################################
# Generate a set of keywords from html content
######################################################
sub generate_keywords {
	
	my $page_text = shift;
	my @keywords;
		
	# Remove all tags and get lower case
	$page_text = extract_text($page_text);
	$page_text = lc($page_text);
	
	# Take all words longer than 4 chars
	(@keywords) = $page_text =~ /\s([a-zA-Z0-9\-\@]{5,})\s/gsx;

	# Count word frequency
	my %tmp = ();
	foreach my $word (@keywords){
		if (defined $tmp{$word}) {$tmp{$word} += 1;}
		else {$tmp{$word} = 1;}
	}

	# Remove duplicates
	my %keyword_hash = map {$_ => 1} @keywords;
	@keywords = keys %keyword_hash;

	# Sort according to frequency
	my @out = sort { $tmp{$b} <=> $tmp{$a} } @keywords;

	my $keywords = join (', ', @out);

	if (length ($keywords) > $max_keywords_length){
		($keywords) = $keywords =~ /^(.{0,$max_keywords_length})\s/gsx;
	}
	
	return $keywords;
}

######################################################
# Generate record to be written to DB
######################################################
sub get_db_record {
	my $url         = shift;
	my $keywords    = shift;
	my $description = shift;

	my $record = $db_layout;
	$record =~ s/##KEYWORDS##/$keywords/;
	$record =~ s/##DESCRIPTION##/$description/;
	$record =~ s/##URL##/$url/;

	return $record;
}

######################################################
# Checks if a page has too many unrecognized characters
######################################################
sub has_encoding_problems {
		my $page_text = shift;
	
		$page_text = extract_text($page_text);
		$page_text = lc ($page_text);
		
		# Remove tags and whitespaces, html escape chars
		$page_text =~ s/\&.{1,5}\;//gsx;
		$page_text =~ s/\s//gsx;
		
		my $original_length = length ($page_text);
		if (!$original_length) {return 0;}
		
		# Remove all good characters
		$page_text =~ s/[a-z0-9]//gsx;
		
		my $strange_chars = length ($page_text);
		
		if ($strange_chars/$original_length > $encoding_problems_threshold){
			return 1;
		}
		else {
			return 0;
		}
}

######################################################
# Remove all tage
######################################################
sub extract_text {
	my $page_text = shift;

	$page_text =~ s/<script.*?\/script>//gsxi;
	$page_text =~ s/<style.*?\/style>//gsxi;
	$page_text =~ s/<[^>]*>//gsx;
	
	return $page_text;
}
