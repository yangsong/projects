#use strict;
#use Benchmark;
#
#my $LOG_FILE = '/var/log/messages';
## 下面qr部分起了关键作用，预编译了表达式
#my @EXT_LIST = map {qr/$_/} qw{
#ACPI
#};
#
#my $startime = new Benchmark;
#my %result;
#map {$result{$_} = 0} @EXT_LIST;
#open LOG_FILE, $LOG_FILE;
#while (<LOG_FILE>){ 
#  foreach my $ext (@EXT_LIST) 
#  { $result{$ext}++ if $_ =~ /$ext/; }
#}
#close LOG_FILE;
#
#while (my ($key, $value) = each(%result)){ 
#  $key =~ s/\(\?-xism:(.*?)\)/$1/g;
#  print "$key:\t$value\n"; 
#}
#
#printf "** %s\n\n", timestr(timediff(new Benchmark, $startime));
#
#
#
@subdomains = qw(wapcms dpool book_pibao_3g_sina_com_cn nba.prog.3g.sina.com.cn nba2.prog.3g.sina.com.cn sinatv.sina.com.cn book.prog.3g.sina.com.cn stock.prog.sina.com.cn );
foreach my $domain(@subdomains){
  if($domain eq "dpool"){
    last;
  }
  print $domain . "\n";
}

my $m = 2;


my ($id, $name) = qw//;
if($m eq 1){
  ($id, $name) = (1, 'alvayang');
}else{
  ($id, $name) = (2, 'netyang');
}
print "\nID:$id, Name:$name\n";


@fetchrow_array = (('index.3g.sina.com.cn', 'index.3g.sina.com.cn/', '221.179.175.208', 123));
foreach ($domain, $path, $ip, $xx) = @fetchrow_array){
  print $domain . $path . $ip . $xx;
}
#foreach my $date(@fetchrow_array){
#  ($domain, $path, $ip, $xx) = $date;
#  print $domain . $path . $ip . $xx;
#}
=pod
while (my ($channel,$path,$ip,$date) = (@fetchrow_array)){
  print $channel . $path . $ip . $date;
}
=cut
