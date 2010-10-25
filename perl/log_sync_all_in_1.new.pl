#!/usr/bin/perl
#===========================================================================
#半小时统计
# 1：半小时日志提取 2：浪首半小时统计 3：任务半小时统计 4：频道首页半小时
#===========================================================================
BEGIN { push @INC, qw(/data1/sinastat/code/run/rss/) }
BEGIN { push @INC, qw(/data1/sinastat/code/run/lib/) }
$HALF_HOUR_YEAR=$ARGV[0];
$HALF_HOUR_MON=$ARGV[1];
$HALF_HOUR_DAY=$ARGV[2];
$HALF_HOUR_HOUR=$ARGV[3];
$PROGRAM=__FILE__;

use Data::Dumper();
use Text::Iconv;
Text::Iconv->raise_error(0);

require ('/data1/sinastat/code/conf/gateway.conf');
my $RSYNC="/usr/bin/rsync  --timeout=60 -az ";
$WAPDIR = "/data1/sinastat";
my $source='cp';
@ISDN_LOGS=();
%ALL_ISDN=();
%SUB_ISDN=();

my $oldh = select(STDOUT);
# 防止自动flush的设置
$| = 1;
select($oldh);

my @stime = (localtime(time() - 1800));
my ($min_start,$hour_start,$day_start,$mon_start,$year_start) = ($stime[1],$stime[2],$stime[3],$stime[4]+1,$stime[5]+1900);
$hour_start= '0'.$hour_start if(length($hour_start)==1);
$day_start= '0'.$day_start if(length($day_start)==1);
$mon_start= '0'.$mon_start if(length($mon_start)==1);
my $start_day = $year_start.$mon_start.$day_start;
if($min_start >=0 and $min_start <30){$min_start='00';}
if($min_start >=30 and $min_start <=59){$min_start='30';}
my $start_hour = $hour_start . ':' . $min_start;
$HALF_HOUR_YEAR=$year_start if( $HALF_HOUR_YEAR eq '' );
$HALF_HOUR_MON=$mon_start if( $HALF_HOUR_MON eq '' );
$HALF_HOUR_DAY=$day_start if( $HALF_HOUR_DAY eq '' );
$HALF_HOUR_HOUR=$start_hour if( $HALF_HOUR_HOUR eq '' );

$GDATE="${HALF_HOUR_YEAR}${HALF_HOUR_MON}${HALF_HOUR_DAY}";
require ('global.pl');
my $begintime=getTimeInS();
message ("PROGRAM log_sync.pl BEGIN ====================");

print "$start_hour,$day_start,$mon_start,$year_start\n";
require ('logfile.conf');
require ('shortdomain.pl');
my $converter = Text::Iconv->new("UTF-8", "GBK");

my $begintime=getToday('yyyy-mm-dd hh:mm:ss');

my %CONFIG_LOGFILE = ();
my %RSYNC_CONFIG_LOGFILE = ();

#读配置数据库
{
	my $date = $GDATE . $hour_start . $min_start;
	my $LOG_PATH = '/data1/sinastat/var/isdn/TIME/';
	my $TEMP_PATH = '/data1/sinastat/var/isdn/TEMP/';
	my $db_PROG = new db('PROG_SLAVE');
	my $conn_PROG = $db_PROG->{'conn'}; #寻找属性
	my ($stmt,$sql);
	$sql = "select channel,path,ip,date from rsync_isdn_status where date = '$date' and length(date)=12 and att = 2 and flag = 1";
	$stmt=$conn_PROG->prepare($sql);
	$stmt->execute();
	while (my ($channel,$path,$ip,$date)=$stmt->fetchrow_array)	
	{
			$path = $LOG_PATH . $path;
#			print "$channel,$path,$ip,$date\n";
			$CONFIG_LOGFILE{$channel}{'hourFiles'}{$ip}=$path;
	}
	foreach my $channel (keys %LOGFILE_CONFIG_COOKIE)
	{
		next if(! defined($LOGFILE_CONFIG_COOKIE{$channel}{'halfhourFiles'}));
		next if(! defined($LOGFILE_CONFIG_COOKIE{$channel}{'halfremoteFiles'}));
		foreach my $ip (keys %{$LOGFILE_CONFIG_COOKIE{$channel}{'halfhourFiles'}})
		{
			my $path = $LOGFILE_CONFIG_COOKIE{$channel}{'halfhourFiles'}{$ip};
			$CONFIG_LOGFILE{$channel}{'hourFiles'}{$ip}=$path;
			$RSYNC_CONFIG_LOGFILE{$channel}{'hourFiles'}{$ip}=$path;
		}
		foreach my $ip (keys %{$LOGFILE_CONFIG_COOKIE{$channel}{'halfremoteFiles'}})
		{
			my $path = $LOGFILE_CONFIG_COOKIE{$channel}{'halfremoteFiles'}{$ip};
			$RSYNC_CONFIG_LOGFILE{$channel}{'remoteFiles'}{$ip}=$path;
		}
	}
		print Dumper(\%CONFIG_LOGFILE);
		print Dumper(\%RSYNC_CONFIG_LOGFILE);

}


message("获取日志");
#rsyncLog() if($HALF_HOUR_HOUR eq '');#带参数重跑时不重新取日志
rsyncLog();#带参数重跑时不重新取日志

message("过滤网管读取日志");
#昌平F5ip都先认为合法
$GATEWAY{'221.179.217.230'}=1;
$GATEWAY{'221.179.217.231'}=1;
$GATEWAY{'221.179.217.20'}=1;
$GATEWAY{'221.179.217.21'}=1;


foreach my $domain_name (keys %CONFIG_LOGFILE)
{
	my $hourFiles_ref=$CONFIG_LOGFILE{$domain_name}{'hourFiles'};
	my @line=();
	foreach my $hourFile (values %$hourFiles_ref)
	{
		if(-e $hourFile)
		{
			open FILE , "<$hourFile" ;
      @subdomains = qw(wapcms dpool book_pibao_3g_sina_com_cn nba.prog.3g.sina.com.cn nba2.prog.3g.sina.com.cn sinatv.sina.com.cn book.prog.3g.sina.com.cn stock.prog.sina.com.cn)
      $pubsysb = 0;
      foreach my $pubsys(@subdomains){
        if($LOGFILE_CONFIG_COOKIE{$domain_name}{'pubsys'} eq $pubsys){
          $pubsys = 1;
          last;
        }
      }
      while(<FILE>){
        my ($time,$mobile,$request,$ua,$ip)={};
        if($pubsys eq 1){
					($time,$mobile,$request,$ua,$ip) = parseIsdnCookie($_);
        }else{
          ($time,$mobile,$request,$ua,$ip) = parseIsdn($_)
        }
        #这里如果可以每次都不用编译就好了。
        $ip =~ s/[\[\]]//g;
        $request =~ s/\s//g;
        if($GATEWAY{$ip}==1)
        {
          push @line,[$request,$mobile];	
        }
      }
			close FILE;
		}
		else
		{
			print "$HALF_HOUR_DAY $HALF_HOUR_HOUR cannot find $hourFile\n";
			my $cmd = "echo '$HALF_HOUR_DAY $HALF_HOUR_HOUR cannot find $hourFile' >> /tmp/index_err ";
			system($cmd);
		}
	}
	$ALL_ISDN{$domain_name}=\@line;
}

=pod
将需要ALL_ISDN的几个部分，结合起来做
包括：
创建排行榜日志
创建读书产品排行榜日志
创建SUB_ISDN
任务点击量分析
=cut

$PERL_PATH="/usr/local/sinawap/perl/bin/perl";
%CONFIGS = {};

message("创建排行榜日志");
foreach my $domain_name(keys %ALL_ISDN)
{
	next if($LOGFILE_CONFIG_COOKIE{$domain_name}{'pubsys'} ne 'wapcms');
	my $file = "/tmp/${domain_name}_${HALF_HOUR_YEAR}_${HALF_HOUR_MON}_${HALF_HOUR_DAY}_${HALF_HOUR_HOUR}";
	open(F,"+>$file") or warn $!;	
	foreach my $line (@{$ALL_ISDN{$domain_name}})
	{
		print F "@$line\n";	
	}
	close(F) or warn $!;sleep 1;
	system("/usr/local/sinawap/perl/bin/perl /data1/sinastat/code/run/rss/news_rank.pl ${domain_name} ${HALF_HOUR_YEAR} ${HALF_HOUR_MON} ${HALF_HOUR_DAY} ${HALF_HOUR_HOUR} >> /tmp/news_rank.log 2>&1 &");
}

message("创建读书产品排行榜日志");
#foreach my $domain_name(keys %ALL_ISDN)
{
	my $domain_name = 'book.prog.3g.sina.com.cn';
	my $file = "/tmp/${domain_name}_${HALF_HOUR_YEAR}_${HALF_HOUR_MON}_${HALF_HOUR_DAY}_${HALF_HOUR_HOUR}";
	open(F,"+>$file") or warn $!;	
	foreach my $line (@{$ALL_ISDN{$domain_name}})
	{
		print F "@$line\n";	
	}
	close(F) or warn $!;sleep 1;
	system("/usr/local/sinawap/perl/bin/perl /data1/sinastat/code/run/rss/book_rank.pl ${domain_name} ${HALF_HOUR_YEAR} ${HALF_HOUR_MON} ${HALF_HOUR_DAY} ${HALF_HOUR_HOUR} >> /tmp/book_rank.log 2>&1 &");
}










