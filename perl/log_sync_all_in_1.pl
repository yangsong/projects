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
#require ('timelib.pl');
require ('/data1/sinastat/code/conf/gateway.conf');
#by debao 2005-07-13 11:42
#my $RSYNC="/usr/bin/rsync -a ";
my $RSYNC="/usr/bin/rsync  --timeout=60 -az ";
$WAPDIR = "/data1/sinastat";
my $source='cp';
@ISDN_LOGS=();
%ALL_ISDN=();
%SUB_ISDN=();

my $oldh = select(STDOUT);
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
			#if($LOGFILE_CONFIG_COOKIE{$domain_name}{'pubsys'} eq 'wapcms' || $LOGFILE_CONFIG_COOKIE{$domain_name}{'pubsys'} eq 'dpool' 
			if($LOGFILE_CONFIG_COOKIE{$domain_name}{'pubsys'} eq 'wapcms' || $LOGFILE_CONFIG_COOKIE{$domain_name}{'pubsys'} eq 'dpool' || $domain_name eq 'book_pibao_3g_sina_com_cn' || $domain_name eq 'nba.prog.3g.sina.com.cn' || $domain_name eq 'nba2.prog.3g.sina.com.cn' || $domain_name eq 'sinatv.sina.com.cn' || $domain_name eq 'book.prog.3g.sina.com.cn' || $domain_name eq 'stock.prog.3g.sina.com.cn' || $domain_name eq 'wapsite.3g.sina.com.cn')
			{
				while(<FILE>)
				{
					my ($time,$mobile,$request,$ua,$ip)=parseIsdnCookie($_);
					$ip =~ s/[\[\]]//g;
					$request =~ s/\s//g;
					if($GATEWAY{$ip}==1)
					{
						push @line,[$request,$mobile];	
					}
				}
			}
			else
			{
				while(<FILE>)
				{
					my ($time,$mobile,$request,$ua,$ip)=parseIsdn($_);
					$ip =~ s/[\[\]]//g;
					$request =~ s/\s//g;
					if($GATEWAY{$ip}==1)
					{
						push @line,[$request,$mobile];	
					}
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

message("精简日志");
foreach my $domain_name (keys %CONFIG_LOGFILE)
{
	foreach my $r_arr_p (@{$ALL_ISDN{$domain_name}})
	{
		my @r_arr = @$r_arr_p;
		my $r = $r_arr[0];
		if($r =~ m/[\?&]pos\=/)
		{
			$r =~s/\s//g;
			$r =~s/[\?&]vt=[^&]*//g;
			$r =~s/[\?&]et=[^&]*//g;
			$r =~s/[\?&]mode=[^&]*//g;
#			$r =~s/[\?&]et=\d*//g;
			$r =~s/[\?&]gsid=[^&]*//g;
			$r =~s/[\?&]from=gd[^&]*//g;
			$r =~s/[\?&]wm=[^&]*//g;

#正文下推荐替换
			$r =~s/pos=text_old/pos=6/;

			$r =~s/[\?&]MISC_ID=[^&]*//g;
			$r =~s/&MISC_SessionID=[^&]*//g;
			$r =~s/&MISC_AccessMode=[^&]*//g;
			$r =~s/&MISC_ServiceID=[^&]*//g;
			$r =~s/&PHPSESSID=[^&]*//g;
			$r =~s/\/&/\/\?/g;
			#短域名加前缀 by lixiang8 20100514
			$r = $LONG_DOMAIN_TO_SHORT{$domain_name}.$r if(defined $LONG_DOMAIN_TO_SHORT{$domain_name} && $r !~ /^\/3g*/);
			$SUB_ISDN{$r}++;
		}
	}
}

print Dumper(\%SUB_ISDN);

message("首页点击量分析");
{
	my %url_num=();

#浪首固定链接配置
	my $addr_pos=requestURL("http://wapcms.pub.sina.com.cn/ms/wap_home_page/block_view.php");
#	$addr_pos=$converter->convert($addr_pos);print $addr_pos;
	print "固定连接配置\n";
	print $addr_pos;
	print "固定连接配置\n";
	foreach my $line (split(/\n/,$addr_pos))
	{
#    $line =~m|<a href=\"(.*)?\">(.*)?</a>|;
		my @line_arr = split(/,/,$line);
    my $url = $line_arr[3];
    my $name = $line_arr[2];
    		$url =~ s/\r//g;
		next if($url !~ /^http\:/);
		$url =~ s/amp\;//g;
		$url =~ s/(http\:\/\/([^\/]*))//g;
		my $surl = $1;
		my $sdomain = $2;
		if(defined $SHORT_DOMAIN_TO_LONG{$sdomain} && $url !~ /^\/3g*/)
		{
			$url = $sdomain.$url;
			$surl = "http://";
		}
		$url =~s/[\?&]vt=[^&]*//g;
		$url =~s/[\?&]et=[^&]*//g;
		$url =~s/[\?&]mode=[^&]*//g;
		$url =~s/[\?&]pos=[^&]*//g;
		$url =~s/\/&/\/\?/g;
		my $url2 = $url;
		my $url3 = $url;
		my $url4 = $url;
		#pos=1简版，100彩板，101彩板,200广东浪首
		if($url =~ m/\/[^\?\&\/]*$/){$url .='?pos=1';$url2 .='?pos=100';$url3 .='?pos=101';$url4 .='?pos=200';}
		else{$url .='&pos=1';$url2 .='&pos=100';$url3 .='&pos=101';$url4 .='&pos=200';}
		print "[$name-$url]\n";
		$url_num{$url}{'t'} = $name;
		$url_num{$url}{'n'} = $SUB_ISDN{$url};
		$url_num{$url}{'u'} = $surl;
		$url_num{$url2}{'t'} = $name;
		$url_num{$url2}{'n'} = $SUB_ISDN{$url2};
		$url_num{$url2}{'u'} = $surl;
		$url_num{$url3}{'t'} = $name;
		$url_num{$url3}{'n'} = $SUB_ISDN{$url3};
		$url_num{$url3}{'u'} = $surl;
		$url_num{$url4}{'t'} = $name;
		$url_num{$url4}{'n'} = $SUB_ISDN{$url4};
		$url_num{$url4}{'u'} = $surl;
		$URL_TITLE{$url4}{'t'} = $name;
		$URL_TITLE{$url4}{'n'} = $SUB_ISDN{$url4};
		$URL_TITLE{$url4}{'c'} = '广东浪首';
		$URL_TITLE{$url4}{'u'} = $surl;
	}	

	#获取浪首地址
#	my $addr=requestURL("http://218.206.86.158/portal/custom_history.conf");
	my $addr=requestURL("http://71.wapcms.pub.sina.com.cn/ms/wap_home_page/custom_check.php");
	foreach my $line (split(/\n/,$addr))
	{
		my (undef,undef,$name,$url) = split(/,/,$line);
		$url =~ s/amp\;//g;
		$url =~ s/\r//g;
		$url =~ s/(http\:\/\/([^\/]*))//g;
		my $surl = $1;
		my $sdomain = $2;
		if(defined $SHORT_DOMAIN_TO_LONG{$sdomain} && $url !~ /^\/3g*/)
		{
			$url = $sdomain.$url;
			$surl = "http://";
		}
		my $url2 = $url;
		my $url3 = $url;
		my $url4 = $url;
		my $url5 = $url;
		#pos=1简版，100彩板，101彩板,200广东浪首
		if($url =~ m/\/[^\?\&\/]*$/){$url .='?pos=1';$url2 .='?pos=3';$url3 .='?pos=100';$url4 .='?pos=101';$url5 .='?pos=200';}
		else{$url .='&pos=1';$url2 .='&pos=3';$url3 .='&pos=100';$url4 .='&pos=101';$url5 .='&pos=200';}
		$url_num{$url}{'t'} = $name;
		$url_num{$url}{'n'} = $SUB_ISDN{$url};
		$url_num{$url}{'u'} = $surl;
		$url_num{$url2}{'t'} = $name;
		$url_num{$url2}{'n'} = $SUB_ISDN{$url2};
		$url_num{$url2}{'u'} = $surl;
		$url_num{$url3}{'t'} = $name;
		$url_num{$url3}{'n'} = $SUB_ISDN{$url3};
		$url_num{$url3}{'u'} = $surl;
		$url_num{$url4}{'t'} = $name;
		$url_num{$url4}{'n'} = $SUB_ISDN{$url4};
		$url_num{$url4}{'u'} = $surl;
		$url_num{$url5}{'t'} = $name;
		$url_num{$url5}{'n'} = $SUB_ISDN{$url5};
		$url_num{$url5}{'u'} = $surl;
		$URL_TITLE{$url5}{'t'} = $name;
		$URL_TITLE{$url5}{'n'} = $SUB_ISDN{$url5};
		$URL_TITLE{$url5}{'c'} = '广东浪首';
		$URL_TITLE{$url5}{'u'} = $surl;
	}


	#推荐位等非浪首配置文档3
	my $addr_pos=requestURL("http://wapcms.pub.sina.com.cn/proc/channel_recommend/check_all.php");
#	print "000000$addr_pos\n";
#	$addr_pos=$converter->convert($addr_pos);
#	print "000$addr_pos\n";
	foreach my $line (split(/\<br\>/,$addr_pos))
	{
    $line =~m|<a href=\"(.*)?\">(.*)?</a>|;
    my $url = $1;
    my $name = $2;
		$url =~ s/amp\;//g;
		$url =~ s/\r//g;
		$url =~ s/(http\:\/\/([^\/]*))//g;
		my $surl = $1;
		my $sdomain = $2;
		if(defined $SHORT_DOMAIN_TO_LONG{$sdomain} && $url !~ /^\/3g*/)
		{
			$url = $sdomain.$url;
			$surl = "http://";
		}
		if($url =~ m/\/[^\?\&\/]*$/){$url .='?pos=6';}
		else{$url .='&pos=6';}
		$url_num{$url}{'t'} = $name;
		$url_num{$url}{'n'} = $SUB_ISDN{$url};
		$url_num{$url}{'u'} = $surl;
	}	
	

#	print Dumper(\%url_num);
	
	message("浪首入库处理");
	my $db = new db('wap_master');
	my $conn = $db->{'conn'};
	my ($sql,$stmt);
	my $tenDaysAgo = dateConv($GDATE,-31,'yyyy-mm-dd');
	my $visit_day = $HALF_HOUR_YEAR.'-'.$HALF_HOUR_MON.'-'.$HALF_HOUR_DAY;
	my $visit_hour = $HALF_HOUR_HOUR.':00';
	$conn->do("set names 'latin1'");
	$conn->do("delete from SINA3G_INDEXPAGE_TOP_URLS_HOUR_STAT where visit_day = '".$tenDaysAgo."' and source='$source'");
	print "delete1\n";
	$conn->do("delete from SINA3G_INDEXPAGE_TOP_URLS_HOUR_STAT where visit_day = '$visit_day' and visit_hour='$visit_hour' and source='$source'");
	print "delete2\n";
	$sql="insert into SINA3G_INDEXPAGE_TOP_URLS_HOUR_STAT 
					set url=? , 
						title=?,
						pv=? ,
						visit_day='$visit_day',
						visit_hour='$visit_hour',
						source='$source'
						";
	$stmt=$conn->prepare($sql);
	foreach my $url (keys %url_num)
	{
		my $title = $url_num{$url}{'t'};
		my $pv = $url_num{$url}{'n'};
		$url = $url_num{$url}{'u'}.$url;
		print "$url,$title,$pv\n";
		next if($pv eq undef);
		next if($title eq undef);
#		my $pv = ($url_num{$url}{'n'} eq undef) ? 0 : $url_num{$url}{'n'};
		#$url = 'http://3g.sina.com.cn'.$url;
		$stmt->execute($url,$title,$pv) or message("insert failed:$sql".$DBI::errstr);
	}
	
	my $sql_complete = "replace RSS_HALF_HOUR_PROG_STATUS set time = '$visit_day $visit_hour',table_name = 'SINA3G_INDEXPAGE_TOP_URLS_HOUR_STAT',ip = '221.179.175.133';";
	$conn->do($sql_complete);

	$conn->disconnect;
	
}


message("频道首页点击量分析");
{
#需要统计的域名和其首页的对应关系国内，国际，社会，体育、娱乐、财经、两性、读书、博客
my %DOMAINS = (

    '国内' => {
        'domain' => 'news.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/news/index.php?tid=141&did=48&vid=68&cid=785&sid=0', #首页连接,
        'display' => '1',
    },
    '国际' => {
        'domain' => 'news.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/news/index.php?tid=141&did=49&vid=68&cid=786&sid=0', #首页连接,
        'display' => '1',
    },
    '社会' => {
        'domain' => 'news.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/news/index.php?tid=141&did=50&vid=68&cid=787&sid=0', #首页连接,
        'display' => '1',
    },
    '法制' => {
        'domain' => 'news.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/news/index.php?tid=141&did=51&vid=68&cid=788&sid=0', #首页连接,
        'display' => '1',
    },
    '健康' => {
        'domain' => 'news.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/news/index.php?tid=141&did=53&vid=68&cid=846&sid=0', #首页连接,
        'display' => '1',
    },
    '奥运' => {
        'domain' => '2008.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/2008/', #首页连接,
        'display' => '1',
    },
    '神七' => {
        'domain' => 'news.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/news/index.php?did=519&tid=122&vid=69', #首页连接,
        'display' => '1',
    },
#    '欧洲杯' => {
#        'domain' => 'sports.3g.sina.com.cn',  #域名，在配置文件里的域名key
#        'index_url' => '/3g/sports/index.php?did=12', #首页连接,
#        'display' => '1',
#    },
    '教育' => {
        'domain' => 'edu.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/edu/', #首页连接,
        'display' => '1',
    },
    '体育' => {
        'domain' => 'sports.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/sports/', #首页连接,
        'display' => '1',
    },
    '娱乐' => {
        'domain' => 'ent.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/ent/', #首页连接,
        'display' => '1'
     },
    '娱乐滚动' => {
        'domain' => 'pro.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/pro/index.php?tid=240&did=20&vid=1536&ch=ent', #首页连接,
        'display' => '1',
    },
    '科技滚动' => {
        'domain' => 'pro.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/pro/index.php?tid=240&did=20&vid=1536&ch=tech', #首页连接,
        'display' => '1',
    },
    '财经滚动' => {
        'domain' => 'pro.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/pro/index.php?tid=240&did=20&vid=1536&ch=finance', #首页连接,
        'display' => '1',
    },
    '国内滚动' => {
        'domain' => 'pro.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/pro/index.php?tid=240&did=20&vid=1536&ch=gn', #首页连接,
        'display' => '1',
    },
    '国际滚动' => {
        'domain' => 'pro.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/pro/index.php?tid=240&did=20&vid=1536&ch=gj', #首页连接,
        'display' => '1',
    },
    '社会滚动' => {
        'domain' => 'pro.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/pro/index.php?tid=240&did=20&vid=1536&ch=sh', #首页连接,
        'display' => '1',
    },
    '博客滚动' => {
        'domain' => 'pro.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/pro/index.php?tid=240&did=20&vid=1536&ch=blog', #首页连接,
        'display' => '1',
    },
    '财经' => {
        'domain' => 'finance.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/finance/', #首页连接,
        'display' => '1',
    },
        
    '读书' => {
        'domain' => 'book.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/book/', #首页连接,
        'display' => '1',
    },
    '游戏' => {
        'domain' => 'game.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/game/', #首页连接,
        'display' => '1',
    },
    '博客' => {
        'domain' => 'blog.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/blog/', #首页连接,
        'display' => '1',
    },

    '军事' => {
        'domain' => 'mil.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/mil/', #首页连接,
        'display' => '1',
    },
    
    '汽车' => {
        'domain' => 'auto.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/auto/', #首页连接,
        'display' => '1',
    },    

    '房产' => {
        'domain' => 'house.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/house/', #首页连接,
        'display' => '1',
    },

    '手机' => {
        'domain' => 'mobile.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/mobile/', #首页连接,
        'display' => '1',
    },    

    '笑话' => {
        'domain' => 'joke.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/joke/', #首页连接,
        'display' => '1',
    },    

    '影院' => {
        'domain' => 'tv.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/tv/', #首页连接,
        'display' => '1',
    },    

    '直播' => {
        'domain' => 'live.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/live/', #首页连接,
        'display' => '1',
    },    

    '论坛' => {
        'domain' => 'bbs.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/bbs/', #首页连接,
        'display' => '1',
    },    

    '女性' => {
        'domain' => 'eladies.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/eladies/', #首页连接,
        'display' => '1',
    },    

    '社区' => {
        'domain' => 'community.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/community/prog/home/home.php', #首页连接,
        'display' => '1',
    },    

    '英超' => {
        'domain' => '',  #域名，在配置文件里的域名key
        'index_url' => '/3g/sports/?&tid=31&did=30&vid=17&cid=652', #首页连接,
        'display' => '1',
    },    

    'NBA' => {
        'domain' => '',  #域名，在配置文件里的域名key
        'index_url' => '/3g/sports/index.php?tid=12&did=45&vid=18', #首页连接,
        'display' => '1',
    },    

    '图片' => {
        'domain' => '',  #域名，在配置文件里的域名key
        'index_url' => '/common/dlf/channel.php?mid=46&bid=5000310', #首页连接,
        'display' => '1',
    },    

    '铃声' => {
        'domain' => '',  #域名，在配置文件里的域名key
        'index_url' => '/common/dlf/channel.php?mid=46&bid=5000305', #首页连接,
        'display' => '1',
    },    

    '下载中心' => {
        'domain' => '',  #域名，在配置文件里的域名key
        'index_url' => '/prog/wapsite/fd/index.php', #首页连接,
        'display' => '1',
    },    

    '评论' => {
        'domain' => '',  #域名，在配置文件里的域名key
        'index_url' => '/3g/news/index.php?tid=141&did=52&vid=68&cid=789&sid=0', #首页连接,
        'display' => '1',
    },    
    
    #不参与首页统计，但因为其他频道会用到这个日志
    '应用前端1' => {
        'domain' => 'wapsite.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'display' => '0',
    },

    #不参与首页统计，但因为其他频道会用到这个日志
    '应用前端2' => {
        'domain' => 'nba.prog.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'display' => '0',
    },

    #不参与首页统计，但因为其他频道会用到这个日志
    '应用前端3' => {
        'domain' => 'stock.prog.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'display' => '0',
    },
    
    #不参与首页统计，但因为其他频道会用到这个日志
    '应用前端4' => {
        'domain' => 'book.prog.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'display' => '0',
    },    
    

    #程序日志，不参与频道首页
    'WAPCMS程序' => {
        'domain' => 'pro.3g.sina.com.cn',
        'display' => '0',
    },

   #####梦网业务
       'wapdl2' => {
        'domain' => 'wapdl2.sina.com.cn',
        'display' => '0',
		},
		
    '色图' => {
        'domain' => 'sexpic.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/sexpic/', #首页连接,
        'display' => '1',
    },
    
    '两性' => {
        'domain' => 'sex.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/sex/', #首页连接,
        'display' => '1',
    },

    '星座' => {
        'domain' => 'ast.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/ast/', #首页连接,
        'display' => '1',
    },
    '科技' => {
            'domain' => 'tech.3g.sina.com.cn',  #域名，在配置文件里的域名key
            'index_url' => '/3g/tech/', #首页连接,
            'display' => '1',
    },
    
    '彩票' => {
            'domain' => 'lotto.3g.sina.com.cn',  #域名，在配置文件里的域名key
            'index_url' => '/3g/lotto/', #首页连接,
            'display' => '1',
    },  
    '奇闻' => {
            'domain' => 'qiwen.3g.sina.com.cn',  #域名，在配置文件里的域名key
            'index_url' => '/3g/qiwen/', #首页连接,
            'display' => '1',
    },        
    '动态池' => {
        'domain' => 'dpool.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => 'http://3g.sina.com.cn/dpool/', #首页连接,
        'display' => '0',
    },     
    '股票' => {
            'domain' => '3g.sina.com.cn',  #域名，在配置文件里的域名key  #因为股票没有域名是在前端单独算
            'index_url' => '/3g/finance/index.php?tid=60&did=13&vid=512', #首页连接,
            'display' => '1',
    },
    
#    '股票' => {
#            'domain' => '3g.sina.com.cn',  #域名，在配置文件里的域名key  #因为股票没有域名是在前端单独算
#            'index_url' => '/prog/wapsite/stock/index.php', #首页连接,
#            'display' => '1',
#    },

		'视频' => {
            'domain' => 'video.3g.sina.com.cn',  #域名，在配置文件里的域名key
            'index_url' => '/3g/video/', #首页连接,
            'display' => '1',
    },
    
    ###精品统计###已经再加精品统计需要key中包含精品字样,在后面的程序中会特殊处理
    '精品女性' => {
        'domain' => 'lady.jp.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/lady/', #首页连接,
        'display' => '1',
    }, 
    '精品日志' => {
        'domain' => 'prog.jp.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => 'http://prog.jp.3g.sina.com.cn', #首页连接,
        'display' => '0',
    },       
   ###精品统计
   
       '视频排行用' => {
        'domain' => 'site.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '/3g/site/', #首页连接,
        'display' => '0',
    },       
    '炫魅广东首页' => {
        'domain' => 'index.3g.sina.com.cn',  #域名，在配置文件里的域名key
        'index_url' => '', #首页连接,
        'display' => '1',
    },
    '世博' => {
            'domain' => 'expo.3g.sina.com.cn',  #域名，在配置文件里的域名key
            'index_url' => '/3g/expo/', #首页连接,
            'display' => '1',
    },  
    '世界杯' => {
            'domain' => '2010.3g.sina.com.cn',  #域名，在配置文件里的域名key
            'index_url' => '/3g/2010/', #首页连接,
            'display' => '1',
    },  
    '文史' => {
            'domain' => 'cul.3g.sina.com.cn',  #域名，在配置文件里的域名key
            'index_url' => '/3g/cul/', #首页连接,
            'display' => '1',
    },  
    '广东读书' => {
            'domain' => 'book.3g.sina.com.cn',  #域名，在配置文件里的域名key
            'index_url' => '/3g/book/?vid=2565', #首页连接,
            'display' => '1',
    },  
    '旅游' => {
            'domain' => 'travel.3g.sina.com.cn',  #域名，在配置文件里的域名key
            'index_url' => '/3g/travel/', #首页连接,
            'display' => '1',
    },
    '草根博客' => {
            'domain' => '3g.sina.com.cn',  #域名，在配置文件里的域名key
            'index_url' => '/3g/blog/?sa=t331d119v2623', #首页连接,
            'display' => '1',
    },
    '漫画' => {
            'domain' => 'comic.3g.sina.com.cn',  #域名，在配置文件里的域名key
            'index_url' => '/3g/comic/', #首页连接,
            'display' => '1',
    },
);



my %pos2domains = (

'8' => '国内',	
'9' => '国际',	
'10' => '体育',	
'11' => '社会',	
'12' => '娱乐',	
'13' => '两性',	
'14' => '色图',	
'15' => '读书',	
'16' => '博客',	
'17' => '财经',	
'18' => '科技',	
'19' => '星座',	
'20' => '精品女性',	
'21' => '股票',	
'22' => '游戏',	
'23' => '视频',	
'24' => '军事',	

'25' => '汽车',
'26' => '房产',
'27' => '手机',
'28' => '笑话',
'29' => '影院',
'30' => '直播',
'31' => '论坛',
'32' => '女性',
'33' => '社区',

'34' => '英超',
'35' => '图片',
'36' => '铃声',
'37' => '下载中心',

'38' => '评论',
'39' => '法制',
'40' => 'NBA',
#'41' => '相关新闻呈现样式'
'42' => '健康',
#'43' => '正文页内链接'
#'44' => '爱问-》张璘'
'45' => '教育',
'46' => '欧洲杯',
'47' => '奥运',
'48' => '神七',
#'100' => '浪首娱版',
#'100' => '改为浪首彩板',2009年5月22日9:58:51
#'101' => '浪首3G版',
'49' => '炫魅广东首页',
#'50' => '科技3G首页',
'54' => '世博',
'55' => '世界杯',
'56' => '文史',
'57' => '广东读书',
'58' => '旅游',
'59' => '草根博客',
'60' => '漫画',
'200' => '广东浪首',
);	

#my $channel = '视频';
foreach my $channel (keys %DOMAINS) 
{
		#if ($channel ne '股票') {next;}
    my $domain= $DOMAINS{$channel}{'domain'};	
    my $index_url= $DOMAINS{$channel}{'index_url'};
    next if(!defined($index_url) or $index_url eq '');
    my $display= $DOMAINS{$channel}{'display'};	
    if ($display) {
        $INDEX_RUL{$index_url} = $channel;
    }
	my $display 		= $DOMAINS{$channel}{'display'};
	my $hourFile_ref = $CONFIG_LOGFILE{$domain}{'hourFiles'};
    print "$channel|$domain|url:$index_url|$hourFile_ref|\n";
	foreach my $hourFile (values %$hourFile_ref)
	{
        if(-e $hourFile)
				 {$hour_files_hash{$hourFile} = 1;}
	}
	my $url = "http://3g.sina.com.cn$index_url";
    if ($channel=~m/精品/) {
    	$url = "http://jp.3g.sina.com.cn$index_url";
    }
    $content=requestURL("$url");
    print "首页 $url\n";

	$content=$converter->convert($content);
	if (($channel ne '股票') && ($channel ne '社区')) {
    while($content=~s/href=["'](.*?)["']\>(.*?)<\/a\>/href/m) {
        my $title = $2;
        my $url = $1;
        $url =~ s/\r//g;
        $url=~s|\&amp;|\&|g;
        $url=~s|http\:\/\/(([^\/]*))||;
		my $surl = $1;
		my $sdomain = $2;
		if(defined $SHORT_DOMAIN_TO_LONG{$sdomain} && $url !~ /^\/3g*/)
		{
			$url = $sdomain.$url;
			$surl = "";
		}
		$url=~s|[\?\&]PHPSESSID=[^&]*||;
        $url=~s|[\?\&]vt=[^&]*||;
        $url=~s|^\?|$index_url\?|;
        $index_url =~m|(/3g/.+?/)|;
		my $dir_url = $1;
		if($url =~ m|/3g/site/|){
			$url =~ m|(/3g/site/.+pos=\d+)|;
        	$dir_url = $1;
        }
				$url=~s|(^index\.php\?.*)|$dir_url$1|;        
#
				$url=~s|^\.\/(index\.php\?.*)|$dir_url$1|;        

#				$url = $dir_url;

        $url=~s/.*SPURL\=http:\/\///gi;
        my $num = $SUB_ISDN{$url};
        $URL_TITLE{$url}{'t'} = $title;
        $URL_TITLE{$url}{'n'} = $num;
        $URL_TITLE{$url}{'c'} = $channel;
		$URL_TITLE{$url}{'u'} = $surl;
        print "|$title|$url|[$num]\n\n";

    }
   }
   elsif ($channel eq '股票'){
    while($content=~s/href=["'](.*?)["']\>(.*?)<\/a\>/href/m) {
        my $title = $2;
        my $url = $1;
        $url =~ s/\r//g;
        $url=~s|\&amp;|\&|g;
        $url=~s|http\:\/\/(([^\/]*))||;
		my $surl = $1;
		my $sdomain = $2;
		if(defined $SHORT_DOMAIN_TO_LONG{$sdomain} && $url !~ /^\/3g*/)
		{
			$url = $sdomain.$url;
			$surl = "";
		}
		$url=~s|[\?\&]PHPSESSID=[^&]*||;
        $url=~s|[\?\&]vt=[^&]*||;
        $url=~s|^\?|$index_url\?|;
        $url=~s/.*SPURL\=http:\/\///gi;
        if ($url!~m|\/|) {
        	$url = "/prog/wapsite/stock/$url";
        }
#        elsif ($url!~m|3g.sina.com.cn|) {
#        	$url = "3g.sina.com.cn$url";
#        }
        my $num = $SUB_ISDN{$url};
        $URL_TITLE{$url}{'t'} = $title;
        $URL_TITLE{$url}{'n'} = $num;
        $URL_TITLE{$url}{'c'} = $channel;
		$URL_TITLE{$url}{'u'} = $surl;
        print "|$title|$url|[$num]\n\n";
    }
    while($content=~s/\<anchor>(.*?)<go href="(.*?)" accept-charset="UTF-8" method="post">/href/m) {
        my $title = $1;
        my $url = $2;
        $url =~ s/\r//g;
        $url=~s|\&amp;|\&|g;
        $url=~s|http\:\/\/(([^\/]*))||;
		my $surl = $1;
		$url=~s|[\?\&]PHPSESSID=[^&]*||;
        $url=~s|[\?\&]vt=[^&]*||;
        $url=~s|^\?|$index_url\?|;
        $url=~s/.*SPURL\=http:\/\///gi;
        if ($url!~m|\/|) {
        	$url = "/prog/wapsite/stock/$url";
        }
#        elsif ($url!~m|3g.sina.com.cn|) {
#        	$url = "3g.sina.com.cn$url";
#        }
        my $num = $SUB_ISDN{$url};
        $URL_TITLE{$url}{'t'} = $title;
        $URL_TITLE{$url}{'n'} = $num;
        $URL_TITLE{$url}{'c'} = $channel;
		$URL_TITLE{$url}{'u'} = $surl;
        print "|$title|$url|[$num]\n\n";
    }    
	}
	 elsif ($channel eq '社区'){
	 while($content=~s/href=[\"|\'](.*?)[\"|\']\>(.*?)<\/a\>/href/m) 
	 {
        $linknum++;
        my $title = $2;
        my $url = $1;
        $url =~ s/\r//g;
        $url=~s|\&amp;|\&|g;
        $url=~s|http\:\/\/(([^\/]*))||;
		my $surl = $1;
		my $sdomain = $2;
		if(defined $SHORT_DOMAIN_TO_LONG{$sdomain} && $url !~ /^\/3g*/)
		{
			$url = $sdomain.$url;
			$surl = "";
		}
		$url=~s|[\?\&]PHPSESSID=[^&]*||;
        $url=~s|[\?\&]vt=[^&]*||;
        $url=~s|^\?|$index_url\?|;
        $url=~s/.*SPURL\=http:\/\///gi;

#        	$url =~ s|^[^3g.sina.com.cn]|3g.sina.com.cn$url|;

#        $url =~s|^(\/community\/.*)|3g.sina.com.cn$1|;
#        $url =~s|^(\/prog\/.*)|3g.sina.com.cn$1|;
        my $num = $SUB_ISDN{$url};
        $URL_TITLE{$url}{'t'} = $title;
        $URL_TITLE{$url}{'n'} = $num;
        $URL_TITLE{$url}{'c'} = $channel;
		$URL_TITLE{$url}{'u'} = $surl;
        print "|$title|$url|[$num]\n\n";
    }	

}
 
}

#广东首页单独处理 页面链接来自浪首配置文件
{
	#获取浪首地址
	my $channel = $pos2domains{'49'};
	my $addr=requestURL("http://71.wapcms.pub.sina.com.cn/ms/wap_home_page/custom_check.php");
	foreach my $line (split(/\n/,$addr))
	{
		my (undef,undef,$name,$url) = split(/,/,$line);
		$url =~ s/\r//g;
		$url =~ s/amp\;//g;
		$url =~ s/http\:\/\/(([^\/]*))//g;
		my $surl = $1;
		my $sdomain = $2;
		if(defined $SHORT_DOMAIN_TO_LONG{$sdomain} && $url !~ /^\/3g*/)
		{
			$url = $sdomain.$url;
			$surl = "";
		}
		if($url =~ m/\/[^\?\&\/]*$/){$url .='?pos=49';}
		else{$url .='&pos=49';}
        
    $URL_TITLE{$url}{'t'} = $name;
    $URL_TITLE{$url}{'n'} = $SUB_ISDN{$url};
    $URL_TITLE{$url}{'c'} = $channel;
	$URL_TITLE{$url}{'u'} = $surl;
	}
	
	#浪首固定链接
	my @word_tmp = (
		"stock,股票,股票,http://3g.sina.com.cn/3g/finance/index.php?tid=60&did=13&vid=512",
		"gn,新闻,新闻,http://3g.sina.com.cn/nc.php",
		"gn,直播,直播,http://3g.sina.com.cn/3g/live/",
		"gn,国内新闻,国内新闻,http://3g.sina.com.cn/3g/news/?tid=110&did=2&vid=88&cid=101",
		"gj,国际新闻,国际新闻,http://3g.sina.com.cn/3g/news/?tid=110&did=3&vid=88&cid=104",
		"ent,娱乐新闻,娱乐新闻,http://3g.sina.com.cn/3g/ent/",
		"sport,体育新闻,体育新闻,http://3g.sina.com.cn/3g/sports/",
		"auto,汽车,汽车,http://3g.sina.com.cn/3g/auto/",
		"finance,财经,财经,http://3g.sina.com.cn/3g/finance/index.php?tid=60&did=2&vid=144",
		"tech,科技,科技,http://3g.sina.com.cn/3g/tech/",
		"sh,社会新闻,社会新闻,http://3g.sina.com.cn/3g/news/?tid=110&did=4&vid=88&cid=107",
		"book,书库,书库,http://3g.sina.com.cn/3g/book/index.php?tid=162&did=17&vid=96&cid=0&sid=9506",
		"book,分类,分类,http://3g.sina.com.cn/3g/book/index.php?tid=162&did=14&vid=96&cid=0&sid=9367",
		"book,推荐榜,推荐榜,http://3g.sina.com.cn/3g/book/index.php?tid=162&did=4&vid=96&cid=0&sid=7387",
		"prod,网站排行,网站排行,http://3g.sina.com.cn/prog/wapsite/webcounter/index.php",
		"prod,【图】一周美图下载,【图】一周美图下载,http://3g.sina.com.cn/common/dlf/channel.php?mid=46&bid=5000310&from=3",
		"prod,[视频]超女祝福VCR,[视频]超女祝福VCR,http://3g.sina.com.cn/common/dlf/channel.php?bid=5000167",
		"prod,炫铃,炫铃,http://3g.sina.com.cn/common/dlf/channel.php?mid=46&bid=5000305",
		"prod,酷图,酷图,http://3g.sina.com.cn/common/dlf/channel.php?mid=46&bid=5000310",
		"prod,视频,视频,http://3g.sina.com.cn/common/dlf/channel.php?bid=5000167",
		"prod,游戏,游戏,http://mbox.sina.com.cn/w/i.php?from=60001",
		"sex,两性,两性,http://3g.sina.com.cn/3g/sex/",
		"pic,色图,色图,http://3g.sina.com.cn/3g/sex/index.php?tid=160&did=2&vid=99&cid=0&sid=0",
		"book,读书,读书,http://3g.sina.com.cn/3g/book/",
		"bbs,论坛,论坛,http://3g.sina.com.cn/3g/bbs/",
		"astro,星座,星座,http://3g.sina.com.cn/3g/ast/",
		"joke,笑话,笑话,http://3g.sina.com.cn/3g/joke/",
		"house,房产,房产,http://3g.sina.com.cn/3g/house/",
		"jczs,军事,军事,http://3g.sina.com.cn/3g/jczs/",
		"blog,博客,博客,http://3g.sina.com.cn/3g/blog/",
		"blog,新闻排行,新闻排行,http://3g.sina.com.cn/3g/site/proc/hotnews/daily_index.wml",
		"eladies,女性,女性,http://3g.sina.com.cn/3g/eladies/",
		"gd,广东新闻,广东新闻,http://3g.sina.com.cn/3g/news/index.php?ptype=0&pid=17&did=10&cid=396&wid=11&tid=110&vid=88",
		"scroll,滚动新闻,滚动新闻,http://3g.sina.com.cn/3g/news/index.php?did=1&tid=110&vid=414&pos=1",
		"pro,百事通,百事通,http://3g.sina.com.cn/3g/pro/index.php?tid=254&did=612&vid=84",
		"pro,天气,天气,http://3g.sina.com.cn/prog/wapsite/weather_new/index.php",
		"pro,下载,下载,http://3g.sina.com.cn/prog/wapsite/fd/index.php",
		"pro,搜索,搜索,http://3g.sina.com.cn/iask/?p=3g",
		"pro,软件,软件,http://3g.sina.com.cn/3g/soft/",
		"pro,游戏,游戏,http://3g.sina.com.cn/3g/game/",
		"pro,江湖,江湖,http://3g.sina.com.cn/community/prog/home/ind.php?to=home",
		"pro,江湖,江湖,http://3g.sina.com.cn/community/prog/home/ind.php?to=home&from=4",
		"pro,奥运,奥运,http://3g.sina.com.cn/3g/2008/",
		"pro,金牌,金牌,http://3g.sina.com.cn/3g/2008/index.php?tid=514&did=122261&vid=84",
		"pro,赛程,赛程,http://3g.sina.com.cn/2008/match/si.php&vt=1",
		"pro,直播,直播,http://3g.sina.com.cn/prog/wapsite/live/live_list.php?type=25",
		"pro,积分,积分,http://3g.sina.com.cn/community/prog/home/ind.php?to=score",
		"pro,健康,健康,http://3g.sina.com.cn/3g/news/index.php?tid=141&did=53&vid=68",
		"pro,壁纸,壁纸,http://wap.sina.com.cn/cms/demo.php?pid=7396&from=301000&app=1",
		"pro,铃声,铃声,http://wap.sina.com.cn/cms/demo.php?pid=7381&from=301000&app=1",
		"edu,教育,教育,http://3g.sina.com.cn/3g/edu/",
		"finance,金融危机,金融危机,http://3g.sina.com.cn/3g/finance/index.php?tid=72&did=565&vid=37",
		"finance,环球股指,环球股指,http://3g.sina.com.cn/prog/wapsite/stock/shareindex.php",
		"finance,股市滚动要闻,股市滚动要闻,http://3g.sina.com.cn/3g/finance/index.php?did=13&tid=60&vid=419",
	);	
	foreach my $line (@word_tmp)
	{
		my (undef,undef,$name,$url) = split(/,/,$line);
		$url =~ s/\r//g;
		$url =~ s/http\:\/\/(([^\/]*))//g;
		my $surl = $1;
		my $sdomain = $2;
		if(defined $SHORT_DOMAIN_TO_LONG{$sdomain} && $url !~ /^\/3g*/)
		{
			$url = $sdomain.$url;
			$surl = "";
		}
		if($url =~ m/\/[^\?\&\/]*$/){$url .='?pos=49';}
		else{$url .='&pos=49';}

    $URL_TITLE{$url}{'t'} = $name;
    $URL_TITLE{$url}{'n'} = $SUB_ISDN{$url};
    $URL_TITLE{$url}{'c'} = $channel;
	$URL_TITLE{$url}{'u'} = $surl;

#		$url_num{$url}{'t'} = $name;
#		$url_num{$url}{'n'} = $SUB_ISDN{$url};
	}
	

}



	message("频道首页入库处理");
	my $db = new db('wap_master');
	my $conn = $db->{'conn'};
	my ($sql,$stmt);
	my $visit_day = $HALF_HOUR_YEAR.'-'.$HALF_HOUR_MON.'-'.$HALF_HOUR_DAY;
	my $visit_hour = $HALF_HOUR_HOUR.':00';
	my $tenDaysAgo = dateConv($GDATE,-31,'yyyy-mm-dd');

	my $name_string;
	my $tk = 0;
	foreach my $name (values %pos2domains) 
	{
		if($tk == 0){$tk =1; $name_string .= " '$name' ";}
		else{$name_string .= ", '$name' ";}
	}
	$conn->do("set names 'latin1'");

	$conn->do("delete from CHANNEL_HOUR_PV where visit_day = '".$tenDaysAgo."' and source='$source'");
	print "delete1\n";
	$conn->do("delete from CHANNEL_HOUR_PV where visit_day='$visit_day' and visit_hour='$visit_hour' and channel in ($name_string ) and source='$source'");
	print "delete2\n";
	foreach  my $url (keys %URL_TITLE)
	{
		my $title = $URL_TITLE{$url}{'t'};
		my $pv = $URL_TITLE{$url}{'n'};
		my $channel = $URL_TITLE{$url}{'c'};
		next if($pv eq undef);
		next if($title eq undef);
		$channel =~ s/滚动//;
		#$url = '3g.sina.com.cn'.$url;
		$url = $URL_TITLE{$url}{'u'}.$url;
		print "[$pv]-$title-$url-$channel\n";
		$conn->do("insert into CHANNEL_HOUR_PV set visit_day='$visit_day',visit_hour='$visit_hour',url='$url',channel='$channel',pv=$pv,title='$title',source='$source'");

	}
	my $sql_complete = "replace RSS_HALF_HOUR_PROG_STATUS set time = '$visit_day $visit_hour',table_name = 'CHANNEL_HOUR_PV',ip = '221.179.175.133';";
	$conn->do($sql_complete);

	$conn->disconnect;

}


print `free -m`;
undef %SUB_ISDN;
print `free -m`;

message("任务点击量分析");
{
	my %ALL_JOBS=();
	my %allR=();
	my %result=();
		
	my $db_slave = new db('wap_slave');
	my $conn_slave = $db_slave->{'conn'};
	
	my $sql="select id,host,type,url,rs_unit,require_type,begin_time,end_time from RSS_SELF_ISDN_JOBS where status =1 and rs_unit=0";
	my $stmt=$conn_slave->prepare($sql);
	$stmt->execute() or print "ERROR: $DBI::errstr\n";
	while( my ($id,$host,$type,$url,$rs_unit,$require_type,$begin_time,$end_time)=$stmt->fetchrow_array)
	{
		next if ($rs_unit eq "1");#如果统计单位是天则不在此处统计
		if ($require_type eq "1")
		{#如果统计需求的类型是时间段			
			my $now=getToday('yyyy-mm-dd');
			if (($begin_time le $now ) && ($now le $end_time))
			{	}
			else
			{
				next;
			}
		}
		#精确不加空格
		if($type==2)
		{
			$url =~ s/\`/\.*/g;	
			$url =~ s/\?/\\\?/g;	
		}
		$ALL_JOBS{$host}{"$id,$type"}=$url;
		#print "需要统计$host $id $type \n";
		$allR{$id}=$url;	
	}
	$stmt->finish;
	$conn_slave->disconnect;

#print Dumper(\%ALL_ISDN);
#print "==========上 ALL_ISDN===========下 ALL_JOBS==========\n";
#print Dumper(\%ALL_JOBS);

	foreach my $host (keys %ALL_JOBS)
	{
		foreach my $r_arr_p (@{$ALL_ISDN{$host}})
		{
			my ($request,$mobile) = @$r_arr_p;
			next if (invalidExtname($request) == 1);
			while(my ($key,$url) = each %{$ALL_JOBS{$host}})
			{
#				next if (index($request,$url)<0);#带`的模糊会被过滤，所以去掉这句判断
				my ($id,$type)=split(/,/,$key);
				#print "开始统计 $host $request,$mobile 统计类型 $id,$type \n";
				
				if ($type==1)
				{#精确匹配
					if ($request eq $url)
					{
						$result{$id}{'pv'}++;
						if ($mobile ne "-")
						{
							$result{$id}{'m'}{$mobile}=1;
						}
						else
						{
							$result{$id}{'b_pv'}++;
						}
					}								
				}
				elsif($type==0)
				{#前缀匹配				
					if (index($request,$url)==0)
					{
						$result{$id}{'pv'}++;
						if ($mobile ne "-")
						{
							$result{$id}{'m'}{$mobile}=1;
						}
						else
						{
							$result{$id}{'b_pv'}++;
						}
					}								
				}
				elsif($type==2)
				{#模糊匹配				
					if ($request =~ m/$url/)#最上面就已经判断$b =~ /$a/
					{
						$result{$id}{'pv'}++;
						if ($mobile ne "-")
						{
							$result{$id}{'m'}{$mobile}=1;
						}
						else
						{
							$result{$id}{'b_pv'}++;
						}
					}								
				}
				
			}
		}			
	}

foreach my $id(keys %result){print "result $id pv: $result{$id}{'pv'}\n";}

	message("任务入库处理");
	my $db = new db('wap_master');
	my $conn = $db->{'conn'};
	my ($sql,$stmt);
	my $visit_day = $HALF_HOUR_YEAR.'-'.$HALF_HOUR_MON.'-'.$HALF_HOUR_DAY;
	my $visit_hour = $HALF_HOUR_HOUR.':00';
	my $i=0;
	$conn->do("set names 'latin1'");

	foreach my $id (keys %result)
	{
		my $pv=isKeyExists($result{$id}{'pv'});
		my $user=isKeyExists(scalar keys %{$result{$id}{'m'}});
		my $user_blank=isKeyExists($result{$id}{'b_pv'});
		print "replace:$id,$pv,$user,$user_blank\n";
		$sql="replace into RSS_SELF_ISDN_HOUR_STAT 
					set id='$id',
						visit_day='$visit_day',
						visit_hour='$visit_hour',
						pv='$pv',
						user='$user',
						source='$source',
						user_blank='$user_blank'";
		$i +=$conn->do($sql);
	}	
	print "共插入$i条记录\n";	
	$conn->disconnect;



}

my $endtime=getToday('yyyy-mm-dd hh:mm:ss');
print "$begintime $endtime \n";


exit;

##
# 函数名称: rsyncLog
# 函数功能：对给定源、目标的主机进行日志同步.
# 参数: 远程主机的rsync模块及文件名，本机对应文件名，日志类型。
# 返回值：无
##
sub rsyncLog()
{
	foreach my $domain_name (keys %RSYNC_CONFIG_LOGFILE)
	{
		#next if ($domain_name ne 'news.=3g.sina.com.cn') ;
		my $remoteFiles_ref	= $RSYNC_CONFIG_LOGFILE{$domain_name}{'remoteFiles'};
		my $hourFiles_ref=$RSYNC_CONFIG_LOGFILE{$domain_name}{'hourFiles'};
		foreach my $host (keys %$remoteFiles_ref)
		{
			my $localfile = $$hourFiles_ref{$host};
			my $remotefile = $host . $$remoteFiles_ref{$host};
			$localfile =~ m/^(.*)\/[^\/]+$/;
			my $localdir = $1;
			my $cmd_mkdir = "mkdir -p $localdir";
			
			print "创建目录|".$cmd_mkdir  ."|\n";
			system("$cmd_mkdir")==0 
				or warning("创建目录出错 日志类型:$domain_name  $!");
			
			my $cmd="$RSYNC $remotefile $localfile";
			print "同步日志|".$cmd  ."|\n";
			system("$cmd")==0 
				or warning("同步日志出错 日志类型:$domain_name  $!");
			my $size = (stat($localfile)) [7];
			if($size > 419430400) {sendSMS_new("ft ${domain_name}${host}日志大于400M");}
			#if($size == 0) {sendSMS_new("ft ${domain_name}${host}日志等于0");}
			
		}
	}
}




