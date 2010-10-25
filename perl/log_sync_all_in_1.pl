#!/usr/bin/perl
#===========================================================================
#��Сʱͳ��
# 1����Сʱ��־��ȡ 2�����װ�Сʱͳ�� 3�������Сʱͳ�� 4��Ƶ����ҳ��Сʱ
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

#���������ݿ�
{
	my $date = $GDATE . $hour_start . $min_start;
	my $LOG_PATH = '/data1/sinastat/var/isdn/TIME/';
	my $TEMP_PATH = '/data1/sinastat/var/isdn/TEMP/';
	my $db_PROG = new db('PROG_SLAVE');
	my $conn_PROG = $db_PROG->{'conn'}; #Ѱ������
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


message("��ȡ��־");
#rsyncLog() if($HALF_HOUR_HOUR eq '');#����������ʱ������ȡ��־
rsyncLog();#����������ʱ������ȡ��־




message("�������ܶ�ȡ��־");
#��ƽF5ip������Ϊ�Ϸ�
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

message("�������а���־");
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

message("���������Ʒ���а���־");
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

message("������־");
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

#�������Ƽ��滻
			$r =~s/pos=text_old/pos=6/;

			$r =~s/[\?&]MISC_ID=[^&]*//g;
			$r =~s/&MISC_SessionID=[^&]*//g;
			$r =~s/&MISC_AccessMode=[^&]*//g;
			$r =~s/&MISC_ServiceID=[^&]*//g;
			$r =~s/&PHPSESSID=[^&]*//g;
			$r =~s/\/&/\/\?/g;
			#��������ǰ׺ by lixiang8 20100514
			$r = $LONG_DOMAIN_TO_SHORT{$domain_name}.$r if(defined $LONG_DOMAIN_TO_SHORT{$domain_name} && $r !~ /^\/3g*/);
			$SUB_ISDN{$r}++;
		}
	}
}

print Dumper(\%SUB_ISDN);

message("��ҳ���������");
{
	my %url_num=();

#���׹̶���������
	my $addr_pos=requestURL("http://wapcms.pub.sina.com.cn/ms/wap_home_page/block_view.php");
#	$addr_pos=$converter->convert($addr_pos);print $addr_pos;
	print "�̶���������\n";
	print $addr_pos;
	print "�̶���������\n";
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
		#pos=1��棬100�ʰ壬101�ʰ�,200�㶫����
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
		$URL_TITLE{$url4}{'c'} = '�㶫����';
		$URL_TITLE{$url4}{'u'} = $surl;
	}	

	#��ȡ���׵�ַ
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
		#pos=1��棬100�ʰ壬101�ʰ�,200�㶫����
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
		$URL_TITLE{$url5}{'c'} = '�㶫����';
		$URL_TITLE{$url5}{'u'} = $surl;
	}


	#�Ƽ�λ�ȷ����������ĵ�3
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
	
	message("������⴦��");
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


message("Ƶ����ҳ���������");
{
#��Ҫͳ�Ƶ�����������ҳ�Ķ�Ӧ��ϵ���ڣ����ʣ���ᣬ���������֡��ƾ������ԡ����顢����
my %DOMAINS = (

    '����' => {
        'domain' => 'news.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/news/index.php?tid=141&did=48&vid=68&cid=785&sid=0', #��ҳ����,
        'display' => '1',
    },
    '����' => {
        'domain' => 'news.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/news/index.php?tid=141&did=49&vid=68&cid=786&sid=0', #��ҳ����,
        'display' => '1',
    },
    '���' => {
        'domain' => 'news.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/news/index.php?tid=141&did=50&vid=68&cid=787&sid=0', #��ҳ����,
        'display' => '1',
    },
    '����' => {
        'domain' => 'news.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/news/index.php?tid=141&did=51&vid=68&cid=788&sid=0', #��ҳ����,
        'display' => '1',
    },
    '����' => {
        'domain' => 'news.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/news/index.php?tid=141&did=53&vid=68&cid=846&sid=0', #��ҳ����,
        'display' => '1',
    },
    '����' => {
        'domain' => '2008.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/2008/', #��ҳ����,
        'display' => '1',
    },
    '����' => {
        'domain' => 'news.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/news/index.php?did=519&tid=122&vid=69', #��ҳ����,
        'display' => '1',
    },
#    'ŷ�ޱ�' => {
#        'domain' => 'sports.3g.sina.com.cn',  #�������������ļ��������key
#        'index_url' => '/3g/sports/index.php?did=12', #��ҳ����,
#        'display' => '1',
#    },
    '����' => {
        'domain' => 'edu.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/edu/', #��ҳ����,
        'display' => '1',
    },
    '����' => {
        'domain' => 'sports.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/sports/', #��ҳ����,
        'display' => '1',
    },
    '����' => {
        'domain' => 'ent.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/ent/', #��ҳ����,
        'display' => '1'
     },
    '���ֹ���' => {
        'domain' => 'pro.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/pro/index.php?tid=240&did=20&vid=1536&ch=ent', #��ҳ����,
        'display' => '1',
    },
    '�Ƽ�����' => {
        'domain' => 'pro.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/pro/index.php?tid=240&did=20&vid=1536&ch=tech', #��ҳ����,
        'display' => '1',
    },
    '�ƾ�����' => {
        'domain' => 'pro.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/pro/index.php?tid=240&did=20&vid=1536&ch=finance', #��ҳ����,
        'display' => '1',
    },
    '���ڹ���' => {
        'domain' => 'pro.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/pro/index.php?tid=240&did=20&vid=1536&ch=gn', #��ҳ����,
        'display' => '1',
    },
    '���ʹ���' => {
        'domain' => 'pro.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/pro/index.php?tid=240&did=20&vid=1536&ch=gj', #��ҳ����,
        'display' => '1',
    },
    '������' => {
        'domain' => 'pro.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/pro/index.php?tid=240&did=20&vid=1536&ch=sh', #��ҳ����,
        'display' => '1',
    },
    '���͹���' => {
        'domain' => 'pro.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/pro/index.php?tid=240&did=20&vid=1536&ch=blog', #��ҳ����,
        'display' => '1',
    },
    '�ƾ�' => {
        'domain' => 'finance.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/finance/', #��ҳ����,
        'display' => '1',
    },
        
    '����' => {
        'domain' => 'book.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/book/', #��ҳ����,
        'display' => '1',
    },
    '��Ϸ' => {
        'domain' => 'game.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/game/', #��ҳ����,
        'display' => '1',
    },
    '����' => {
        'domain' => 'blog.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/blog/', #��ҳ����,
        'display' => '1',
    },

    '����' => {
        'domain' => 'mil.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/mil/', #��ҳ����,
        'display' => '1',
    },
    
    '����' => {
        'domain' => 'auto.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/auto/', #��ҳ����,
        'display' => '1',
    },    

    '����' => {
        'domain' => 'house.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/house/', #��ҳ����,
        'display' => '1',
    },

    '�ֻ�' => {
        'domain' => 'mobile.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/mobile/', #��ҳ����,
        'display' => '1',
    },    

    'Ц��' => {
        'domain' => 'joke.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/joke/', #��ҳ����,
        'display' => '1',
    },    

    'ӰԺ' => {
        'domain' => 'tv.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/tv/', #��ҳ����,
        'display' => '1',
    },    

    'ֱ��' => {
        'domain' => 'live.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/live/', #��ҳ����,
        'display' => '1',
    },    

    '��̳' => {
        'domain' => 'bbs.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/bbs/', #��ҳ����,
        'display' => '1',
    },    

    'Ů��' => {
        'domain' => 'eladies.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/eladies/', #��ҳ����,
        'display' => '1',
    },    

    '����' => {
        'domain' => 'community.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/community/prog/home/home.php', #��ҳ����,
        'display' => '1',
    },    

    'Ӣ��' => {
        'domain' => '',  #�������������ļ��������key
        'index_url' => '/3g/sports/?&tid=31&did=30&vid=17&cid=652', #��ҳ����,
        'display' => '1',
    },    

    'NBA' => {
        'domain' => '',  #�������������ļ��������key
        'index_url' => '/3g/sports/index.php?tid=12&did=45&vid=18', #��ҳ����,
        'display' => '1',
    },    

    'ͼƬ' => {
        'domain' => '',  #�������������ļ��������key
        'index_url' => '/common/dlf/channel.php?mid=46&bid=5000310', #��ҳ����,
        'display' => '1',
    },    

    '����' => {
        'domain' => '',  #�������������ļ��������key
        'index_url' => '/common/dlf/channel.php?mid=46&bid=5000305', #��ҳ����,
        'display' => '1',
    },    

    '��������' => {
        'domain' => '',  #�������������ļ��������key
        'index_url' => '/prog/wapsite/fd/index.php', #��ҳ����,
        'display' => '1',
    },    

    '����' => {
        'domain' => '',  #�������������ļ��������key
        'index_url' => '/3g/news/index.php?tid=141&did=52&vid=68&cid=789&sid=0', #��ҳ����,
        'display' => '1',
    },    
    
    #��������ҳͳ�ƣ�����Ϊ����Ƶ�����õ������־
    'Ӧ��ǰ��1' => {
        'domain' => 'wapsite.3g.sina.com.cn',  #�������������ļ��������key
        'display' => '0',
    },

    #��������ҳͳ�ƣ�����Ϊ����Ƶ�����õ������־
    'Ӧ��ǰ��2' => {
        'domain' => 'nba.prog.3g.sina.com.cn',  #�������������ļ��������key
        'display' => '0',
    },

    #��������ҳͳ�ƣ�����Ϊ����Ƶ�����õ������־
    'Ӧ��ǰ��3' => {
        'domain' => 'stock.prog.3g.sina.com.cn',  #�������������ļ��������key
        'display' => '0',
    },
    
    #��������ҳͳ�ƣ�����Ϊ����Ƶ�����õ������־
    'Ӧ��ǰ��4' => {
        'domain' => 'book.prog.3g.sina.com.cn',  #�������������ļ��������key
        'display' => '0',
    },    
    

    #������־��������Ƶ����ҳ
    'WAPCMS����' => {
        'domain' => 'pro.3g.sina.com.cn',
        'display' => '0',
    },

   #####����ҵ��
       'wapdl2' => {
        'domain' => 'wapdl2.sina.com.cn',
        'display' => '0',
		},
		
    'ɫͼ' => {
        'domain' => 'sexpic.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/sexpic/', #��ҳ����,
        'display' => '1',
    },
    
    '����' => {
        'domain' => 'sex.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/sex/', #��ҳ����,
        'display' => '1',
    },

    '����' => {
        'domain' => 'ast.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/ast/', #��ҳ����,
        'display' => '1',
    },
    '�Ƽ�' => {
            'domain' => 'tech.3g.sina.com.cn',  #�������������ļ��������key
            'index_url' => '/3g/tech/', #��ҳ����,
            'display' => '1',
    },
    
    '��Ʊ' => {
            'domain' => 'lotto.3g.sina.com.cn',  #�������������ļ��������key
            'index_url' => '/3g/lotto/', #��ҳ����,
            'display' => '1',
    },  
    '����' => {
            'domain' => 'qiwen.3g.sina.com.cn',  #�������������ļ��������key
            'index_url' => '/3g/qiwen/', #��ҳ����,
            'display' => '1',
    },        
    '��̬��' => {
        'domain' => 'dpool.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => 'http://3g.sina.com.cn/dpool/', #��ҳ����,
        'display' => '0',
    },     
    '��Ʊ' => {
            'domain' => '3g.sina.com.cn',  #�������������ļ��������key  #��Ϊ��Ʊû����������ǰ�˵�����
            'index_url' => '/3g/finance/index.php?tid=60&did=13&vid=512', #��ҳ����,
            'display' => '1',
    },
    
#    '��Ʊ' => {
#            'domain' => '3g.sina.com.cn',  #�������������ļ��������key  #��Ϊ��Ʊû����������ǰ�˵�����
#            'index_url' => '/prog/wapsite/stock/index.php', #��ҳ����,
#            'display' => '1',
#    },

		'��Ƶ' => {
            'domain' => 'video.3g.sina.com.cn',  #�������������ļ��������key
            'index_url' => '/3g/video/', #��ҳ����,
            'display' => '1',
    },
    
    ###��Ʒͳ��###�Ѿ��ټӾ�Ʒͳ����Ҫkey�а�����Ʒ����,�ں���ĳ����л����⴦��
    '��ƷŮ��' => {
        'domain' => 'lady.jp.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/lady/', #��ҳ����,
        'display' => '1',
    }, 
    '��Ʒ��־' => {
        'domain' => 'prog.jp.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => 'http://prog.jp.3g.sina.com.cn', #��ҳ����,
        'display' => '0',
    },       
   ###��Ʒͳ��
   
       '��Ƶ������' => {
        'domain' => 'site.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '/3g/site/', #��ҳ����,
        'display' => '0',
    },       
    '���ȹ㶫��ҳ' => {
        'domain' => 'index.3g.sina.com.cn',  #�������������ļ��������key
        'index_url' => '', #��ҳ����,
        'display' => '1',
    },
    '����' => {
            'domain' => 'expo.3g.sina.com.cn',  #�������������ļ��������key
            'index_url' => '/3g/expo/', #��ҳ����,
            'display' => '1',
    },  
    '���籭' => {
            'domain' => '2010.3g.sina.com.cn',  #�������������ļ��������key
            'index_url' => '/3g/2010/', #��ҳ����,
            'display' => '1',
    },  
    '��ʷ' => {
            'domain' => 'cul.3g.sina.com.cn',  #�������������ļ��������key
            'index_url' => '/3g/cul/', #��ҳ����,
            'display' => '1',
    },  
    '�㶫����' => {
            'domain' => 'book.3g.sina.com.cn',  #�������������ļ��������key
            'index_url' => '/3g/book/?vid=2565', #��ҳ����,
            'display' => '1',
    },  
    '����' => {
            'domain' => 'travel.3g.sina.com.cn',  #�������������ļ��������key
            'index_url' => '/3g/travel/', #��ҳ����,
            'display' => '1',
    },
    '�ݸ�����' => {
            'domain' => '3g.sina.com.cn',  #�������������ļ��������key
            'index_url' => '/3g/blog/?sa=t331d119v2623', #��ҳ����,
            'display' => '1',
    },
    '����' => {
            'domain' => 'comic.3g.sina.com.cn',  #�������������ļ��������key
            'index_url' => '/3g/comic/', #��ҳ����,
            'display' => '1',
    },
);



my %pos2domains = (

'8' => '����',	
'9' => '����',	
'10' => '����',	
'11' => '���',	
'12' => '����',	
'13' => '����',	
'14' => 'ɫͼ',	
'15' => '����',	
'16' => '����',	
'17' => '�ƾ�',	
'18' => '�Ƽ�',	
'19' => '����',	
'20' => '��ƷŮ��',	
'21' => '��Ʊ',	
'22' => '��Ϸ',	
'23' => '��Ƶ',	
'24' => '����',	

'25' => '����',
'26' => '����',
'27' => '�ֻ�',
'28' => 'Ц��',
'29' => 'ӰԺ',
'30' => 'ֱ��',
'31' => '��̳',
'32' => 'Ů��',
'33' => '����',

'34' => 'Ӣ��',
'35' => 'ͼƬ',
'36' => '����',
'37' => '��������',

'38' => '����',
'39' => '����',
'40' => 'NBA',
#'41' => '������ų�����ʽ'
'42' => '����',
#'43' => '����ҳ������'
#'44' => '����-���ŭU'
'45' => '����',
'46' => 'ŷ�ޱ�',
'47' => '����',
'48' => '����',
#'100' => '�������',
#'100' => '��Ϊ���ײʰ�',2009��5��22��9:58:51
#'101' => '����3G��',
'49' => '���ȹ㶫��ҳ',
#'50' => '�Ƽ�3G��ҳ',
'54' => '����',
'55' => '���籭',
'56' => '��ʷ',
'57' => '�㶫����',
'58' => '����',
'59' => '�ݸ�����',
'60' => '����',
'200' => '�㶫����',
);	

#my $channel = '��Ƶ';
foreach my $channel (keys %DOMAINS) 
{
		#if ($channel ne '��Ʊ') {next;}
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
    if ($channel=~m/��Ʒ/) {
    	$url = "http://jp.3g.sina.com.cn$index_url";
    }
    $content=requestURL("$url");
    print "��ҳ $url\n";

	$content=$converter->convert($content);
	if (($channel ne '��Ʊ') && ($channel ne '����')) {
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
   elsif ($channel eq '��Ʊ'){
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
	 elsif ($channel eq '����'){
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

#�㶫��ҳ�������� ҳ�������������������ļ�
{
	#��ȡ���׵�ַ
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
	
	#���׹̶�����
	my @word_tmp = (
		"stock,��Ʊ,��Ʊ,http://3g.sina.com.cn/3g/finance/index.php?tid=60&did=13&vid=512",
		"gn,����,����,http://3g.sina.com.cn/nc.php",
		"gn,ֱ��,ֱ��,http://3g.sina.com.cn/3g/live/",
		"gn,��������,��������,http://3g.sina.com.cn/3g/news/?tid=110&did=2&vid=88&cid=101",
		"gj,��������,��������,http://3g.sina.com.cn/3g/news/?tid=110&did=3&vid=88&cid=104",
		"ent,��������,��������,http://3g.sina.com.cn/3g/ent/",
		"sport,��������,��������,http://3g.sina.com.cn/3g/sports/",
		"auto,����,����,http://3g.sina.com.cn/3g/auto/",
		"finance,�ƾ�,�ƾ�,http://3g.sina.com.cn/3g/finance/index.php?tid=60&did=2&vid=144",
		"tech,�Ƽ�,�Ƽ�,http://3g.sina.com.cn/3g/tech/",
		"sh,�������,�������,http://3g.sina.com.cn/3g/news/?tid=110&did=4&vid=88&cid=107",
		"book,���,���,http://3g.sina.com.cn/3g/book/index.php?tid=162&did=17&vid=96&cid=0&sid=9506",
		"book,����,����,http://3g.sina.com.cn/3g/book/index.php?tid=162&did=14&vid=96&cid=0&sid=9367",
		"book,�Ƽ���,�Ƽ���,http://3g.sina.com.cn/3g/book/index.php?tid=162&did=4&vid=96&cid=0&sid=7387",
		"prod,��վ����,��վ����,http://3g.sina.com.cn/prog/wapsite/webcounter/index.php",
		"prod,��ͼ��һ����ͼ����,��ͼ��һ����ͼ����,http://3g.sina.com.cn/common/dlf/channel.php?mid=46&bid=5000310&from=3",
		"prod,[��Ƶ]��Ůף��VCR,[��Ƶ]��Ůף��VCR,http://3g.sina.com.cn/common/dlf/channel.php?bid=5000167",
		"prod,����,����,http://3g.sina.com.cn/common/dlf/channel.php?mid=46&bid=5000305",
		"prod,��ͼ,��ͼ,http://3g.sina.com.cn/common/dlf/channel.php?mid=46&bid=5000310",
		"prod,��Ƶ,��Ƶ,http://3g.sina.com.cn/common/dlf/channel.php?bid=5000167",
		"prod,��Ϸ,��Ϸ,http://mbox.sina.com.cn/w/i.php?from=60001",
		"sex,����,����,http://3g.sina.com.cn/3g/sex/",
		"pic,ɫͼ,ɫͼ,http://3g.sina.com.cn/3g/sex/index.php?tid=160&did=2&vid=99&cid=0&sid=0",
		"book,����,����,http://3g.sina.com.cn/3g/book/",
		"bbs,��̳,��̳,http://3g.sina.com.cn/3g/bbs/",
		"astro,����,����,http://3g.sina.com.cn/3g/ast/",
		"joke,Ц��,Ц��,http://3g.sina.com.cn/3g/joke/",
		"house,����,����,http://3g.sina.com.cn/3g/house/",
		"jczs,����,����,http://3g.sina.com.cn/3g/jczs/",
		"blog,����,����,http://3g.sina.com.cn/3g/blog/",
		"blog,��������,��������,http://3g.sina.com.cn/3g/site/proc/hotnews/daily_index.wml",
		"eladies,Ů��,Ů��,http://3g.sina.com.cn/3g/eladies/",
		"gd,�㶫����,�㶫����,http://3g.sina.com.cn/3g/news/index.php?ptype=0&pid=17&did=10&cid=396&wid=11&tid=110&vid=88",
		"scroll,��������,��������,http://3g.sina.com.cn/3g/news/index.php?did=1&tid=110&vid=414&pos=1",
		"pro,����ͨ,����ͨ,http://3g.sina.com.cn/3g/pro/index.php?tid=254&did=612&vid=84",
		"pro,����,����,http://3g.sina.com.cn/prog/wapsite/weather_new/index.php",
		"pro,����,����,http://3g.sina.com.cn/prog/wapsite/fd/index.php",
		"pro,����,����,http://3g.sina.com.cn/iask/?p=3g",
		"pro,���,���,http://3g.sina.com.cn/3g/soft/",
		"pro,��Ϸ,��Ϸ,http://3g.sina.com.cn/3g/game/",
		"pro,����,����,http://3g.sina.com.cn/community/prog/home/ind.php?to=home",
		"pro,����,����,http://3g.sina.com.cn/community/prog/home/ind.php?to=home&from=4",
		"pro,����,����,http://3g.sina.com.cn/3g/2008/",
		"pro,����,����,http://3g.sina.com.cn/3g/2008/index.php?tid=514&did=122261&vid=84",
		"pro,����,����,http://3g.sina.com.cn/2008/match/si.php&vt=1",
		"pro,ֱ��,ֱ��,http://3g.sina.com.cn/prog/wapsite/live/live_list.php?type=25",
		"pro,����,����,http://3g.sina.com.cn/community/prog/home/ind.php?to=score",
		"pro,����,����,http://3g.sina.com.cn/3g/news/index.php?tid=141&did=53&vid=68",
		"pro,��ֽ,��ֽ,http://wap.sina.com.cn/cms/demo.php?pid=7396&from=301000&app=1",
		"pro,����,����,http://wap.sina.com.cn/cms/demo.php?pid=7381&from=301000&app=1",
		"edu,����,����,http://3g.sina.com.cn/3g/edu/",
		"finance,����Σ��,����Σ��,http://3g.sina.com.cn/3g/finance/index.php?tid=72&did=565&vid=37",
		"finance,�����ָ,�����ָ,http://3g.sina.com.cn/prog/wapsite/stock/shareindex.php",
		"finance,���й���Ҫ��,���й���Ҫ��,http://3g.sina.com.cn/3g/finance/index.php?did=13&tid=60&vid=419",
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



	message("Ƶ����ҳ��⴦��");
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
		$channel =~ s/����//;
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

message("������������");
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
		next if ($rs_unit eq "1");#���ͳ�Ƶ�λ�������ڴ˴�ͳ��
		if ($require_type eq "1")
		{#���ͳ�������������ʱ���			
			my $now=getToday('yyyy-mm-dd');
			if (($begin_time le $now ) && ($now le $end_time))
			{	}
			else
			{
				next;
			}
		}
		#��ȷ���ӿո�
		if($type==2)
		{
			$url =~ s/\`/\.*/g;	
			$url =~ s/\?/\\\?/g;	
		}
		$ALL_JOBS{$host}{"$id,$type"}=$url;
		#print "��Ҫͳ��$host $id $type \n";
		$allR{$id}=$url;	
	}
	$stmt->finish;
	$conn_slave->disconnect;

#print Dumper(\%ALL_ISDN);
#print "==========�� ALL_ISDN===========�� ALL_JOBS==========\n";
#print Dumper(\%ALL_JOBS);

	foreach my $host (keys %ALL_JOBS)
	{
		foreach my $r_arr_p (@{$ALL_ISDN{$host}})
		{
			my ($request,$mobile) = @$r_arr_p;
			next if (invalidExtname($request) == 1);
			while(my ($key,$url) = each %{$ALL_JOBS{$host}})
			{
#				next if (index($request,$url)<0);#��`��ģ���ᱻ���ˣ�����ȥ������ж�
				my ($id,$type)=split(/,/,$key);
				#print "��ʼͳ�� $host $request,$mobile ͳ������ $id,$type \n";
				
				if ($type==1)
				{#��ȷƥ��
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
				{#ǰ׺ƥ��				
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
				{#ģ��ƥ��				
					if ($request =~ m/$url/)#��������Ѿ��ж�$b =~ /$a/
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

	message("������⴦��");
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
	print "������$i����¼\n";	
	$conn->disconnect;



}

my $endtime=getToday('yyyy-mm-dd hh:mm:ss');
print "$begintime $endtime \n";


exit;

##
# ��������: rsyncLog
# �������ܣ��Ը���Դ��Ŀ�������������־ͬ��.
# ����: Զ��������rsyncģ�鼰�ļ�����������Ӧ�ļ�������־���͡�
# ����ֵ����
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
			
			print "����Ŀ¼|".$cmd_mkdir  ."|\n";
			system("$cmd_mkdir")==0 
				or warning("����Ŀ¼���� ��־����:$domain_name  $!");
			
			my $cmd="$RSYNC $remotefile $localfile";
			print "ͬ����־|".$cmd  ."|\n";
			system("$cmd")==0 
				or warning("ͬ����־���� ��־����:$domain_name  $!");
			my $size = (stat($localfile)) [7];
			if($size > 419430400) {sendSMS_new("ft ${domain_name}${host}��־����400M");}
			#if($size == 0) {sendSMS_new("ft ${domain_name}${host}��־����0");}
			
		}
	}
}




