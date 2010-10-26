#!/usr/bin/perl

#调试开关
#9 所有信息
#8 Dumpper调试信息
#7 数据库信息
#6 数组调试信息
#5 文件句柄
$debug = 6;

#===========================================================================
#半小时统计
# 1：半小时日志提取 2：浪首半小时统计 3：任务半小时统计 4：频道首页半小时
#===========================================================================



BEGIN { push @INC, qw(/opt/test/fengtai2/rss/ /data1/sinastat/code/run/rss/) }
BEGIN { push @INC, qw(/opt/test/fengtai2/lib/ /data1/sinastat/code/run/lib/) }
$HALF_HOUR_YEAR=$ARGV[0];
$HALF_HOUR_MON=$ARGV[1];
$HALF_HOUR_DAY=$ARGV[2];
$HALF_HOUR_HOUR=$ARGV[3];
$PROGRAM=__FILE__;

use Data::Dumper();
use Text::Iconv;
Text::Iconv->raise_error(0);

require ('/opt/test/fengtai2/config/gateway.conf');
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
my @CHANNEL_LISTS = ();

$SINA_PERL_BASE="/usr/local/sinawap/perl/bin/perl";
%SINA_PERL_TOPS_SCRIPTS = ("wapcms" => "/opt/test/fengtai2/rss/news_rank.pl", "book.prog.3g.sina.com.cn" => "/opt/test/fengtai2/rss/book_rank.pl");
%SINA_PERL_TOPS_TMPS = ("wapcms" => "/tmp/news_rank.log", "book.prog.3g.sina.com.cn" => "book_rank.log");

#读配置数据库
{
    my $date = $GDATE . $hour_start . $min_start;
    my $LOG_PATH = '/opt/test/';
    my $TEMP_PATH = '/opt/test/tmp/';
    my $db_PROG = new db('PROG_SLAVE');
    my $conn_PROG = $db_PROG->{'conn'}; #寻找属性
    my ($stmt,$sql);
    # for debug:
    $date = '201010250500';
    $sql = "select channel,path,ip,date from rsync_isdn_status where date = '$date' and length(date)=12 and att = 2 and flag = 1";
    $stmt=$conn_PROG->prepare($sql);
    $stmt->execute();
    while (my ($channel,$path,$ip,$date)=$stmt->fetchrow_array)	
    {
        push @CHANNEL_LISTS, $channel;
        $path = $LOG_PATH . $path;
        if($debug > 6){
            print "$channel,$path,$ip,$date\n";
        }
        $CONFIG_LOGFILE{$channel}{'hourFiles'}{$ip}=$path;
    }

    @CHANNEL_LISTS = keys  %LOGFILE_CONFIG_COOKIE;

    message("数据库执行完毕");
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
    # 运算逻辑，之所以分开，是为了中间可以进行一下sync
#    rsyncLog();
    router();
    if($debug > 7){
        print Dumper(\%CONFIG_LOGFILE);
        print Dumper(\%RSYNC_CONFIG_LOGFILE);
    }

}

# 测试期间，不开sync
#message("获取日志");
#rsyncLog() if($HALF_HOUR_HOUR eq '');#带参数重跑时不重新取日志
#rsyncLog();#带参数重跑时不重新取日志

=pod
控制器
=cut
sub router{
    foreach my $channel (keys %LOGFILE_CONFIG_COOKIE){
        single_channel($channel);
    }
}


=pod 
处理单个域名
=cut
sub single_channel{
    # 没有传递频道参数的，直接返回
    if(@_ < 1){
        return 0;
    }
    my ($channel_name) = @_;
    my %rank_array = ("wapcms"  => 1, "book.prog.3g.sina.com.cn" => 1);
    #1. 读取文件
    read_single_channel($channel_name);
    if(exists($rank_array{$channel_name})){
        # 将需要的排行榜运算出来
        create_domain_ranks($SINA_PERL_TOPS_SCRIPTS{$channel_name}, $channel_name, $SINA_PERL_TOPS_TMPS{$channel_name});
    }
    create_sub_isdn($channel_name);
    #2. 进行子运算
}

message("创建排行榜日志");
sub create_domain_ranks{
    my($script_name, $domain_name, $log_file);
    ($script_name, $domain_name, $log_file) = @_;
    print "PARAM:" . $script_name . $domain_name . $log_file;
    if(!$script_name || !$domain_name || !$log_file){
        0;
    }
    my $file = "/tmp/${domain_name}_${HALF_HOUR_YEAR}_${HALF_HOUR_MON}_${HALF_HOUR_DAY}_${HALF_HOUR_HOUR}";
    open(F,"+>$file") or warn $!;   
    foreach my $line (@{$ALL_ISDN{$domain_name}})
    {
        print F "@$line\n"; 
    }
    close(F) or warn $!;sleep 1;
    my $perl_cmd = "${PERL_BASE} ${script_name} ${domain_name}  ${HALF_HOUR_YEAR} ${HALF_HOUR_MON} ${HALF_HOUR_DAY} ${HALF_HOUR_HOUR} >> ${log_file} 2>&1 &";
    if($debug > 5){
        print "PERL CMD:" . $perl_cmd . "\n";
    }
    system("${SINA_PERL_BASE} ${script_name} ${domain_name}  ${HALF_HOUR_YEAR} ${HALF_HOUR_MON} ${HALF_HOUR_DAY} ${HALF_HOUR_HOUR} >> ${log_file} 2>&1 &");
}

sub read_single_channel{
    if(@_ < 1){
        return 0;
    }
    my ($domain_name) = @_;
    my $hourFiles_ref=$CONFIG_LOGFILE{$domain_name}{'hourFiles'};
    my @line=();
    foreach my $hourFile (values %$hourFiles_ref)
    {
        if(-e $hourFile)
        {
            if($debug > 5){
                print "Now Open HOUR FILE:" . $hourFile;
            }
            open FILE , "<$hourFile" ;
            my %subdomains = map qw(wapcms dpool book_pibao_3g_sina_com_cn nba.prog.3g.sina.com.cn nba2.prog.3g.sina.com.cn sinatv.sina.com.cn book.prog.3g.sina.com.cn stock.prog.sina.com.cn);
            $pubsysb = 0;
            if(exists($subdomains{$LOGFILE_CONFIG_COOKIE{$domain_name}{'pubsys'}})){
                $pubsys = 1;
            }
            while(<FILE>){
                my ($time,$mobile,$request,$ua,$ip)={};
                if($pubsys eq 1){
                    ($time,$mobile,$request,$ua,$ip) = parseIsdnCookie($_);
                }else{
                    ($time,$mobile,$request,$ua,$ip) = parseIsdn($_)
                }
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
同步远程文件
=cut
sub rsyncLog()
{
    foreach my $domain_name (keys %RSYNC_CONFIG_LOGFILE)
    {
        my $remoteFiles_ref = $RSYNC_CONFIG_LOGFILE{$domain_name}{'remoteFiles'};

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

        }
    }
}

message("精简日志");
sub create_sub_isdn{
    my ($domain_name) = @_
    if(!$domain_name){
        return 0;
    }
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
            $r =~s/[\?&]gsid=[^&]*//g;
            $r =~s/[\?&]from=gd[^&]*//g;
            $r =~s/[\?&]wm=[^&]*//g;

            $r =~s/pos=text_old/pos=6/;

            $r =~s/[\?&]MISC_ID=[^&]*//g;
            $r =~s/&MISC_SessionID=[^&]*//g;
            $r =~s/&MISC_AccessMode=[^&]*//g;
            $r =~s/&MISC_ServiceID=[^&]*//g;
            $r =~s/&PHPSESSID=[^&]*//g;
            $r =~s/\/&/\/\?/g;
            $r = $LONG_DOMAIN_TO_SHORT{$domain_name}.$r if(defined $LONG_DOMAIN_TO_SHORT{$domain_name} && $r !~ /^\/3g*/);
            $SUB_ISDN{$r}++;
        }
    }
}
message("过滤网管读取日志");
#昌平F5ip都先认为合法
$GATEWAY{'221.179.217.230'}=1;
$GATEWAY{'221.179.217.231'}=1;
$GATEWAY{'221.179.217.20'}=1;
$GATEWAY{'221.179.217.21'}=1;

=pod
将需要ALL_ISDN的几个部分，结合起来做
包括：
创建排行榜日志
创建读书产品排行榜日志
创建SUB_ISDN
任务点击量分析

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

=cut


message("首页点击量分析");
sub count_index{
    my %url_num=();
    my $addr_pos=requestURL("http://wapcms.pub.sina.com.cn/ms/wap_home_page/block_view.php");
    print "固定连接配置\n";
    print $addr_pos;
    print "固定连接配置\n";
    my @url_postfix = ("pos=1", "pos=100", "pos=101", "pos=200");
    foreach my $line (split(/\n/,$addr_pos))
    {
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
        $prefix = ($url =~ m/\/[^\?\&\/]*$/) ? "?" : "&";
        #if($url =~ m/\/[^\?\&\/]*$/){$url .='?pos=1';$url2 .='?pos=100';$url3 .='?pos=101';$url4 .='?pos=200';}
        #else{$url .='&pos=1';$url2 .='&pos=100';$url3 .='&pos=101';$url4 .='&pos=200';}
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


    my $addr_pos=requestURL("http://wapcms.pub.sina.com.cn/proc/channel_recommend/check_all.php");
    foreach my $line (split(/\<br\>/,$addr_pos))
    {
        $line =~ m|<a href=\"(.*)?\">(.*)?</a>|;
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


=pod
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
        $stmt->execute($url,$title,$pv) or message("insert failed:$sql".$DBI::errstr);
    }

    my $sql_complete = "replace RSS_HALF_HOUR_PROG_STATUS set time = '$visit_day $visit_hour',table_name = 'SINA3G_INDEXPAGE_TOP_URLS_HOUR_STAT',ip = '221.179.175.133';";
    $conn->do($sql_complete);

    $conn->disconnect;
=cut
}
