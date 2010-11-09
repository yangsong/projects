#!/usr/bin/perl
#===========================================================================
# 视屏平台统计
# lixiang8 20100825
# 说明：按播放、下载、接入方式、格式、频道、专题统计。
#     .fun 统计播放数  (在线播放的视频文件)
#     .fsxs 统计播放请求次数 (在线播放的引导文件)
#     .3gp .mp4 等统计下载次数
#     播放请求和下载可以分频道，在线播放分不了频道，频道号用-1代替
#===========================================================================
BEGIN { push @INC, qw(/data1/sinastat/code/run/rss/) }
BEGIN { push @INC, qw(/data1/sinastat/code/run/lib/) }
require('timelib.pl');
use Data::Dumper();
$GDATE=$ARGV[0];
$GDATE=getYesterday('yyyymmdd') if (!defined($GDATE) or $GDATE eq "");


$hourLast = '';
require ('global.pl');
require ('logfile.conf');

my $oldh = select(STDOUT);
$| = 1;
select($oldh);

message ("PROGRAM media_statistics.pl $GDATE BEGIN ====================");
my @phone = ('13910166705');
#验证三个机房的rawlog是否都入库
my $rawlog_status = check_rawlog_status();
my $maxtimes = 10;
my $sleep_seconds = '600';
my $trytimes = 0;
my $debug = 1;
if(!$debug){
    while ($rawlog_status ne '')
    {
        $trytimes++;
        if($trytimes>10)
        {
            sendSMS_new($rawlog_status,\@phone);
            exit;
        }
        print $rawlog_status," 等待 $trytimes 次\n";
        sleep($sleep_seconds);
        $rawlog_status = check_rawlog_status();
    }
}

my $domain = "sinatv.sina.com.cn";

my %consult=(); #道合计:各频道 播放、下载、用户数
my $total_cid = '-2';#整站合计cid = -2
my %format = (); #格式：预览、3gp、mp4
my %apn = ();    #接入方式：分wap网关、wifi 各分频道的播放请求、播放、下载量和用户数
my %subject = ();#专题量：播放请求、下载、用户数
my %video = ();  #单个视频的量：播放、下载、播放请求
my @vids = ();   #视频id
my %video_c = ();
my %channels_c = (); #频道下的统计

print "读库统计 begin ====",`date`;
my $db_RAWLOG = new db('RAWLOG_SLAVE');
my $conn_RAWLOG = $db_RAWLOG->{'conn'};
my ($stmt,$sql);
my $tablename = $LOGFILE_CONFIG_COOKIE{$domain}{'reqlogtable'}.'_all';
my $sql ="select request,processid,mobile,utype,apn,status from $tablename where (request like '\/tv\/video\/%' or request like '\/tv\/web\/%')";
print $sql,"\n";
$stmt = $conn_RAWLOG->prepare($sql);
$stmt->execute();
while (  my ( $request, $processid, $phone, $utype ,$apn,$status) = $stmt->fetchrow_array ) {
	next if($status !~ /^2/);
	next if ( $request =~ m/\.ucs/ );#ucweb引导页全部不算
	if ( $utype ne 'mobile' && $utype ne 'gsid' && $utype ne 'sm' ) {
		$phone = '-';
	}
	my @fun_startbyte = $processid =~ m/bytes\s{0,1}=\s{0,1}(\d+)-/;
	if ( $fun_startbyte[0] eq '0' || $processid eq '-' ){#针对.fun根据第一段流来统计
		#文件名
		my @url = split( /\//, $request );
		my $filename = $url[-1];
		$filename = $1 if($filename =~ /(.+?)\?/);
		
		# 获取文件id以便从陈天提供的表中查出中文名
		my ($vid) = $filename =~ m/^(\d{1,})_\d{1,2}/;
		next if(!defined($vid) || $vid eq '');
		push @vids, $vid;
			
		#fun,统计播放量
		if($filename =~/\.fun/ && $url[-1] =~ /^[^\?]*\?play/)
		{
			my $cid = -1;#.fun设置cid=-1；
			$consult{$cid}{'play_pv'}++;
			$consult{$cid}{'pv'}++;
			$consult{$total_cid}{'play_pv'}++;
			$consult{$total_cid}{'pv'}++;
			$format{$cid}{'fun'}++;
			$format{$total_cid}{'fun'}++;
			$apn{$cid}{$apn.'_play_pv'}++;
			$apn{$total_cid}{$apn.'_play_pv'}++;
			$video{$vid}{'play_pv'}++;
			$video{$vid}{$apn.'_play_pv'}++;
			$video_c{$cid}{$vid}{'play_pv'}++;
			$video_c{$cid}{$vid}{$apn.'_play_pv'}++;
			if($phone ne '-')
			{
				$consult{$cid}{'play_valid_pv'}++;
				$consult{$cid}{'play_valid_uv'}{$phone} = 1;
				$consult{$cid}{'valid_pv'}++;
				$consult{$cid}{'valid_uv'}{$phone} = 1;
				$consult{$total_cid}{'play_valid_pv'}++;
				$consult{$total_cid}{'play_valid_uv'}{$phone} = 1;
				$consult{$total_cid}{'valid_pv'}++;
				$consult{$total_cid}{'valid_uv'}{$phone} = 1;
				$apn{$cid}{$apn.'_play_valid_pv'}++;
				$apn{$cid}{$apn.'_play_valid_uv'}{$phone} = 1;
				$apn{$total_cid}{$apn.'_play_valid_pv'}++;
				$apn{$total_cid}{$apn.'_play_valid_uv'}{$phone} = 1;
				$video{$vid}{'uv'}{$phone} =1;
				$video{$vid}{'play_valid'}++;
				$video{$vid}{'play_valid_uv'}{$phone} = 1;
			}
		}else#fsxs,3gp,mp4...
		{
            # 新需求统计tid
			my ($cid) = $url[-1] =~ m/cid=(\d*)/;
			my ($sid) = $url[-1] =~ m/sid=(\d*)/;
			my ($tid) = $url[-1] =~ m/tid=(\d*)/;
			my ($did) = $url[-1] =~ m/did=(\d*)/;
            $channels_c{$tid}{'total'}++;
			$video{$vid}{'tid'} = $tid;
			$video{$vid}{'did'} = $did;
			my ($format) = $filename =~/\.(.*)/;
			if($filename =~/play\.fsxs/)#.fsxs统计播放请求量
			{
				$consult{$cid}{'fsxs_play_pv'}++;
				$consult{$total_cid}{'fsxs_play_pv'}++;
				$subject{$sid}{'fsxs_play_pv'}++;
				$apn{$cid}{$apn.'_fsxs_play_pv'}++;
				$apn{$total_cid}{$apn.'_fsxs_play_pv'}++;
				$video{$vid}{'fsxs_play_pv'}++;
				$video{$vid}{$apn.'_fsxs_play_pv'}++;
				$video_c{$cid}{$vid}{'fsxs_play_pv'}++;
				$video_c{$cid}{$vid}{$apn.'_fsxs_play_pv'}++;
                $channels_c{$tid}{'fsxs_play_pv'}++;
				if($phone ne '-')
				{
                    $channels_c{$tid}{'fsxs_play_valid_pv'}++;
					$consult{$cid}{'fsxs_play_valid_pv'}++;
					$consult{$cid}{'fsxs_play_valid_uv'}{$phone} = 1;
					$consult{$total_cid}{'fsxs_play_valid_pv'}++;
					$consult{$total_cid}{'fsxs_play_valid_uv'}{$phone} = 1;
					$subject{$sid}{'fsxs_play_valid_pv'}++;
					$subject{$sid}{'fsxs_play_valid_uv'}{$phone} = 1;
					$subject{$sid}{'valid_uv'}{$phone} = 1;
					$apn{$cid}{$apn.'_fsxs_play_valid_pv'}++;
					$apn{$cid}{$apn.'_fsxs_play_valid_uv'}{$phone} = 1;
					$apn{$total_cid}{$apn.'_fsxs_play_valid_pv'}++;
					$apn{$total_cid}{$apn.'_fsxs_play_valid_uv'}{$phone} = 1;
					$video{$vid}{'uv'}{$phone} =1;
					$video{$vid}{'fsxs_valid'}++;
					$video{$vid}{'fsxs_valid_uv'}{$phone} = 1;
				}
			}elsif($format eq '3gp' || $format eq 'mp4')#.3gp .mp4等统计下载量
			{
				$subject{$sid}{'down_pv'}++;
				$consult{$cid}{'down_pv'}++;
				$consult{$cid}{'pv'}++;
				$consult{$total_cid}{'down_pv'}++;
				$consult{$total_cid}{'pv'}++;
				$apn{$cid}{$apn.'_down_pv'}++;
				$apn{$total_cid}{$apn.'_down_pv'}++;
				$video{$vid}{'down_pv'}++;
				$video{$vid}{$apn.'_down_pv'}++;
				$video_c{$cid}{$vid}{'down_pv'}++;
				$video_c{$cid}{$vid}{$apn.'_down_pv'}++;
                $channels_c{$tid}{'down_pv'}++;
				if($phone ne '-')
				{
                    $channels_c{$tid}{'down_valid_pv'}++;
					$consult{$cid}{'down_valid_pv'}++;
					$consult{$cid}{'down_valid_uv'}{$phone} = 1;
					$consult{$cid}{'valid_pv'}++;
					$consult{$cid}{'valid_uv'}{$phone} = 1;
					$consult{$total_cid}{'down_valid_pv'}++;
					$consult{$total_cid}{'down_valid_uv'}{$phone} = 1;
					$consult{$total_cid}{'valid_pv'}++;
					$consult{$total_cid}{'valid_uv'}{$phone} = 1;
					$subject{$sid}{'down_valid_pv'}++;
					$subject{$sid}{'down_valid_uv'}{$phone} = 1;
					$subject{$sid}{'valid_uv'}{$phone} = 1;
					$apn{$cid}{$apn.'_down_valid_pv'}++;
					$apn{$cid}{$apn.'_down_valid_uv'}{$phone} = 1;
					$apn{$total_cid}{$apn.'_down_valid_pv'}++;
					$apn{$total_cid}{$apn.'_down_valid_uv'}{$phone} = 1;
					$video{$vid}{'uv'}{$phone} =1;
					$video{$vid}{'down_valid'}++;
					$video{$vid}{'down_valid_uv'}{$phone} = 1;
				}
				$format = 'preview' if($filename =~ /_3\.3gp/);
				$format{$cid}{$format}++;
				$format{$total_cid}{$format}++;
			}
		}
	}
}
$stmt->finish;
$conn_RAWLOG->disconnect;
print "读库统计 end ====",`date`;

print '%consult',"\n";
#print Dumper(\%consult);
print '%format',"\n";
#print Dumper(\%format);
print '%apn',"\n";
#print Dumper(\%apn);
print '%subject',"\n";
#print Dumper(\%subject);
print '%video',"\n";
#print Dumper(\%video);

#初始化标题hash
$titles = ();
my $db_video   = new db('video');
my $conn_video = $db_video->{'conn'};
$conn_video->do("set names gbk");
my $vids = join( ",", @vids );
my $sql_gettitle = "select id,title  from video_info where id in ($vids)";
#print $sql_gettitle, "\n";
my $stmt_video = $conn_video->prepare($sql_gettitle);
$stmt_video->execute();
while ( my ( $id,$title ) = $stmt_video->fetchrow_array ) {

	$title = $conn_video->quote($title);    ##转义，很重要，否则入库将出错。
	$title =~ s/^'//g;
	$title =~ s/'$//g;

	$titles{$id} = $title;
	#print "查询标题结果 $id,$title\n";
}
$stmt_video->finish;
$conn_video->disconnect;

#入库处理
my $db_wap   = new db('wap_slave');
my $conn_wap = $db_wap->{'conn'};
$conn_wap->do("set names latin1");
##delete video_consult
    my $tablename = 'video_consult';
if(!$debug){
 $table_name = 'video_consult_test';
}
$conn_wap->do("delete from $table_name where visit_day='$GDATE'");
##insert video_consult
foreach my $cid ( keys %consult ) {
	my $valid_uv = scalar keys %{$consult{$cid}{'valid_uv'}};
	my $play_valid_uv = scalar keys %{$consult{$cid}{'play_valid_uv'}};
	my $fsxs_play_valid_uv = scalar keys %{$consult{$cid}{'fsxs_play_valid_uv'}};
	my $down_valid_uv = scalar keys %{$consult{$cid}{'down_valid_uv'}};
    #my $sql = "replace into video_consult set visit_day = '$GDATE', 
	my $sql = "replace into $table_name set visit_day = '$GDATE', 
			cid = '$cid', 
			pv = '$consult{$cid}{'pv'}', 
			valid_pv = '$consult{$cid}{'valid_pv'}', 
			valid_uv = '$valid_uv', 
			play_pv = '$consult{$cid}{'play_pv'}', 
			play_valid_pv = '$consult{$cid}{'play_valid_pv'}', 
			play_valid_uv = '$play_valid_uv', 
			fsxs_play_pv = '$consult{$cid}{'fsxs_play_pv'}', 
			fsxs_play_valid_pv = '$consult{$cid}{'fsxs_play_valid_pv'}', 
			fsxs_play_valid_uv = '$fsxs_play_valid_uv', 
			down_pv = '$consult{$cid}{'down_pv'}', 
			down_valid_pv = '$consult{$cid}{'down_valid_pv'}', 
			down_valid_uv = '$down_valid_uv'";
            if($debug){
                print $sql, "\n";
            }
	$conn_wap->do("$sql");
}

##delete video_format
$table_name = 'video_format';
if($debug){
$table_name = 'video_format_test';
}
$conn_wap->do("delete from $table_name where visit_day='$GDATE'");
##insert video_format
foreach my $cid ( keys %format ) {
	foreach my $format (keys %{$format{$cid}})
	{
        #my $sql = "replace into video_format set visit_day = '$GDATE', 
		my $sql = "replace into $table_name set visit_day = '$GDATE', 
				cid = '$cid', 
				format = '$format', 
				pv = '$format{$cid}{$format}'";
                if($debug){
                    print $sql, "\n";
                }
		$conn_wap->do("$sql");	
	}
}

##delete video_apn
$table_name = 'video_apn';
if($debug){
$table_name = 'video_apn_test';
}
$conn_wap->do("delete from $table_name where visit_day='$GDATE'");
##insert video_apn
foreach my $cid ( keys %apn ) {
	my $wap_play_valid_uv = scalar keys %{$apn{$cid}{'wap_play_valid_uv'}};
	my $wap_fsxs_play_valid_uv = scalar keys %{$apn{$cid}{'wap_fsxs_play_valid_uv'}};
	my $wap_down_valid_uv = scalar keys %{$apn{$cid}{'wap_down_valid_uv'}};
	my $wifi_play_valid_uv = scalar keys %{$apn{$cid}{'wifi_play_valid_uv'}};
	my $wifi_fsxs_play_valid_uv = scalar keys %{$apn{$cid}{'wifi_fsxs_play_valid_uv'}};
	my $wifi_down_valid_uv = scalar keys %{$apn{$cid}{'wifi_down_valid_uv'}};
	my $sql = "replace into $table_name set visit_day = '$GDATE', 
			cid = '$cid', 
			wap_play_pv = '$apn{$cid}{'wap_play_pv'}', 
			wap_play_valid_pv = '$apn{$cid}{'wap_play_valid_pv'}', 
			wap_play_valid_uv = '$wap_play_valid_uv', 
			wap_fsxs_play_pv = '$apn{$cid}{'wap_fsxs_play_pv'}', 
			wap_fsxs_play_valid_pv = '$apn{$cid}{'wap_fsxs_play_valid_pv'}', 
			wap_fsxs_play_valid_uv = '$wap_fsxs_play_valid_uv', 
			wap_down_pv = '$apn{$cid}{'wap_down_pv'}', 
			wap_down_valid_pv = '$apn{$cid}{'wap_down_valid_pv'}', 
			wap_down_valid_uv = '$wap_down_valid_uv',
			wifi_play_pv = '$apn{$cid}{'wifi_play_pv'}', 
			wifi_play_valid_pv = '$apn{$cid}{'wifi_play_valid_pv'}', 
			wifi_play_valid_uv = '$wifi_play_valid_uv', 
			wifi_fsxs_play_pv = '$apn{$cid}{'wifi_fsxs_play_pv'}', 
			wifi_fsxs_play_valid_pv = '$apn{$cid}{'wifi_fsxs_play_valid_pv'}', 
			wifi_fsxs_play_valid_uv = '$wifi_fsxs_play_valid_uv', 
			wifi_down_pv = '$apn{$cid}{'wifi_down_pv'}', 
			wifi_down_valid_pv = '$apn{$cid}{'wifi_down_valid_pv'}', 
			wifi_down_valid_uv = '$wifi_down_valid_uv'";
            if($debug){
                print $sql, "\n";
            }
	$conn_wap->do("$sql");
}

##delete video_subject
$table_name = 'video_subject';
if($debug){
$table_name = 'video_subject_test';
}
$conn_wap->do("delete from $table_name where visit_day='$GDATE'");
##insert video_subject
foreach my $sid ( keys %subject ) {
	my $fsxs_play_valid_uv = scalar keys %{$subject{$sid}{'fsxs_play_valid_uv'}};
	my $down_valid_uv = scalar keys %{$subject{$sid}{'down_valid_uv'}};
	my $valid_uv = scalar keys %{$subject{$sid}{'valid_uv'}};
    #my $sql = "replace into video_subject set visit_day = '$GDATE', 
	my $sql = "replace into $table_name set visit_day = '$GDATE', 
			sid = '$sid', 
			fsxs_play_pv = '$subject{$sid}{'fsxs_play_pv'}', 
			fsxs_play_valid_pv = '$subject{$sid}{'fsxs_play_valid_pv'}', 
			fsxs_play_valid_uv = '$fsxs_play_valid_uv', 
			down_pv = '$subject{$sid}{'down_pv'}', 
			down_valid_pv = '$subject{$sid}{'down_valid_pv'}', 
			down_valid_uv = '$down_valid_uv',
			valid_uv = '$valid_uv'";
	#print $sql, "\n";
	$conn_wap->do("$sql");
}

##delete video_detail
$table_name = 'video_detail';
if($debug){
    $table_name = 'video_detail_test';
}
$conn_wap->do("delete from $table_name where visit_day='$GDATE'");
#delete x-days ago
my $x_days_ago = getNDaysAgo('yyyy-mm-dd',10);
$conn_wap->do("delete from $table_name where visit_day<'$x_days_ago'");
##insert video_detail
foreach my $vid ( keys %video ) {
	my $play_valid_uv = scalar keys %{$video{$vid}{'play_valid_uv'}};
	my $fsxs_play_valid_uv = scalar keys %{$video{$vid}{'fsxs_play_valid_uv'}};
	my $down_valid_uv = scalar keys %{$video{$vid}{'down_valid_uv'}};
	my $valid_uv = scalar keys %{$video{$vid}{'valid_uv'}};
	$video{$vid}{'title'} = $titles{$vid};
	my $sql = "replace into $table_name set visit_day = '$GDATE', 
			vid = '$vid',
			tid = '$video{$vid}{'tid'}',
			did = '$video{$vid}{'did'}',
			title = '$video{$vid}{'title'}',  
			play_pv = '$video{$vid}{'play_pv'}', 
			wap_play_pv = '$video{$vid}{'wap_play_pv'}', 
			wifi_play_pv = '$video{$vid}{'wifi_play_pv'}',  
			play_valid_pv = '$video{$vid}{'play_valid_pv'}', 
			play_valid_uv = '$play_valid_uv', 
			fsxs_play_pv = '$video{$vid}{'fsxs_play_pv'}', 
			wap_fsxs_play_pv = '$video{$vid}{'wap_fsxs_play_pv'}', 
			wifi_fsxs_play_pv = '$video{$vid}{'wifi_fsxs_play_pv'}',  
			fsxs_play_valid_pv = '$video{$vid}{'fsxs_play_valid_pv'}', 
			fsxs_play_valid_uv = '$fsxs_play_valid_uv', 
			down_pv = '$video{$vid}{'down_pv'}', 
			wap_down_pv = '$video{$vid}{'wap_down_pv'}', 
			wifi_down_pv = '$video{$vid}{'wifi_down_pv'}',  
			down_valid_pv = '$video{$vid}{'down_valid_pv'}', 
			down_valid_uv = '$down_valid_uv',
			valid_uv = '$valid_uv'";
            if($debug){
                print $sql, "\n";
            }
	$conn_wap->do("$sql");
}
#排行
my %video_rank = ();
#播放排行
foreach my $cid(keys %video_c)
{
	my $i=0;
	foreach my $vid (sort{$video_c{$cid}{$b}{'play_pv'} <=> $video_c{$cid}{$a}{'play_pv'}} keys %{$video_c{$cid}})
	{
		$i++;last if($i>20);
		$video_rank{$cid}{$vid} = $video_c{$cid}{$vid};
		$video_rank{$cid}{$vid}{'tid'} = $video{$vid}{'tid'};
		$video_rank{$cid}{$vid}{'did'} = $video{$vid}{'did'};
		$video_rank{$cid}{$vid}{'title'} = $video{$vid}{'title'};
	}
}
#fsxs排行
foreach my $cid(keys %video_c)
{
	my $i=0;
	foreach my $vid (sort{$video_c{$cid}{$b}{'fsxs_play_pv'} <=> $video_c{$cid}{$a}{'fsxs_play_pv'}} keys %{$video_c{$cid}})
	{
		$i++;last if($i>20);
		$video_rank{$cid}{$vid} = $video_c{$cid}{$vid};
		$video_rank{$cid}{$vid}{'tid'} = $video{$vid}{'tid'};
		$video_rank{$cid}{$vid}{'did'} = $video{$vid}{'did'};
		$video_rank{$cid}{$vid}{'title'} = $video{$vid}{'title'};
	}
}
#下载排行
foreach my $cid(keys %video_c)
{
	my $i=0;
	foreach my $vid (sort{$video_c{$cid}{$b}{'down_pv'} <=> $video_c{$cid}{$a}{'down_pv'}} keys %{$video_c{$cid}})
	{
		$i++;last if($i>20);
		$video_rank{$cid}{$vid} = $video_c{$cid}{$vid};
		$video_rank{$cid}{$vid}{'tid'} = $video{$vid}{'tid'};
		$video_rank{$cid}{$vid}{'did'} = $video{$vid}{'did'};
		$video_rank{$cid}{$vid}{'title'} = $video{$vid}{'title'};
	}
}
print '%video_rank',"\n";
#print Dumper(\%video_rank);
##delete video_rank
$conn_wap->do("delete from video_rank where visit_day='$GDATE'");
##insert video_rank
foreach my $cid (keys %video_rank)
{
	foreach my $vid ( keys %{$video_rank{$cid}}) {
		my $sql = "replace into video_rank set visit_day = '$GDATE', 
				cid = '$cid',
				vid = '$vid',
				tid = '$video_rank{$cid}{$vid}{'tid'}',
				did = '$video_rank{$cid}{$vid}{'did'}',
				title = '$video_rank{$cid}{$vid}{'title'}',  
				play_pv = '$video_rank{$cid}{$vid}{'play_pv'}', 
				wap_play_pv = '$video_rank{$cid}{$vid}{'wap_play_pv'}', 
				wifi_play_pv = '$video_rank{$cid}{$vid}{'wifi_play_pv'}',  
				fsxs_play_pv = '$video_rank{$cid}{$vid}{'fsxs_play_pv'}', 
				wap_fsxs_play_pv = '$video_rank{$cid}{$vid}{'wap_fsxs_play_pv'}', 
				wifi_fsxs_play_pv = '$video_rank{$cid}{$vid}{'wifi_fsxs_play_pv'}',  
				down_pv = '$video_rank{$cid}{$vid}{'down_pv'}', 
				wap_down_pv = '$video_rank{$cid}{$vid}{'wap_down_pv'}', 
				wifi_down_pv = '$video_rank{$cid}{$vid}{'wifi_down_pv'}'";
		#print $sql, "\n";
		$conn_wap->do("$sql");
	}
}
$conn_wap->disconnect;
exit;
sub check_rawlog_status()
{
	my %sources = ('ft'=>'174',
	'gd'=>'42',
	'xd'=>'253');
	my %rawlog_status = ();
	my $db_wap   = new db('wap_slave');
	my $conn_wap = $db_wap->{'conn'};
	my ($stmt_status,$sql_status);
	$sql_status = "select visit_day,source from video_rawlog_status where visit_day='$GDATE'";
	print $sql_status,"\n";
	$stmt_status = $conn_wap->prepare($sql_status);
	$stmt_status->execute();
	while (  my ( $visit_day,$source) = $stmt_status->fetchrow_array ) {
		$rawlog_status{$source} = 1;
	}
	foreach my $source(keys %sources)
	{
		return "no $source - $source{$source} sinatv.sina.com.cn rawlog" if(!$rawlog_status{$source});
	}
	$conn_wap->disconnect;
	return '';
}
