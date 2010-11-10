#=================================================================
# 摘要: 获取天气通日志并分析
#===========================================================================
BEGIN { push @INC, qw(/data1/sinastat/code/run/rss/) }
BEGIN { push @INC, qw(/data1/sinastat/code/run/lib/) }
require('timelib.pl');
$GDATE=$ARGV[0];
$GDATE=getYesterday('yyyymmdd') if (!defined($GDATE) or $GDATE eq "");
$year=substr($GDATE,0,4);
$month=substr($GDATE,4,2);
$day=substr($GDATE,6,2);
require('global.pl');
$PROGRAM=__FILE__;
use vars qw /$GDATE $PROGRAM/;

my $oldh = select(STDOUT);
$| = 1;
select($oldh);

my $dir = '/data0/data1/sinastat/var/isdn/TIME/tianqitong/';
my $activityfiles = '';#activity文件列表
my $registryfiles = '';#register文件列表
my %activity = ();
my %registry = ();
my %pathhits = ();
my $debug = 1;
my %devices_c = ();
my %cities_c = ();
my %versions_c = ();
my %ips_c = ();


my $mailto="lixiang8\@staff.sina.com.cn,wenyue\@staff.sina.com.cn";
my $remote_activityfiles = "access$GDATE.log";
my $activityfiles = $dir."tianqitong_activity_$GDATE.log";
my $remote_registryfiles = "access$GDATE.zip";
my $registryfiles = $dir."tianqitong_registry_$GDATE.zip";
if($debug){
    $registryfiles = $dir ."/registry/" . "access$GDATE.log";
}

if(!$debug){
    use Net::FTP;
    my $username = 'httplog';
    my $password = 'mylog@6161';
    print "获取天气通activity日志 begin ====",`date`;
    $ftp = Net::FTP->new
    (
        "10.71.16.107",
        Port => '6021',
        Timeout => 30,
        Passive=>0,
        Debug => 1,
    ) or die "Could not connect.\n", $ftp->message;
    $ftp->login($username,$password) or die "Could not login.\n" , $ftp->message;
    $ftp->binary;
    $ftp->get($remote_activityfiles,$activityfiles) or die "Could not get remotefile:$remote_activityfiles\n" , $ftp->message; 
    $ftp->quit;
    print "获取天气通registry日志 begin ====",`date`;
    $ftp = Net::FTP->new
    (
        "221.179.175.156",
        Port => '6021',
        Timeout => 30,
        Passive=>0,
        Debug => 1,
    ) or die "Could not connect.\n", $ftp->message;
    $ftp->login($username,$password) or die "Could not login.\n" , $ftp->message;
    $ftp->binary;
    $ftp->get($remote_registryfiles,$registryfiles) or die "Could not get remotefile:$remote_registryfiles\n" , $ftp->message; 
    $ftp->quit;
    my $registrydir = $dir."registry/";
    my $unzipcmd = "unzip -o $registryfiles -d $registrydir";
    print $unzipcmd,"\n";
    die "解压失败：$unzipcmd\n" if (system($unzipcmd));
    $registryfiles = $registrydir."access$GDATE.log";
    print "获取天气通日志 end ====",`date`;
}
print $activityfiles . "\n";
print $registryfiles . "\n";
#activity
open(ACT,"cat $activityfiles|") or die "can not open cat $activityfiles:$!";
while(<ACT>)
{
	#next if($_ !~ /report\d*\.php/);
	my ($ip,undef,undef,$time,undef,undef,$request) = split(/ /);
	next if($request !~ /\/report[^.]*\.php/);
	($time) = $time =~ /:(\d+:\d+:\d+)/;
	my ($uid) = $request =~ /uid=([^&]*)/;	#用户手机IMEI码
	next if(!defined($uid) || $uid eq '');
	my ($device) = $request =~ /device=([^&]*)/;#手机型号，例如“N73”
	my ($version) = $request =~ /version=([^&]*)/;#天气通版本号
	my ($city) = $request =~ /city=([^&]*)/;	#用户关注的城市（城市代码，多个城市用下划线连接，例如WMXX2925_CHXX0133_WMXX1037_)
	my ($counter) = $request =~ /counter=([^&]*)/;#用户当前的广告版本号，数字，例如72
	my ($adv) = $request =~ /adv=([^&]*)/;	#同counter，用户当前的广告版本号，数字，例如72（可选参数，旧版的客户端未上报此参数）
	my ($conf) = $request =~ /conf=([^&]*)/;	#用户配置信息（可选参数，旧版的客户端未上报此参数）
    # device
    $devices_c{lc($device)}++;
    # version
    $versions_c{lc($version)}++;
    # city
    foreach my $c (split(/_/, $city)){
        $cities_c{lc($c)}++;
    }
    $ips_c{$ip}++;

	$activity{$uid}{'device'} = $device;
	$activity{$uid}{'version'} = $version;
	$activity{$uid}{'city'} = $city;
	$activity{$uid}{'counter'} = $counter;
	$activity{$uid}{'adv'} = $adv;
	$activity{$uid}{'conf'} = $conf;
	$activity{$uid}{'ip'} = $ip;
	$activity{$uid}{'time'} = $time;
}
close(ACT);

#registry & adv
open(REG,"cat $registryfiles|") or die "can not open cat $registryfiles:$!";
while(<REG>)
{
	my ($ip,undef,undef,$time,undef,undef,$request) = split(/ /);
	my ($path) = $request =~ /^([^?]*)/;
	my ($uid) = $request =~ /uid=([^&]*)/;
	$pathhits{$path}{'pv'}++;
	$pathhits{$path}{'users'}{$uid} = 1 if(defined($uid) && $uid ne '');
	
	if($path eq '/app/register.php' && defined($uid) && $uid ne '')
	{
		($time) = $time =~ /:(\d+:\d+:\d+)/;
		my ($device) = $request =~ /device=([^&]*)/;#手机型号，例如“N73”
		my ($pver) = $request =~ /pver=([^&]*)/;#产品版本号（区分天气通版本，以前有移动市场版、移动梦网版等收费版本）
		my ($pid) = $request =~ /pid=([^&]*)/;	#渠道编号，基本无规律，现在有qihoo这种纯字母的，也有P150这种字母+数字的形式。
		$registry{$uid}{'device'} = $device;
		$registry{$uid}{'pver'} = $pver;
		$registry{$uid}{'pid'} = $pid;
		$registry{$uid}{'ip'} = $ip;
		$registry{$uid}{'time'} = $time;
	}
}
close(REG);

#统计数据入库
print "写库 begin ====",`date`;
my $db_PROG = new db('TIANQITONG_MASTER');
my $conn_PROG = $db_PROG->{'conn'};
my ($stmt,$sql);

my %exists_user = ();
$sql = "select uid from user_dim where date < '$GDATE'";
print $sql,"\n";
my $stmt=$conn_PROG->prepare($sql);
$stmt->execute();
while (my ($uid)=$stmt->fetchrow_array)
{
	$exists_user{$uid} = 1;
}

my %newuser = ();
my $newusernum = 0;
if($debug){
    # debug
    # version input
    # clear the old ones
    my $sql = "delete from ip_stat where date='$GDATE';";
    $conn_PROG->do($sql);
    my $index = 0;
    foreach my $ip(keys %ips_c){
        if($index % 500 == 0){
        $sql = substr($sql, 0, -1);
        $sql .= ";";
        print "$sql\n";
            $conn_PROG->do($sql);
            $sql = "insert into ip_stat (`date`, `type`, `num`) values";
        }
        $sql .= "('$GDATE', '$ip', '$ips_c{$ip}'),";
        $index++;
    }
    print $sql;
    if(($index - 1) % 500 != 0){
        $sql = substr($sql, 0, -1);
        $sql .= ";";
        print "$sql\n";
        $conn_PROG->do($sql);
    }
    undef(%ips_c);

    # load mobile
    $sql = "delete from device_stat where date='$GDATE';";
    $conn_PROG->do($sql);
    $index = 0;
    foreach my $device(keys %devices_c){
        if($index % 500 == 0){
        $sql = substr($sql, 0, -1);
        $sql .= ";";
        print "$sql\n";
            $conn_PROG->do($sql);
            $sql = "insert into device_stat (`date`, `type`, `num`) values";
        }
        $sql .= "('$GDATE', '$device', '$devices_c{$device}'),";
        $index++;
    }
    if(($index - 1) % 500 != 0){
        $sql = substr($sql, 0, -1);
        $sql .= ";";
        print "$sql\n";
        $conn_PROG->do($sql);
    }
    print $sql;
    undef(%devices_c);

    # load city
    $sql = "delete from city_stat where date='$GDATE';";
    $conn_PROG->do($sql);
    $index = 0;
    foreach my $city(keys %cities_c){
        if($index % 500 == 0){
            $sql = substr($sql, 0, -1);
            $sql .= ";";
            print "$sql\n";
            $conn_PROG->do($sql);
            $sql = "insert into city_stat (`date`, `type`, `num`) values";
        }
        $sql .= "('$GDATE', '$city', '$cities_c{$city}'),";
        $index++;
    }
    if(($index - 1) % 500 != 0){
        $sql = substr($sql, 0, -1);
        $sql .= ";";
        print "$sql\n";
        $conn_PROG->do($sql);
    }
    print $sql;
    undef(%cities_c);
}else{
foreach my $uid (keys %activity) {
	#行为日志
	$sql = "replace into user_act_fact_month set date='$GDATE',uid='$uid',device='$activity{$uid}{'device'}',version='$activity{$uid}{'version'}',city='$activity{$uid}{'city'}',counter='$activity{$uid}{'counter'}',adv='$activity{$uid}{'adv'}',conf='$activity{$uid}{'conf'}',ip='$activity{$uid}{'ip'}',time='$activity{$uid}{'time'}'";
	print $sql,"\n";
	$conn_PROG->do($sql);
	if(!$exists_user{$uid})
	{
		#新增用户
		$sql = "replace into user_dim set date='$GDATE',uid='$uid',device='$activity{$uid}{'device'}',ip='$activity{$uid}{'ip'}',time='$activity{$uid}{'time'}'";
		print $sql,"\n";
		$conn_PROG->do($sql);
		$newuser{$uid} = 1;
	}
}
#天活跃用户数
my $usernum = keys %activity;
$sql = "replace into active_users_num set date='$GDATE',type='day',num='$usernum'";
print $sql,"\n";
$conn_PROG->do($sql);
undef(%activity);

#渠道注册量
#此处需要修改，1、表切回user_dim，2、需把渠道注册的量也算到新增里，因为渠道注册的用户可能没有行为日志。
foreach my $uid (keys %registry) {
	$sql = "replace into user_dim_p set date='$GDATE',uid='$uid',device='$registry{$uid}{'device'}',pver='$registry{$uid}{'pver'}',pid='$registry{$uid}{'pid'}',ip='$registry{$uid}{'ip'}',time='$registry{$uid}{'time'}'";
	print $sql,"\n";
	$conn_PROG->do($sql);
	#$newuser{$uid} = 1;
}
undef(%registry);

#天新增用户数
$newusernum = keys(%newuser);
$sql = "replace into active_users_num set date='$GDATE',type='new',num='$newusernum'";
print $sql,"\n";
$conn_PROG->do($sql);

#总累积用户数
$sql = "select count(*) from user_dim";
print $sql,"\n";
my $stmt=$conn_PROG->prepare($sql);
$stmt->execute();
my ($total_user_num)=$stmt->fetchrow_array;
$sql = "replace into active_users_num set date='$GDATE',type='total',num='$total_user_num'";
print $sql,"\n";
$conn_PROG->do($sql);

#路径点击量
$sql = "delete from url_pv where date='$GDATE'";
$conn_PROG->do($sql);
foreach my $path (keys %pathhits) {
	my $uv = keys %{$pathhits{$path}{'users'}};
	$sql = "insert into url_pv set date='$GDATE',path='$path',pv='$pathhits{$path}{'pv'}',uv='$uv'";
	print $sql,"\n";
	$conn_PROG->do($sql);
}
undef(%pathhits);

#月活跃用户数
my $monthbegin = $year.$month.'01';
my $monthend = $year.$month.'31';
my %monthuids = ();
my $monthuidnum = 0;
$sql = "select uid from user_act_fact_month where date between '$monthbegin' and '$monthend'";
print $sql,"\n";
my $stmt=$conn_PROG->prepare($sql);
$stmt->execute();
while (my ($uid)=$stmt->fetchrow_array)
{
	$monthuidnum++ if(!($monthuids{$uid}));
	$monthuids{$uid} = 1;
}
$sql = "replace into active_users_num set date='$monthbegin',type='month',num='$monthuidnum'";
print $sql,"\n";
$conn_PROG->do($sql);
undef(%monthuids);
}


$conn_PROG->disconnect;
print "写库 end ====",`date`;

