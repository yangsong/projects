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
$| = 1;
select($oldh);
