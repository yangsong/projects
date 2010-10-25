
#!/usr/bin/perl -w

package Mapper;

use strict;

sub new{
        my $class=shift;
        my $self={};
        bless ($self,$class);
        return $self;
}

sub run{
        my ($self, $fun, $keynum) = @_;
        if(!defined $keynum){$keynum=1};
        while(<STDIN>){
                chomp;

                my $key;
                my $value;

                my $i=0;
                my $pos = -1;
                while($i<$keynum){
                        $pos = index($_, "\t", $pos+1);
                        if($pos==-1){last;}
                        $i++;
                }

                if($pos!=-1){
                        $key = substr($_, 0, $pos);
                        $value = substr($_, $pos+1);
                }else{
                        if($keynum==0){
                                $key = "";
                                $value = $_;
                        }else{
                                $key = $_;
                                $value = "";
                        }
                }
                $fun->($key, $value);
        }
}

1;

