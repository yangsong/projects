#!/usr/bin/perl -w

package Reducer;

use strict;

sub new{
        my $class = shift();
        my $self = {};
        bless($self, $class);
        return $self;
}

sub run{
        my ($self, $fun, $keynum, $separator) = @_;
        if(!defined $keynum){$keynum=0;}
        if(!defined $separator){$separator=",";}

        $self->{keynum} = $keynum;
        $self->{separator} = $separator;

        my $ret = $self->getline();
        while($ret){
                $self->{key0} = $self->{key};
                $fun->($self->{keyall}, $self->{value});
                while($self->{key0} eq $self->{key}){
                        $ret = $self->getline();
                        if(!$ret){last;}
                }
        }
}

sub nextline{
        my ($self, $key, $value) = @_;
        if($self->getline()){
                if($self->{key0} eq $self->{key}){
                        #return ($self->{keyall},$self->{value});
                        $$key = $self->{keyall};
                        $$value = $self->{value};
                        return 1;
                }else{
                        return 0;
                }
        }else{
                return 0;
        }
}

sub getline{
        my ($self) = @_;
        my $line = <STDIN>;
        if(defined $line){
                chomp($line);

                my $pos = index($line, "\t");
                if($pos!=-1){
                        $self->{keyall} = substr($line, 0, $pos);
                        $self->{value} = substr($line, $pos+1);
                }else{
                        $self->{keyall} = $line;
                        $self->{value} = "";
                }

                my $i=0;
                my $key;

                $pos=-1;

                while($i<$self->{keynum}){
                        $pos = index($self->{keyall}, $self->{separator}, $pos+1);
                        if($pos==-1){last;}
                        $i++;
                }
                if($pos!=-1){
                        $self->{key} = substr($self->{keyall}, 0, $pos);
                }else{
                        $self->{key} = $self->{keyall};
                }

                return 1;
        }else{
                return 0;
        }
}

1;

