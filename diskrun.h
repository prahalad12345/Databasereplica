#ifndef diskRun_h
#define diskRun_h
#include <vector>
#include <cstdint>
#include <cstring>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <cassert>
#include <algorithm>
#include "bloom.h"

using namespace std;

class Disklevel;

class Diskrun{
    friend class Disklevel;
public:
    pair<string,string> *map;
    int fd;
    int pagesize;
    Bloomfilter bf;
    long _capacity;
    string _filename;
    int _level;
    vector<string> fencepointer;
    unsigned _imaxfp;
    unsigned _runid;
    double _bf_fp;

    string minkey = "a";
    string maxkey = "zzzzzzzzzzzzzzzzzzzzzzzzz";

    Diskrun(long capacity,int pagesize,int level,int runid,double bf_fp):_capacity(capacity),_level(level),_imaxfp(0),pagesize(pagesize),_runid(runid),_bf_fp(bf_fp),bf(capacity,bf_fp){
        _filename="C_"+to_string(level)+"_"+to_string(runid)+".txt";
        size_t filesize=capacity*sizeof(pair<string,string>);
        long result;
        fd=open(_filename.c_str(),O_RDWR|O_CREAT|O_TRUNC,(mode_t)0600);
        if(fd==-1){
            perror("ERROR OPENING FILE FOR WRITING");
            exit(EXIT_FAILURE);
        }
        result = lseek(fd, filesize - 1, SEEK_SET);
        if(result==-1){
            close(fd);
            perror("ERROR CALLING LSEEK TO STRETCH THE FILE");
            exit(EXIT_FAILURE);
        }

        result=write(fd,"",1);
        if(result!=1){
            close(fd);
            perror("Error writing last byte of the file");
            exit(EXIT_FAILURE);
        }

        map=(pair<string,string>*) mmap(0,filesize,PROT_READ|PROT_WRITE,MAP_SHARED,fd,0);
        if(map==MAP_FAILED){
            close(fd);
            perror("ERROR mapping the file");
            exit(EXIT_FAILURE);
        }
    }
    ~Diskrun(){
        fsync(fd);
        dounmap();

        if(remove(_filename.c_str())){
            perror(("ERRRP REMOVING FILE "+string(_filename)).c_str());
        }
    }

    int compute_hash(string s) {
        const int p = 31;
        const int m = 1e9 + 9;
        long long hash_value = 0;
        long long p_pow = 1;
        for (char c : s) {
            if(c>=97)
                (hash_value += ( (c - 'a' + 1) * p_pow) % m)%=m;
            else if(c>=65)
                (hash_value += ((c - 'A' + 1) * p_pow) % m)%=m;
            p_pow = (p_pow * p) % m;
        }
        return hash_value;
    }

    void setcapacity(long newcap){
        _capacity=newcap;
    }

    long getcapacity(){
        return _capacity;
    }

    void writedata(pair<string,string> *run,size_t offset,long len){
        memcpy(map+offset,run,len*sizeof(pair<string,string>));
        _capacity=len;
    }

    void constructindex(){
        fencepointer.reserve(_capacity/pagesize);
        _imaxfp=-1;
        int num=compute_hash(map[j].first)
        for(int j=0;j<_capacity;j++){
            if(j%pagesize==0){
                bf.add((int*)&num,sizeof(int));
                _imaxfp++;
            }
        }
        if(_imaxfp>=0){
            fencepointer.resize(_imaxfp+1);
        }
        minkey=map[0].first;
        maxkey=map[_capacity-1].first;
    }

    long binary_search(long offset,long n,string key,bool &found){
        if(n==0){
            found=true;
            return offset;
        }
        long min=offset,max=offset+n-1;
        long middle=(min+max)/2;
        while(min<=max){
            if(key>map[middle].first)
                min=middle+1;
            else if(key==map[middle].first){
                found=true;
                return middle;
            }
            else    
                max=middle-1;
            middle=(min+max)>>1;
        }
        return min;
    }

    void get_flanking_fp(string key,long &start,long &end){
        if(_imaxfp==0){
            start=0;
            end=_capacity;
        }
        else if(key<fencepointer[1]){
            start=0;
            end=pagesize;
        }
        else if(key>=fencepointer[_imaxfp]){
            start=_imaxfp*pagesize;
            end=_capacity;
        }
        else{
            unsigned min=0,max=_imaxfp;

            while(min<max){
                unsigned middle=(min+max)>>1;
                if(key>fencepointer[middle]){
                    if(key<fencepointer[middle+1]){
                        start=middle*pagesize;
                        end=(middle+1)*pagesize;
                        return;
                    }
                }
                else if(key<fencepointer[middle]){
                    if(key>=fencepointer[middle-1]){
                        start=(middle-1)*pagesize;
                        end=(middle)*pagesize;
                        return;
                    }
                    max=middle-1;
                }
                else{
                    start=middle*pagesize;
                    end=start;
                    return;
                }
            }
        }
    }

    long get_index(string key,bool &found){
        long start,end;
        get_flanking_fp(key,start,end);
        long ret=binary_search(start,end-start,key,found);
        return ret;
    }

    string lookup(string key,bool &found){
        long idx=get_index(key,found);
        string ret=map[idx].second;
        return found?ret:"NULL";
    }

    void range(string key1,string key2,long &i1,long &i2){
        i1=0;
        i2=0;
        if(key1>maxkey || key2<minkey){
            return;
        }
        if(key1>=minkey){
            bool found=false;
            i1=get_index(key1,found);
        }
        if(key2>maxkey){
            i2=_capacity;
            return;
        }
        else{
            bool found=false;
            i2=get_index(key2,found);
        }
    }

    void printelts(){
        for(int j=0;j<_capacity;j++){
            cout<<map[j].first<<" ";
        }
        cout<<endl;
    }

    void domap(){
        size_t filesize=_capacity*sizeof(pair<string,string>);
        fd=open(_filename.c_str(),O_RDWR|O_CREAT|O_TRUNC,(mode_t)0600);
        if(fd==-1){
            perror("ERROR OPENING FILE FOR WRITING");
            exit(EXIT_FAILURE);
        }
        map=(pair<string,string>*)mmap(0,filesize,PROT_READ|PROT_WRITE,MAP_SHARED,fd,0);
        if(map==MAP_FAILED){
            close(fd);
            perror("ERROR MMAPING THE FILE");
            exit(EXIT_FAILURE);
        }
    }

    void dounmap(){
        size_t filesize=_capacity*sizeof(pair<string,string>);

        if(munmap(map,filesize)==-1){
            perror("ERROR unmapping the file");
        }
        close(fd);
        fd=-5;
    }

    void doublesize(){
        long newcapacity=_capacity*2;

        size_t newfilesize=newcapacity*sizeof(pair<string,string>);
        int result=lseek(fd,newfilesize-1,SEEK_SET);
        if(result==-1){
            close(fd);
            perror("Error calling lseek() to stretch file");
            exit(EXIT_FAILURE);
        }
        result=write(fd,"",1);
        if(result!=1){
            close(fd);
            perror("Error writing last byte of the file");
            exit(EXIT_FAILURE);
        }
        map=(pair<string,string>*)mmap(0,newfilesize,PROT_READ|PROT_WRITE,MAP_SHARED,fd,0);
        if(map==MAP_FAILED){
            close(fd);
            perror("ERROR MMAPING THE FILE");
            exit(EXIT_FAILURE);
        }
        _capacity=newcapacity;
    }
};

#endif