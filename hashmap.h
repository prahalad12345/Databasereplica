#include "MurmurHash.h"
#include <stdlib.h>
#include <cstdint>
#include <climits>
#include <iostream>
#include <array>

#ifndef HASHMAP_H
#define HASHMAP_H

using namespace std;

class Hashtable{
private:
    pair<int,int> *table;
public:
    unsigned long _size;
    unsigned long _elts;
    pair<int,int> empty=make_pair(INT_MIN,INT_MIN);

    Hashtable(unsigned long size):_size(2*size),_elts(0){
        table=new pair<int,int>[_size]();
        fill(table,table+_size,empty);
    }

    ~Hashtable(){
        delete []table;
    }

    unsigned long hashfunc(int key){
        array<int,2> hashvalue;
        MurmurHash3_x64_128(&key,4,0,hashvalue.data());
        return hashvalue[0]%_size;
    }

    void resize(){
        _size*=2;
        auto newtable=new pair<int,int>[_size]();
        fill(newtable,newtable+_size,empty);

        for(unsigned long i=0;i<_size/2;i++){
            if(table[i]!=empty){
                unsigned long newhash=hashfunc(table[i].first);
                for(int j=0;;j++){
                    if(newtable[(newhash+j)%_size]==empty){
                        newtable[(newhash+j)%_size]=table[i];
                    }
                }
            }
        }
        delete [] table;
        table=newtable;
    }

    bool get(int &key,int &value){
        unsigned long hashvalue=hashfunc(key);
        for(int i=0;;i++){
            if(table[(hashvalue+i)%_size]==empty){
                return false;
            }
            else if(table[(hashvalue+i)%_size].first==key){
                value=table[(hashvalue+i)%_size].second;
                return true;
            }
        }
        return false;
    }

    void put(int &key,int &value){
        if(_elts*2>_size){
            resize();
        }
        unsigned long hashvalue=hashfunc(key);
        for(unsigned long i=0;;i++){
            if(table[(hashvalue+i)%_size]==empty){
                table[(hashvalue+i)%_size].first=key;
                table[(hashvalue+i)%_size].second=value;
                ++_elts;
                return;
            }
            else if(table[(hashvalue+i)%_size].first==key){
                table[(hashvalue+i)%_size].second=value;
                return;
            }
        }
    }

    int putifempty(int &key,int &value){
        if(_elts*2>_size)
            resize();
        unsigned long hashvalue=hashfunc(key);
        for(unsigned long i=0;;i++){
            if(table[(hashvalue+i)%_size]==empty){
                table[(hashvalue+i)%_size].first=key;
                table[(hashvalue+i)%_size].second=value;
                ++_elts;
                return (int)NULL;
            }
            else if(table[(hashvalue+i)%_size].first==key){
                return table[(hashvalue+i)%_size].second=value;
            }
        }
    }
};

#endif123
