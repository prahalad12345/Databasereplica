#ifndef diskLevel_h
#define diskLevel_h
#include <vector>
#include <cstdint>
#include <string>
#include <cstring>
#include <iostream>
#include "diskrun.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <cassert>
#include <algorithm>

#define LEFTCHILD(x) 2 * x + 1
#define RIGHTCHILD(x) 2 * x + 2
#define PARENT(x) (x - 1) / 2

string TOMBSTONE = "A";

using namespace std;

class Disklevel{
public:
    pair<string,string> kvpairmax;
    pair< pair<string,string> , int> kvintpairmax;
    string v_tombstone=TOMBSTONE;

    struct StaticHeap{
        int size;
        vector< pair< pair<string,string> ,int> > arr;
        pair< pair< string,string > , int> max;

        StaticHeap(unsigned sz,pair pair< pair< string,string > , int> mx){
            size=0;
            arr=vector< pair< pair< string,string > , int> >(sz,mx);
            max=mx;
        }

        void push(pair< pair< string,string > , int> blob){
            unsigned i=size++;
            while(i && blob<arr[PARENT(i)]){
                arr[i]=arr[PARENT(i)];
                i=PARENT(i);
            }
            arr[i]=blob;
        }

        void heapify(int i){
            int smallest=(LEFTCHILD(i)<size && arr[LEFTCHILD(i)]<arr[i])?LEFTCHILD(i):i;
            if(RIGHTCHILD(i)<size && arr[RIGHTCHILD(i)]<arr[smallest]){
                smallest=RIGHTCHILD(i);
            }
            if(smallest!=i){
                pair< pair< string,string > , int> temp=arr[i];
                arr[i]=arr[smallest];
                arr[smallest]=temp;
                heapify(smallest);
            }
        }
        
        pair< pair< string,string > , int> pop(){
            pair< pair< string,string > , int> ret=arr[0];
            arr[0]=arr[--size];
            heapify(0);
            return ret;
        }
    };

    int _level;
    unsigned _pagesize;
    unsigned long _runsize;
    unsigned _activerun;
    unsigned _mergesize;
    unsigned _numruns;
    double _bf_fp;
    vector<Diskrun*> runs;

    Disklevel(int pagesize,int level,unsigned long runsize,unsigned numruns,unsigned mergesize,double bf_fp):_numruns(numruns),_runsize(runsize),_level(level),_pagesize(pagesize),_mergesize(mergesize),_activerun(0),_bf_fp(bf_fp){
        kvpairmax=(pair< string, string >){"zzzzzzzzzzzzzz","a"};
        kvintpairmax=(pair< pair< string,string > , int> ){kvpairmax,-1};

        for(int i=0;i<_numruns;i++){
            Diskrun *run=new Diskrun(_runsize,pagesize,level,i,_bf_fp);
            runs.push_back(run);
        }
    }

    ~Disklevel(){
        for(int i=0;i<runs.size();i++){
            delete runs[i];
        }
    }

    void addruns(vector<Diskrun*> &runlist,long runlen,bool lastlevel){
        StaticHeap h=StaticHeap(runlist.size(),kvintpairmax);
        vector<int> head(runlist.size(),0);
        for(int i=0;i<runlist.size();i++){
            pair<string,string> kvp=runlist[i]->map[0];
            h.push(make_pair(kvp,i));
        }
        int j=-1;
        int lastkey=INT16_MAX;
        unsigned lastk=INT16_MIN;
        while(h.size!=0){
            auto val_run_pair=h.pop();
            assert(val_run_pair!=kvintpairmax);
            if(lastkey==val_run_pair.first.first){
                if(lastk<val_run_pair.second)
                    runs[_activerun]->map[j]=val_run_pair.first;
            }
            else{
                ++j;
                if(j!=-1 && lastlevel && runs[_activerun]->map[j].second==v_tombstone){
                    --j;
                }
                runs[_activerun]->map[j]=val_run_pair.first;
            }
            lastkey=val_run_pair.first.first;
            lastk=val_run_pair.second;
            unsigned k=val_run_pair.second;
            if(++head[k]<runlist[k]->getcapacity()){
                pair<string,string> kvp=runlist[k]->map[head[k]];
                h.push(make_pair(kvp,k));
            }
        }
        if(lastlevel && runs[_activerun]->map[j].second==v_tombstone){
            --j;
        }
        runs[_activerun]->setcapacity(j+1);
        runs[_activerun]->constructindex();
        if(j+1>0)
            ++_activerun;
    }

    void addrunbyarray(pair<string,string>* runtoadd,unsigned long runlen){
        assert(_activerun<_numruns);
        assert(runlen==_runsize);
        runs[_activerun]->writedata(runtoadd,0,runlen);
        runs[_activerun]->constructindex();
        _activerun++;
    }

    vector<Diskrun*> getrunstomerge(){
        vector<Diskrun*> tomerge;
        for(int i=0;i<_mergesize;i++){
            tomerge.push_back(runs[i]);
        }
        return tomerge;
    }

    void freemergedruns(vector<Diskrun*> &tofree){
        assert(tofree.size()==_mergesize);
        for(int i=0;i<_mergesize;i++){
            assert(tofree[i]->_level==_level);
            delete tofree[i];
        }
        runs.erase(runs.begin(),runs.end()+_mergesize);
        _activerun-=_mergesize;
        for(int i=0;i<_activerun;i++){
            runs[i]->_runid=i;
            string newname=("C_"+to_string(runs[i]->_level)+"_"+to_string(runs[i]->_runid)+".txt");
            if(rename(runs[i]->_filename.c_str(),newname.c_str())){
                perror(("Error renaming file "+runs[i]->_filename+" to "+newname).c_str());
                exit(EXIT_FAILURE);
            }
            runs[i]->_filename=newname;
        }
        for(int i=_activerun;i<_numruns;i++){
            Diskrun* newrun=new Diskrun(_runsize,_pagesize,_level,i,_bf_fp);
            runs.push_back(newrun);
        }
    }

    bool levelfull(){
        return _activerun==_numruns;
    }

    bool levelempty(){
        return _activerun==0;
    }

    int lookup(int &key,bool &found){
        int maxruntosearch=levelfull()?_numruns-1:_activerun-1;
        for(int i=maxruntosearch;i>=0;i--){
            if(runs[i]->maxkey==INT16_MIN || key<runs[i]->minkey || key>runs[i]->maxkey || !runs[i]->bf.maycontain(&key,sizeof(int)))
                continue;
            int lookupres=runs[i]->lookup(key,found);
            if(found)
                return lookupres;
        }
        return (int)NULL;
    }

    long num_elements(){
        long total=0;
        for(int i=0;i<_activerun;i++){
            total+=runs[i]->getcapacity();
        }
        return total;
    }
};

#endif