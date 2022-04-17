#include "disklevel.h"
#include "skiplist.h"
#include  "hashmap.h"
#include "classification.h"
#include "bloom.h"
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <stdlib.h>
#include <future>
#include <vector>
#include <mutex>
#include <thread>
#include <iostream>

class LSM{
    public:
    string v_tombstone="A";
    mutex *mergelock;
    vector<Bloomfilter*> filters;
    vector<Disklevel*> disklevels;
    vector<Skiplist *> C_0;
    unsigned int _activeRun;
    unsigned long _eltsPerRun;
    double _bfFalsePositiveRate;
    unsigned int _num_runs;
    double _frac_runs_merged;
    unsigned int _numDiskLevels;
    unsigned int _diskRunsPerLevel;
    unsigned int _num_to_merge;
    unsigned int _pageSize;
    unsigned long _n;
    thread mergeThread;
    //doubt
    LSM(const LSM &other) = default;
    LSM(LSM &&other) = default;

    LSM(long eltsperrun,int numruns,double mergedfrac,double bffp,int pagesize,int diskrunsperlevel):_eltsPerRun(eltsperrun),_num_runs(numruns),_frac_runs_merged(mergedfrac),_diskRunsPerLevel(diskrunsperlevel),_num_to_merge(ceil(_frac_runs_merged*_num_runs)),_pageSize(pagesize){
        _activeRun=0;
        _bfFalsePositiveRate=bffp;
        _n=0;
        Disklevel *disklevel=new Disklevel(pagesize,1,_num_to_merge*eltsperrun,_diskRunsPerLevel,ceil(_diskRunsPerLevel*_frac_runs_merged),_bfFalsePositiveRate);
        disklevels.push_back(disklevel);
        _numDiskLevels=1;
        for(int i=0;i<_num_runs;i++){
            Skiplist* run=new Skiplist("A","zzzzzzzzzzzzzzzzzzzzzzzzzzzz");
            run->set_size(eltsperrun);
            C_0.push_back(run);

            Bloomfilter *bf=new Bloomfilter(_eltsPerRun,_bfFalsePositiveRate);
            filters.push_back(bf);
        }
        mergelock=new mutex();
    }

    ~LSM(){
        if(mergeThread.joinable())
            mergeThread.join();
        delete mergelock;
        for(int i=0;i<C_0.size();i++){
            delete C_0[i];
            delete filters[i];
        }
        for(int i=0;i<disklevels.size();i++){
            delete disklevels[i];
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

    void mergerunstolevel(int level){
        bool islast=false;
        if(level==_numDiskLevels){
            Disklevel *newlevel=new Disklevel(_pageSize,level+1,disklevels[level-1]->_runsize,_diskRunsPerLevel,ceil(_diskRunsPerLevel*_frac_runs_merged),_bfFalsePositiveRate);
            disklevels.push_back(newlevel);
            _numDiskLevels++;
        }

        if(disklevels[level]->levelfull()){
            mergerunstolevel(level+1);
        }
        if(level+1==_numDiskLevels && disklevels[level]->levelempty()){
            islast=true;
        }

        vector<Diskrun *> runtomerge=disklevels[level-1]->getrunstomerge();
        long runlen=disklevels[level-1]->_runsize;
        disklevels[level]->addruns(runtomerge,runlen,islast);
        disklevels[level-1]->freemergedruns(runtomerge);
    }

    void mergeruns(vector<Skiplist*> runstomerge,vector<Bloomfilter*> bftomerge){
        vector< pair<string,string> > tomerge=vector< pair<string,string> >();
        tomerge.reserve(_eltsPerRun*_num_to_merge);
        for(int i=0;i<runstomerge.size();i++){
            vector<pair<string,string>> all=runstomerge[i]->getall();
            tomerge.insert(tomerge.begin(),all.begin(),all.end());
            delete runstomerge[i];
            delete (bftomerge)[i];
        }
        sort(tomerge.begin(),tomerge.end());
        mergelock->lock();
        if(disklevels[0]->levelfull()){
            mergerunstolevel(1);
        }
        disklevels[0]->addrunbyarray(&tomerge[0],tomerge.size());
        mergelock->unlock();
    }

    void do_merge(){
        if(_num_to_merge==0)
            return;
        vector<Skiplist*> runstomerge=vector<Skiplist*>();
        vector<Bloomfilter*> bftomerge=vector<Bloomfilter*> ();
        for(int i=0;i<_num_to_merge;i++){
            runstomerge.push_back(C_0[i]);
            bftomerge.push_back(filters[i]);
        }
        if(mergeThread.joinable())
            mergeThread.join();
        
        mergeThread=thread(&LSM::mergeruns,this,runstomerge,bftomerge);
        C_0.erase(C_0.begin(),C_0.begin()+_num_to_merge);

        filters.erase(filters.begin(),filters.begin()+_num_to_merge);
        _activeRun-=_num_to_merge;
        for(int i=_activeRun;i<_num_runs;i++){
            Skiplist *run=new Skiplist("A","zzzzzzzzzzzzzzzzzz");
            run->set_size(_eltsPerRun);
            C_0.push_back(run);
            Bloomfilter *bf=new Bloomfilter(_eltsPerRun,_bfFalsePositiveRate);
            filters.push_back(bf);
        }
    }

    unsigned long num_buffer(){
        if(mergeThread.joinable())
            mergeThread.join();
        long total=0;
        for(int i=0;i<=_activeRun;i++){
            total+=C_0[i]->num_elements();
        }
        return total;
    }


    void insert_key(string key,string value){
        if(value!="NULL")
            string value=classifier(key);
        if(C_0[_activeRun]->num_elements()>=_eltsPerRun)
            ++_activeRun;
        //cout<<1<<endl;
        if(_activeRun>=_num_runs)
            do_merge();
        C_0[_activeRun]->insertkey(key);
        int num=compute_hash(key);
        filters[_activeRun]->add(&num,sizeof(int));
    }

    bool lookup(string key){
        string value=classifier(key);
        int num=compute_hash(key);
        bool found=0;
        for(int i=_activeRun;i>=0;i--){
            if(key<C_0[i]->get_min() || key>C_0[i]->get_max() || filters[i]->maycontain(&num,sizeof(int)))
                continue;
            value=C_0[i]->lookup(key,found);
            if(found)
                return value!=v_tombstone;
        }

        if(mergeThread.joinable())
            mergeThread.join();

        for(int i=0;i<_numDiskLevels;i++){
            value=disklevels[i]->lookup(key,found);
            if(found)
                return value!=v_tombstone;
        }
        return false;
    }

    void delete_key(string &key){
        insert_key(key,"NULL");
    }

    vector< pair<string,string> > range(string key1,string key2){
        if(key2<=key1){
            return (vector<pair<string,string>> {});
        }
        cout<<1<<endl;
        auto ht=Hashtable((1<<12)*1000);
        
        vector<pair<string,string>> eltsinrange=vector<pair<string,string>>();
        cout<<1<<endl;
        for(int i=_activeRun;i>=0;i--){
            vector<pair<string,string>> curelts=C_0[i]->getallinrange(key1,key2);
            cout<<1<<endl;
            if(curelts.size()!=0){
                eltsinrange.reserve(eltsinrange.size()+curelts.size());
                cout<<1<<endl;
                for(int c=0;c<curelts.size();c++){
                    int num=compute_hash(curelts[c].first);
                    cout<<3<<endl;
                    string dummy=ht.putifempty(num,curelts[c].second);
                    cout<<4<<endl;
                    if(dummy!="NULL" && curelts[c].second!=v_tombstone)
                        eltsinrange.push_back(curelts[c]);
                }
            }
            cout<<1<<endl;
        }
        if(mergeThread.joinable()){
            mergeThread.join();
        }
        for(int j=0;j<_numDiskLevels;j++){
            for(int r=disklevels[j]->_activerun-1;r>=0;r--){
                long i1,i2;
                disklevels[j]->runs[r]->range(key1,key2,i1,i2);
                if(i2-i1!=0){
                    auto oldsize=eltsinrange.size();
                    eltsinrange.reserve(oldsize+(i2-i1));
                    for(long m=i1;m<i2;m++){
                        auto kv=disklevels[j]->runs[r]->map[m];
                        int num=compute_hash(kv.first);
                        string dummy=ht.putifempty(num,kv.second);
                        if(dummy!="NULL" && kv.second!=v_tombstone)
                            eltsinrange.push_back(kv);
                    }
                }
            }
        }
        return eltsinrange;
    }

    
    void printElts(){
        if(mergeThread.joinable())
            mergeThread.join();
        cout<<"Memory buffer"<<endl;
        for(int i=0;i<=_activeRun;i++){
            cout<<"Memory buffer run"<<" "<<i<<endl;
            auto all=C_0[i]->getall();
            for(auto &c:all){
                cout<<c.first<<": "<<c.second<<endl;
            }
            cout<<endl;
        }

        cout<<"DISK BUFFER"<<endl;
        for(int i=0;i<_numDiskLevels;i++){
            cout<<"DISKLEVEL "<<i<<endl;
            for(int j=0;j<disklevels[i]->_activerun;j++){
                cout<<"RUN "<<j<<endl;
                for(int k=0;k<disklevels[i]->runs[j]->getcapacity();k++){
                    cout<<disklevels[i]->runs[j]->map[k].first<<":"<<disklevels[i]->runs[j]->map[k].second<<" ";
                }
                cout<<endl;
            }
            cout<<endl;

        }
    }

    unsigned long sizze(){
        string min = "A";
        string max = "zzzzzzzzzzzzzzzzzzzzzzzzzzzz";
        auto r = range(min, max);
        return r.size();
    }

    void printstats(){
        //cout<<1<<endl;
        cout<<"HELP PLEAW"<<endl;
        cout<<"Number of Elements:"<<sizze()<<endl;
        cout<<"HELP PLEAW"<<endl;
        cout<<"Number of Elements in buffer (includes deletes):"<<num_buffer()<<endl;
        for(int i=0;i<disklevels.size();i++){
            cout<<"number of Elements in disklevel "<<i<<" deletes:"<<disklevels[i]->num_elements()<<endl;
        }
        cout<<"Key value dump by level:"<<endl;
        printElts();
    }
};