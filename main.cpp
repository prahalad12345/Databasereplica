
#include <stdio.h>
#include <cstdio>
#include <ctime>
#include <iostream>
#include <fstream>
#include <cstdint>
#include <math.h>
#include <random>
#include <algorithm>
#include "skiplist.h"
#include "bloom.h"
#include "hashmap.h"
#include "lsm.h"
#include "classification.h"


using namespace std;

struct LSMParams {
    int num_inserts;
    int num_runs;
    int elts_per_run;
    double bf_fp;
    int pageSize;
    int disk_runs_per_level;
    double merge_fraction;
};

void loadfrombin(LSM &lsm,string filename){
    FILE *intarrayfile;
    long size;
    intarrayfile=fopen(filename.c_str(),"rb");
    fseek(intarrayfile,0,SEEK_END);
    size=ftell(intarrayfile);
    int newarray[size/sizeof(int)];
    rewind(intarrayfile);
    size_t num;
    num=fread(newarray,sizeof(int),size/sizeof(int)+1,intarrayfile);
    assert(num==size/sizeof(int));
    int *ptr=newarray;
    int read=0;
    int k,v;
    while(read+1<num){
        k=*ptr;
        v=*(ptr+1);
        lsm.insert_key(k,v);
        ptr+=2;
        read+=2;   
    }
}

void queryline(LSM &lsm,string &line,vector<string> &strings){
    //cout<<1<<endl;
    //cout<<line<<endl;
    //cout<<line.find(' ')<<endl;
    unsigned long pos=line.find(' ');
    long ip=0;
    strings.clear();
    while(pos!=string::npos){
        strings.push_back(line.substr(ip,pos-ip+1));
        ip=pos+1;
        pos=line.find(' ',ip);
    }

    strings.push_back(line.substr(ip,(pos<line.size()?pos:line.size())-ip+1));
    switch((char)strings[0].c_str()[0]){
        case 'p':{
            string pk=strings[1];
            string v=strings[2];
            cout<<pk<<" "<<v<<endl;
            lsm.insert_key(pk,v);
        }
        break;
        case 'g':{
            string lk=strings[1];
            string v=classifier(lk);
            bool found=lsm.lookup(lk);
            if(found)
                cout<<v<<endl;
            else 
                cout<<"NULL"<<endl;
            cout<<endl;
        }
        break;
        case 'r':{
            string lk1=strings[1];
            string lk2=strings[2];
            auto res=lsm.range(lk1,lk2);
            if(!res.empty()){
                for(int i=0;i<res.size();i++){
                    cout<<res[i].first<<":"<<res[i].second<<" ";
                }
            }
            cout<<endl;
        }
        break;
        case 'd':{
            string dk=strings[1];
            lsm.delete_key(dk);
        }
        break;
        case 'l':{
            string ls=strings[1];
            loadfrombin(lsm,ls);
        }
        break;
        case 's':{
            lsm.printstats();
        }
    }
}

int main(int argc,char *argv[]){
    auto lsm=LSM(800,20,1.0,0.001,1024,20);
    auto strings=vector<string>(3);
    if (argc == 2){
       // cout<<1<<endl;
    cout << "LSM Tree DSL Interactive Mode" << endl;
        while (true){
            cout << "> ";
            string input;
            //cout<<1<<endl;
            getline(cin, input);
            //cout<<input<<endl;
            queryline(lsm, input, strings);
        }
    }
    else{
        string line;
        ifstream f;
        cout<<1<<endl;
        for (int i = 1; i < argc; ++i){
            f.open(argv[i]);
            
            if(!f.is_open()) {
                perror("Error open");
                exit(EXIT_FAILURE);
            }
            while(getline(f, line)) {
                queryline(lsm, line, strings);
            }
        }
    }
}