#ifndef SKIPLIST_H
#define SKIPLIST_H
#include <cstdint>
#include <cstdlib>
#include <strings.h>
#include <random>
#include <vector>
#include <string>
#include "classification.h"
// maxlevel is 14
using namespace std;

default_random_engine generator;
uniform_real_distribution<double> distribution(0.0, 1.0);
const double NODE_PROBABILITY = 0.5;

class Skiplist_Node
{
public:
    string key;
    string value;
    Skiplist_Node *forward[15];
    Skiplist_Node(string searchkey) : key(searchkey)
    {
        try{
            value=classifier(searchkey);
        }
        catch(...){
            value="NULL";
        }
        for (int i = 1; i <= 14; i++)
        {
            forward[i] = NULL;
        }
    }
    virtual ~Skiplist_Node() {}
};

class Skiplist
{
    public:
    int maxlevel;
    string min;
    string max;
    string minkey;
    string maxkey;
    unsigned long long _n;
    size_t maxsize;
    int currmaxlevel;
    Skiplist_Node *plisthead;
    Skiplist_Node *plisttail;
    uint32_t keysperlevel[14];


    Skiplist(string minkey, string maxkey) : plisthead(NULL), plisttail(NULL),
                                       currmaxlevel(1), maxlevel(14), min(minkey), max(maxkey),
                                       minkey(minkey), maxkey(maxkey), _n(0)
    {
        
        plisthead=new Skiplist_Node(minkey);
        plisttail=new Skiplist_Node(maxkey);
        for(int i=1;i<=14;i++){
            plisthead->forward[i]=plisttail;
        }
    }


    ~Skiplist(){
        Skiplist_Node *currnode=plisthead->forward[1];
        while(currnode!=plisttail){
            Skiplist_Node *tempnode=currnode;
            currnode->forward[1];
            delete tempnode;
        }
        delete plisthead;
        delete plisttail;
    }

    int generateNodeLevel()
    {
        return ffs(rand() & ((1 << 14) - 1)) - 1;
    }
    //inserting a node in the skiplist
    void insertkey(string key){
        if(key>max){
            max=key;
        }
        else if(key<min){
            min=key;
        }
        string value=classifier(key);
        Skiplist_Node *update[15];
        Skiplist_Node *currnode=plisthead;
        for(int level=currmaxlevel;level>0;level--){
            while(currnode->forward[level]->key<key){
                currnode=currnode->forward[level];
            }
            update[level]=currnode;
        }
        currnode=currnode->forward[1];
        if(currnode->key==key){
            currnode->value=value;
        }
        else{
            int insertlevel=generateNodeLevel();
            if(insertlevel>currmaxlevel && insertlevel<13){
                for(int lv=currmaxlevel+1;lv<=insertlevel;lv++){
                    update[lv]=plisthead;
                }
                currmaxlevel=insertlevel;
            }
            currnode=new Skiplist_Node(key);
            for(int level=1;level<=currmaxlevel;level++){
                currnode->forward[level]=update[level]->forward[level];
                update[level]->forward[level]=currnode;
            }
            _n++;
        }
    }

    //deleting a node in the skiplist
    void deletekey(string searchkey){
        Skiplist_Node *update[15];
        Skiplist_Node *currnode=plisthead;
        for(int level=currmaxlevel;level>=1;level--){
            while(currnode->forward[level]->key<searchkey){
                currnode=currnode->forward[level];
            }
            update[level]=currnode;
        }
        
        currnode=currnode->forward[1];
        if(currnode->key==searchkey){
            for(int level=1;level<=currmaxlevel;level++){
                if(update[level]->forward[level]!=currnode){
                    break;
                }
                update[level]->forward[level]=currnode->forward[level];
            }
            delete currnode;
            while(currmaxlevel>1 && plisthead->forward[currmaxlevel]==NULL){
                currmaxlevel--;
            }
        }
        _n--;
    }
    //search if the element exist in the skiplist
    string lookup(string searchkey,bool &found){
        Skiplist_Node* currnode=plisthead;
        for(int level=currmaxlevel;level>=1;level--){
            while(currnode->forward[level]->key<searchkey){
                currnode=currnode->forward[level];
            }
        }
        currnode=currnode->forward[1];
        if(currnode->key==searchkey){
            found=true;
            return currnode->value;
        }
        else{
            return "NULL";
        }
    }

    //find all the key in the list
    vector< pair<string,string> > getall(){
        vector< pair<string,string> > vec=vector< pair<string,string> > ();
        Skiplist_Node *node=plisthead->forward[1];
        while(node!=plisttail){
            vec.push_back(make_pair(node->key,node->value));
            node=node->forward[1];
        }
        return vec;
    }
    //find all the keys in the range key1,key2
    vector< pair<string,string> > getallinrange(string key1,string key2){
        if(key1>max || key2<min){
            return (vector< pair<string,string> >){};
        }
        vector< pair<string,string> > vec=vector< pair<string,string> > ();
        Skiplist_Node *node=plisthead->forward[1];
        while(node->key<key1){
            node=node->forward[1];
        }
        while(node->key<key2){
            vec.push_back(make_pair(node->key,node->value));
            node=node->forward[1];
        }
        return vec;
    }
    void set_size(unsigned long size)
    {
        maxsize = size;
    }
    unsigned long long num_elements()
    {
        return _n;
    }
    string get_min(){
        return min;
    }
    string get_max(){
        return max;
    }
};
#endif