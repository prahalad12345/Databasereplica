#ifndef bloom_h
#define bloom_h

#include <stdio.h>
#include <cstdint> 
#include <vector>
#include <array>
#include <math.h>

#include "MurmurHash.h"
 
using namespace std;

class Bloomfilter{
    private:
       uint8_t m_numHashes;
        vector<bool> m_bits;
    public:
    Bloomfilter(int n,double fp){
        double denom=0.480453013918201;
        double size=-1*(double)n *(log(fp)/denom);
        m_bits=vector<bool>((int)size);
        double ln2=0.69314780559945;
        m_numHashes=(int) ceil((size/n)*ln2);
    }
    array<uint64_t,2> hash(int *data,int len){
        array<uint64_t,2> hashvalue;
        MurmurHash3_x86_128(data,(int)len,0,hashvalue.data());
        return hashvalue;
    }

    uint64_t nthhash(uint32_t n,uint64_t hasha,uint64_t hashb,uint64_t filtersize){
        return (hasha+n*hashb)%filtersize;
    }

    void add(int *data,size_t len){
        auto hashvalues=hash(data,len);
        for(int n=0;n<m_numHashes;n++){
            m_bits[nthhash(n,hashvalues[0],hashvalues[1],m_bits.size())]=true;
        }
    }

    bool maycontain(int *data,size_t len){
        auto hashvalues=hash(data,len);
        for(int n=0;n<m_numHashes;n++){
            if(!m_bits[nthhash(n,hashvalues[0],hashvalues[1],m_bits.size())])
                return false;
        }
        return true;
    }
};

#endif