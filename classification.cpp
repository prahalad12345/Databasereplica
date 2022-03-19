#include "classification.h"

string classifier(string filename){
    Mat frame=imread(filename);
    vector<string> classes;
    string classfile="classification_classes_ILSVRC2012.txt";
    ifstream ifs(classfile.c_str());
    string line;
    while(getline(ifs,line)){
        classes.push_back(line);
    }
    float scale=1.0;
    int inheight=224;
    int inwidth=224;
    bool swapRB=false;
    Scalar mean=Scalar(255,0,0);
    Net net=readNetFromCaffe("bvlc_googlenet.prototxt","bvlc_googlenet.caffemodel");
    Mat blob;
    blobFromImage(frame,blob,scale,Size(inwidth,inheight),mean,swapRB,false);
    net.setInput(blob);
    Mat prob=net.forward();
    Point classidpoint;
    double confidence;
    minMaxLoc(prob.reshape(1,1),0,&confidence,0,&classidpoint);
    int classid=classidpoint.x;
    return classes[classid];
}