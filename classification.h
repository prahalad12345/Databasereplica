#ifndef CLASSIFICATION_H 
#define CLASSIFICATION_H

#include <opencv2/opencv.hpp>
#include <opencv2/core.hpp>
#include <opencv2/highgui.hpp>
#include <opencv2/imgcodecs.hpp>
#include <iostream>
#include<vector>
#include <fstream>
#include <sstream> 
#include <opencv2/dnn.hpp>
using namespace std;
using namespace cv;
using namespace dnn;

string classifier(string filename);

#endif 