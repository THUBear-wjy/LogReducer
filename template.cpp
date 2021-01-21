#include <iostream>
#include <string>
#include "template.h"
#include <set>
#include "constant.h"
#include <cstring>
using namespace std;

int templateNode::sum = 0;
// set<string> templateNode::split;
// templateNode** templateNode::allTemplates=NULL;
// int*** templateNode::intData=NULL;
// string*** templateNode::stringData=NULL;
// int* templateNode::topPointer=NULL;

//constructor, need Eid, tokens,and length
templateNode::templateNode(int eid,string *logMessage,int l){
    Eid = eid;
	//allTemplates[eid]=this;
    length = l;
    templates = new string[length];
	stringLength = 0;
	stringIndex = new int[MAXTOCKEN];
	int tempParaLength = 0;
    for (int i = 0;i < length;i++){
        templates[i]=logMessage[i];
		if (templates[i] == "<*>"){
			tempParaLength++;
            
			stringIndex[stringLength++]=i;
		}
    }
	paraLength = tempParaLength;
}

templateNode::~templateNode(){
	//cout<<"distruction templates:"<<Eid<<endl;
	delete[] templates;
	delete[] stringIndex;
}

//the match function foe match,need length
int templateNode::matchMatch(string *logMessage,int logLength){
   // cout << "Start Match" << endl;
   // cout << *logMessage << endl;
   // cout << *templates << endl;
    if (logLength != length){
        return -1;
    }
    int sim = 0;
    string token1;
    string token2;
    for (int i=0;i<length;i++){
        token1 = templates[i];
        token2 = logMessage[i];
        if (token1 != token2)
        {
            if (token1!="<*>"){
                return -1;
            } 
        }
        sim++;
        // if (token1 != "<*>")
        // {
        //     if (token1 != token2){
        //         return -1;
        //     }
        //     sim++;
        // }
    }
    return sim;
}

void templateNode::reset(){
    sum = 1;//E0,failed match
}

void templateNode::initPara(){
	stringLength = 0;
	stringIndex = new int[paraLength];
	for (int i=0;i<length;i++){
		if (templates[i] == "<*>"){
			stringIndex[stringLength++]=i;
		}
	}
}

void templateNode::print(){
	cout<<"E" << Eid << "\t";
	for (int i = 0;i<length;i++){
		cout<<templates[i]<<" ";
    }
    cout << endl;
}
