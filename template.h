#ifndef TEMPLATENODE_H
#define TEMPLATENODE_H

#include <iostream>
#include <string>
#include <set>
#include <string>

using namespace std;

class templateNode{
public:
    // static set<string> split;//加一个固定的set，split
    static int sum;
    // static templateNode **allTemplates;
	// static int ***intData;
	// static string ***stringData;
	// static int *topPointer;

	int Eid;
    string *templates;
    int length;
    int paraLength;
	int intLength;
	int stringLength;
	int *intIndex;
	int *stringIndex;

    //constructor, need Eid, tokens,and length
    templateNode(int Eid,string *logMessage,int length);
    ~templateNode();
    //the match function foe match,need length
    int matchMatch(string *logMessage,int logLength);
    static void reset();
	void initPara();
	void print();

};   

#endif
