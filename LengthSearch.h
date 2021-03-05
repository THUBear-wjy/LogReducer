#ifndef LENGTHSEARCH_H
#define LENGTHSEARCH_H
#include <string>
#include <unordered_map>
#include <map>
#include <vector>
#include <bitset>
#include <iostream>
#include "constant.h"
#include "template.h"
class LengthSearch {
private:
    std::vector<templateNode *> LengthTemplatePool[MAXTOCKEN*4];
public:
    LengthSearch();
    int addTemplate(int eid, std::string *tokens, int length, int TemplateLength);                 // the function of adding a template
    templateNode * SearchTemplate(std::string * tokens , int length, int realLength); // the funtion of searching for the suitable template
    void TemplatePrint();
};
#endif // lengthsearch header
