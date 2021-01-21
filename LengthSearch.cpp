#include "LengthSearch.h"

LengthSearch::LengthSearch() {
    for (int i = 0; i < MAXTOCKEN*2;i++)
        LengthTemplatePool[i].clear();
}

int LengthSearch::addTemplate(int eid, std::string *tokens,int length, int TemplateLength) {
    cout << "Insert template with length: " << length << " TemplateLength: " << TemplateLength << endl;
    templateNode *tempnode = new templateNode(eid, tokens, length);
    LengthTemplatePool[TemplateLength].push_back(tempnode);
}

templateNode * LengthSearch::SearchTemplate(std::string * tokens , int length, int realLength){
    //cout << "Start search template" << endl;
    for (auto &temp:LengthTemplatePool[realLength])
    {
        if (temp->matchMatch(tokens,length)>=0)
            return temp;
    }
    return NULL;
}

void LengthSearch::TemplatePrint(){
   for (int i = 0; i < MAXTOCKEN*2; i++){
        if (LengthTemplatePool[i].size() == 0)
            continue;
        cout << "Template with length: " << i << endl;
        for (auto &temp:LengthTemplatePool[i]){
            temp -> print();
        }
   }
}
