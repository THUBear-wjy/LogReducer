#include<cstdlib>
#include<cstdio>
#include<map>
#include<set>
#include<iostream>
#include"constant.h"
int number[MAXLOG];
int idd[MAXLOG];
int idu[MAXLOG];
using namespace std;
map<string, int> dic;
set<int> up_counter;
set<int> do_counter;

int main(int argc, char ** argv){
    FILE* in;
    if ((in = fopen(argv[1], "r")) == NULL){
        printf("Iddiff: Fail to open input array\n");
        exit(0);
    }
    int a = 0, count = 0;
    while(fscanf(in, "%d", &a) == 1){
        number[count++] = a;
    }
    fclose(in);

    FILE* id_input;
    if ((id_input = fopen(argv[2], "r")) == NULL){
        printf("Iddiff: Fail to open input id\n");
        exit(0);
    }
    char temp[BUFSIZE];
    count = 0;
    while(fscanf(id_input, "%s", temp) == 1){
        string cc = temp;
        int result = -1;
        map<string, int>::iterator iit = dic.find(cc);
        if (iit == dic.end()){
            dic.insert(pair<string, int>(cc, count));
        }else{
            result = iit ->second;
            dic[cc] = count;
        }
        idd[count] = result;
        idu[count++] = -1;
    }
    fclose(id_input);

    for(int i = count -1; i >= 0; i--){
        if (idd[i] == -1) continue;
        idu[idd[i]] = i;
    }

    FILE* up_output;
    FILE* down_output;
    if ((up_output = fopen(argv[3], "w")) == NULL || (down_output = fopen(argv[4], "w")) == NULL){
        printf("Iddiff: Fail to open output file\n");
        exit(0);
    }
    for (int i = 0; i < count; i++){
        int idt = number[i];
        int iut = number[i];
        if (idd[i] != -1) {
            idt = number[i] - number[idd[i]];
        }
        if (idu[i] != -1){
            iut = number[idu[i]] - number[i];
        }
        do_counter.insert(idt);
        fprintf(down_output, "%d\n", idt);
        up_counter.insert(iut);
        fprintf(up_output, "%d\n", iut);
    }
    fclose(up_output);
    fclose(down_output);
    
    // FILE* message;
    // if ((message = fopen(argv[5], "a")) == NULL){
    //     printf("Iddiff: Fail to open message file\n");
    //     exit(0);
    // }
    // fprintf(message, "%s:%lu\n", argv[3], up_counter.size());
    // fprintf(message, "%s:%lu\n", argv[4], do_counter.size());
    // fclose(message);

    return 0;
}