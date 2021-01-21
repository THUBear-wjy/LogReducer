#include<cstdlib>
#include<cstdio>
#include<cstring>
#include<set>
#include<map>
#include<cmath>
#include"constant.h"
int number[MAXCOL][MAXLOG];
using namespace std;
map <int, int> num_counter;
double entropy(){
    int tot_length = 0;
    for(map<int, int>::iterator it=num_counter.begin(); it != num_counter.end(); it++){
        tot_length += it ->second;
    }
    if (tot_length == 0) return 0;

    double average = 0.0;
    for(map<int, int>::iterator it=num_counter.begin(); it != num_counter.end(); it++){
        double prob = (double)it -> second / tot_length;
        average += -prob * log(prob);
    }
    return average * tot_length;
}

void num_insert(int temp){
    map<int, int>::iterator iit = num_counter.find(temp);
    if (iit == num_counter.end()){
        num_counter.insert(pair<int, int>(temp, 1));
    }else{
        num_counter[temp]++;
    }
}
int main(int argc, char** argv){
    FILE* message;
    if ((message = fopen(argv[1], "a")) == NULL){
        printf("Diff: Message file open fail\n");
        exit(0);
    }

    FILE* mapping;
    if ((mapping = fopen(argv[2], "r")) == NULL){
        printf("Diff: Mapping file open fail\n");
        exit(0);
    }
    int index, count = 0;

    char path[BUFSIZE];
    while(fscanf(mapping, "%d:%s\n", &index, path) == 2){
        FILE* input;
        if ((input = fopen(path, "r")) == NULL){
            printf("Diff: Output input file fail\n");
            exit(0);
        }
        int temp;
        count = 0;
        num_counter.clear();
        int sequtial = -1;
        int last_num = 0;
        while(fscanf(input, "%d", &temp) == 1){
            number[index][count++] = temp;
            num_insert(temp);
            // if (sequtial == -1){
            //     sequtial = 1;
            //     last_num = temp;
            // }else{
            //     if (last_num == temp){
            //         //sequtial++;
            //         sequtial++;
            //         continue;
            //     }else{
            //         num_insert(last_num);
            //         sequtial = 1;
            //         last_num = temp;
            //     }
            // }
        }
        fprintf(message, "%d:%.4f\n", index, entropy());
        fclose(input);
    }
    fclose(mapping);

    FILE* diff_two;
    if ((diff_two = fopen(argv[3], "r")) == NULL){
        printf("Diff: 2 diff file open fail\n");
        exit(0);
    }
    int col1, col2;
    while(fscanf(diff_two, "%d%d", &col1, &col2) == 2){
        num_counter.clear();
        int sequtial = -1;
        int last_num = 0;
        for (int i = 0; i < count; i++){
            int temp = number[col1][i] - number[col2][i];
            num_insert(temp);
            // if (sequtial == -1){
            //     sequtial = 1;
            //     last_num = temp;
            // }else{
            //     if (last_num == temp){
            //         //sequtial++;
            //         sequtial++;
            //         continue;
            //     }else{
            //         num_insert(last_num);
            //         sequtial = 1;
            //         last_num = temp;
            //     }
            // }
        }
        fprintf(message, "%d-%d:%.4f\n", col1, col2, entropy());
        //printf("%d-%d finish\n", col1, col2);
    }
    fclose(diff_two);

    FILE* diff_three;
    if ((diff_three = fopen(argv[4], "r")) == NULL){
        printf("Diff: 3 diff file open fail\n");
        exit(0);
    }
    int _col1, _col2, _col3;
    while(fscanf(diff_three, "%d%d%d", &_col1, &_col2, &_col3) == 3){
        num_counter.clear();
        int sequtial = -1;
        int last_num = 0;
        for (int i = 0; i < count; i++){
            int temp = number[_col1][i] - number[_col2][i] - number[_col3][i];
            num_insert(temp);
            // if (sequtial == -1){
            //     sequtial = 1;
            //     last_num = temp;
            // }else{
            //     if (last_num == temp){
            //         //sequtial++;
            //         sequtial++;
            //         continue;
            //     }else{
            //         num_insert(last_num);
            //         sequtial = 1;
            //         last_num = temp;
            //     }
            // }
        }
        fprintf(message, "%d-%d-%d:%.4f\n", _col1, _col2, _col3, entropy());
        //printf("%d-%d-%d finish\n", _col1, _col2, _col3);
    }
    fclose(diff_three);
    
    fclose(message);
    return 0;
}