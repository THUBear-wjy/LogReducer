//Input: ./NumDiff up(or down) input_path output_path
//Output: *up.col(or *do.col)
#include<cstdlib>
#include<cstdio>
#include<set>
#include"constant.h"
int number[MAXLOG];
int up[MAXLOG];
int down[MAXLOG];
using namespace std;
set<int> original;
set<int> up_count;
set<int> do_count;

int main(int argc, char** argv){
    FILE* fp;
    if ((fp = fopen(argv[1], "r")) == NULL){
        printf("numdiff: Fail to open input file\n");
        exit(0);
    }
    int a = 0, count = 0;
    while(fscanf(fp, "%d", &a) == 1){
        number[count++] = a;
        original.insert(a);
    }
    fclose(fp);
    int last = number[count - 1];
    up[count - 1] = number[count - 1];
    for(int i = count; i >= 0; i--){
        int temp = last - number[i];
        up[i] = temp;
        up_count.insert(temp);
        last = number[i];
    }
    
    last = number[0];
    down[0] = number[0];
    for (int i = 1; i < count; i++){
        int temp = number[i] - last;
        down[i] = temp;
        do_count.insert(temp);
        last = number[i];
    }
    
    FILE* up_out;
    FILE* down_out;
    if ((up_out = fopen(argv[2], "w")) == NULL){
        printf("numdiff: Fail to open up output file\n");
        exit(0);
    }

    if ((down_out = fopen(argv[3], "w")) == NULL){
        printf("numdiff: Fail to open down output file\n");
        exit(0);
    }

    for(int i = 0; i < count; i++){
        fprintf(up_out, "%d\n", up[i]);
    }
    for(int i = 0; i < count; i++){
        fprintf(down_out, "%d\n", down[i]);
    }
    fclose(up_out);
    fclose(down_out);

    // FILE* message;
    // if ((message = fopen(argv[4], "a")) == NULL){
    //     printf("numdiff: Fail to open message file\n");
    //     exit(0);
    // }
    // fprintf(message, "%s:%lu\n", argv[1], original.size());
    // fprintf(message, "%s:%lu\n", argv[2], up_count.size());
    // fprintf(message, "%s:%lu\n", argv[3], do_count.size());
    // fclose(message);
}