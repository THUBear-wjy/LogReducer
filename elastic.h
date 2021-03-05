#include<cstdlib>
#include<cstdio>
#include<cstring>
#include<cmath>
#include<iostream>
#include"constant.h"
using namespace std;
typedef unsigned char byte;
byte bBuffer[4 * MAXLOG];

int int_to_zigzag(int n){
    return (n << 1) ^ (n >> 31);
}

int zigzag_to_int(int n){
    return (((unsigned int)n) >> 1) ^ -(n & 1);
}

int write_to_buffer(int zz, byte* buf, int size){
    int ret = 0;
    for(int i = 0 ; i < size; i++){
        if ((zz & (~0x7f)) == 0){
            buf[i] = (byte)zz;
            ret = i + 1;
            break;
        }else{
            buf[i] = (byte)((zz & 0x7f) | 0x80);
            zz = ((unsigned int )zz) >> 7;
        }
    }
    return ret;
}

unsigned int read_from_buffer(byte* buf, long& start, int max_size){
    unsigned int ret = 0;
    int offset = 0;
    int i = start;
    for(; i - start < max_size; i++, offset += 7){
        byte n = buf[i];
        if ((n & 0x80) != 0x80){
            ret |= (n << offset);
            i++;
            break;
        }else{
            ret |= ((n & 0x7f) << offset);
        }
    }
    start = i;
    return ret;
}

int write_to_file(int num, FILE* file){
    byte tBuffer[5];
    memset(tBuffer, 0, sizeof(byte)*5);
    int ret = write_to_buffer(num, tBuffer, 5);
    fwrite(tBuffer, sizeof(byte), ret, file);
    return ret;
}

void PositiveEncoder(FILE* out, int * array, int size, bool enable=0){
        int minus = 0;
        unsigned int* minus_value = (unsigned int *) malloc(size * sizeof(unsigned int));
        int* minus_loc = (int *)malloc(size * sizeof(int));
        memset(minus_value, 0, sizeof(unsigned int)*size);
        memset(minus_loc, 0, sizeof(int) * size);
        
        for (int i = 0; i < size; i++){
            if (array[i] < 0){
                minus_value[minus] = -array[i];
                minus_loc[minus] = i;
                minus++;
            }
        }
        write_to_file(size, out);
        write_to_file(minus, out);
        for (int i = 0; i < minus; i++){
            write_to_file(minus_loc[i], out);
            write_to_file(minus_value[i], out);    
            //printf("Write Minus Loc: %d, Minux Value: %d\n", minus_loc[i], minus_value[i]);        
        }
        free(minus_value);
        free(minus_loc);

        for (int i = 0; i < size;i++){
            if (array[i] < 0) continue;
            write_to_file(array[i], out);
        }
}

void PositiveDecoder(const char * file_name, int* buffer){
        FILE* in;
        if ((in = fopen(file_name, "rb")) == NULL){
            printf("Fail to open input file");
            exit(0);
        }
        fseek(in, 0, SEEK_END);
        long lSize = ftell(in);
        rewind(in);
        long result = fread(bBuffer, 1, lSize, in);
        if (result != lSize){
            printf("Reading entair file fail");
            exit(0);
        }
        fclose(in);
        long start = 0;
        int tot = read_from_buffer(bBuffer, start, 5);
        int minus = read_from_buffer(bBuffer, start, 5);
        cout << minus << endl;
        //printf("Read minus: %d", minus);
        unsigned int * minus_value = (unsigned int*)malloc(minus * sizeof(unsigned int));
        int * minus_loc = (int*)malloc(minus * sizeof(int));
        memset(minus_value, 0, sizeof(unsigned int)*minus);
        memset(minus_loc, 0, sizeof(int) * minus);
        
        for (int i = 0; i < minus; i++){
            minus_loc[i] = read_from_buffer(bBuffer, start, 5);
            minus_value[i] = read_from_buffer(bBuffer, start, 5);
            //printf("Minus Loc: %d, Minux Value: %d\n", minus_loc[i], minus_value[i]);
        }
        int count = 0;
        int now_minus = 0;
        printf("start: %ld, result: %ld", start, result);
        while(count < tot){
            //printf("start: %d, result: %d\n", start, (int)result);
            
            if (now_minus < minus && count == minus_loc[now_minus]){
                buffer[count] = -minus_value[now_minus];
                now_minus++;
                count++;
                continue;
            }
            int ret = read_from_buffer(bBuffer, start, 5);
            buffer[count] = ret;
            count++;
        }
        free(minus_value);
        free(minus_loc);
}

void ZigzagEncoder(FILE* out, int * array, int size){
    write_to_file(size, out);
    for (int i = 0; i < size;i++){
        int zigzag = int_to_zigzag(array[i]);
        write_to_file(zigzag, out);
    }
}

void IntEncoder(const char * file_name, int * array, int size, int type, bool diff=false){
    if (diff){
        int last = array[0];
        for (int i = 1; i < size; i++){
            int diff_res = array[i] - last;
            last = array[i];
            array[i] = diff_res;
        }
    }

    FILE* out;
    if ((out = fopen(file_name, "w")) == NULL){
        printf("Fail to open output file");
        exit(0);
    }
    if (type == 0){
        for (int i = 0; i < size;i++){
            fprintf(out, "%d\n", array[i]);
        }
    }
    if (type == 1){
        ZigzagEncoder(out, array, size);
    }
    
    fclose(out);
    return;
}

void ZigzagDecoder(const char * file_name, int* buffer){
    long start = 0;
    FILE* in;
    if ((in = fopen(file_name, "rb")) == NULL){
        printf("Fail to open output file\n");
        exit(0);
    }
    fseek(in, 0, SEEK_END);
    long lSize = ftell(in);
    rewind(in);
    long result = fread(bBuffer, 1, lSize, in);
    if (result != lSize){
        printf("Reading entair file fail");
        exit(0);
    }
    fclose(in);
    int count = 0;
    int tot = read_from_buffer(bBuffer, start, 5);
    while(count < tot){
        int zigzag = read_from_buffer(bBuffer, start, 5);
        int ret = zigzag_to_int(zigzag);
        buffer[count++] = ret;
    }

}

void StringEncoder(const char* file_name, string * array, int size){
    FILE* out;
    if ((out = fopen(file_name, "w")) == NULL){
        printf("%s", file_name);
        printf("Fail to open output file\n");
        exit(0);
    }

    for(int i = 0; i < size;i++){
        fprintf(out, "%s\n", array[i].c_str());
    }
    fclose(out);
}
