#include<cstdlib>
#include<cstdio>
#include<cstring>
#include<cmath>
#include<iostream>
using namespace std;
#define BUFFERSIZE 1000000

typedef unsigned char byte;
const int MAXSIZE = 4; //int equals 4 bytes
const int BYTE_BITS = 8;
const int INT_BIT_SIZE = sizeof (int) * BYTE_BITS;

int iBuffer[BUFFERSIZE];
byte bBuffer[4 * BUFFERSIZE];

const char* bytes_to_binary_str(byte* v, int byte_count, char* out, int out_size){
    int idx = 0;
    for(int i = 0; i < byte_count; i++){
        if (i > 0) out[idx++] = '_';
        for(int j = 0; j < BYTE_BITS && i * BYTE_BITS + j < out_size; j++){
            out[idx++] = '0' + ((v[i] >> (BYTE_BITS - j - 1)) & 1);
        }
    }
    out[idx] = 0;
    return out;
}

const char* int_to_binary_str(int v, char* out, int out_size){
    int idx = 0;
    for(int i = 0; i < INT_BIT_SIZE && i < out_size; i++){
        if (i > 0 && i % BYTE_BITS == 0){
            out[idx++] = '_';
        }
        out[idx++] = '0' + ((v >> (INT_BIT_SIZE - i - 1)) & 1);
    }
    out[idx] = 0;
    return out;
}

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

int write_to_file(int num, FILE* file, bool debug=false){
    byte tBuffer[5];
    memset(tBuffer, 0, sizeof(byte)*5);
    char str[INT_BIT_SIZE + INT_BIT_SIZE / BYTE_BITS + 1];
    int_to_binary_str(num, str, sizeof(str));
    int ret = write_to_buffer(num, tBuffer, 5);
    char str_write[ret * BYTE_BITS + ret + 1];
    bytes_to_binary_str(tBuffer, ret, str_write, sizeof(str_write));
    if (debug){
        printf("ret: %d, %11d [%s] ==to-buf==> %s\n", ret, num, 
            str, str_write
            );
    }

    fwrite(tBuffer, sizeof(byte), ret, file);
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

int main(int argc, char** argv){
    int idx = 0;
    bool zigzag = false;
    if (argc > 4 && strncmp(argv[4], "O", 1) == 0){
        zigzag = true;
    }

    if (zigzag){
        //printf("Zigzag mode!\n");
        if (strncmp(argv[1], "e",1) == 0){ //Encoder
            FILE* in;
            FILE* out;
            if ((in = fopen(argv[2], "r")) == NULL){
                printf("Fail to open input file");
                exit(0);
            }
            if ((out = fopen(argv[3], "wb")) == NULL){
                printf("Fail to open output file");
                exit(0);
            }
            int temp = 0;
            while(fscanf(in, "%d", &temp) == 1){
                iBuffer[idx++] = temp;
            }
            fclose(in);
            write_to_file(idx, out);
            for (int i = 0; i < idx;i++){
                int zigzag = int_to_zigzag(iBuffer[i]);
                write_to_file(zigzag, out);
            }
            fclose(out);
        }else{
            FILE* in;
            FILE* out;
            if ((in = fopen(argv[2], "rb")) == NULL){
                printf("Fail to open input file");
                exit(0);
            }
            if ((out = fopen(argv[3], "w")) == NULL){
                printf("Fail to open output file");
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
            int count = 0;
            while(count < tot){
            //printf("start: %d, result: %d\n", start, (int)result);
            
            int ret = read_from_buffer(bBuffer, start, 5);
            fprintf(out, "%d\n", zigzag_to_int(ret));
            count++;
        }
            fclose(out);
        }
        return 0;
    }


    if (strncmp(argv[1], "e",1) == 0){ //Encoder
        FILE* in;
        FILE* out;
        if ((in = fopen(argv[2], "r")) == NULL){
            printf("Fail to open input file");
            exit(0);
        }
        if ((out = fopen(argv[3], "wb")) == NULL){
            printf("Fail to open output file");
            exit(0);
        }
        int temp = 0;
        while(fscanf(in, "%d", &temp) == 1){
            iBuffer[idx++] = temp;
        }
        fclose(in);
        int minus = 0;
        unsigned int* minus_value = (unsigned int *) malloc(idx * sizeof(unsigned int));
        int* minus_loc = (int *)malloc(idx * sizeof(int));
        memset(minus_value, 0, sizeof(unsigned int)*idx);
        memset(minus_loc, 0, sizeof(int) * idx);
        
        for (int i = 0; i < idx; i++){
            if (iBuffer[i] < 0){
                minus_value[minus] = -iBuffer[i];
                minus_loc[minus] = i;
                minus++;
            }
        }
        write_to_file(idx, out);
        write_to_file(minus, out);
        for (int i = 0; i < minus; i++){
            write_to_file(minus_loc[i], out);
            write_to_file(minus_value[i], out);    
            //printf("Write Minus Loc: %d, Minux Value: %d\n", minus_loc[i], minus_value[i]);        
        }
        free(minus_value);
        free(minus_loc);

        for (int i = 0; i < idx;i++){
            if (iBuffer[i] < 0) continue;
            write_to_file(iBuffer[i], out);
        }
        fclose(out);
    }else{
        FILE* in;
        FILE* out;
        if ((in = fopen(argv[2], "rb")) == NULL){
            printf("Fail to open input file");
            exit(0);
        }
        if ((out = fopen(argv[3], "w")) == NULL){
            printf("Fail to open output file");
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
                fprintf(out, "-%u\n", minus_value[now_minus]);
                now_minus++;
                count++;
                continue;
            }
            int ret = read_from_buffer(bBuffer, start, 5);
            fprintf(out, "%d\n", ret);
            count++;
        }
        fclose(out);
        free(minus_value);
        free(minus_loc);
    }
}

