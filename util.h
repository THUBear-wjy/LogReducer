#include<cstdlib>
#include<cstdio>
#include<ctype.h>

int Myreadline(FILE* in, int& readSize, int& nowSize, long totSize, char *buffer){
    readSize = 0;
    int badChar = 0;
    while(nowSize++ < totSize){
        char temp;
        fread(&temp, 1, 1, in);
        if (int(temp) == 10){
            break;
        }
		if (int(temp) == 0){
			badChar = 1;
		}
		buffer[readSize++] = temp;        
    }
    return badChar;
}

int Mystrtok(char *s, const char *delim, char* &buf, bool headMode=false){

    const char *spanp;
    int c, sc;
    char *tok;
    static char *last;
    int dcount = 0;
    buf = NULL;
    if (s == NULL && (s = last) == NULL)
        return 0;
    
    c = *s++;
    for (spanp = delim; (sc = *spanp++) != 0;) {
        if (c == sc){           
            last = s;
            return c;
        }
    }
    
    if (c == 0) {                 
        last = NULL;
        return c;
    }
    
    tok = s - 1;
    for(;;){
        c = *s++;
        spanp = delim;
        do {
            if ((sc = *spanp++) == c) {
                int return_value = 0;
                if (c == 0){
                    s = NULL;
                    return_value = 0;
                }
                else{
                    return_value = s[-1];
                    s[-1] = 0;
                }   
                last = s;
                (buf) = (tok);
                return return_value;
            }
        } while (sc != 0);
    }
//    return strtok(s, delim);
}

int Myatoi(char *nptr, int max_length, int &readLength){
    int c; int total;
 	total = 0;
    c = (int)(unsigned char)*nptr++;
    while (isdigit(c)) {
        readLength++;
        if (readLength > max_length){
            readLength = -1;
            break;
        } 
        total = total * 10 + (c - '0');
        c = (int)(unsigned char)*nptr++;
    }
	return total;
}