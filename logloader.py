import sys
import pandas as pd
import re
import multiprocessing as mp
from itertools import groupby, count, chain
import numpy as np
import json
import os
import io
import time
class LogLoader(object):
    def __init__(self, headLength, isMulti, headRegex, maxLength):
        self.headLength = headLength
        self.isMulti = isMulti
        if (headRegex):
            self.headRegex = re.compile(headRegex)
        self.maxLength = maxLength
        self.splitregex = re.compile(r'(\s+|\|)')

    def formalize_message(self, lines):
        def get_content(line):
            count = 0
            in_head = False
            for idx, i in enumerate(line):
                #print(i)
                if not self.splitregex.search(i):
                    if not (in_head):
                        count += 1
                    in_head = True
                else:
                    in_head = False
                if (self.headLength + 1 == count):
                    return line[idx:].strip()
            return line.strip()
            #print("{}: count -> {}".format(line, count))
        def get_head(line_seg, headers, delimer):
            head_count = 0
            for idx, se in enumerate(line_seg):
                if (head_count >= self.headLength):
                    break
                if (idx % 2 == 0):
                    headers[head_count].append(se)
                else:
                    delimer[head_count].append(se)
                    head_count += 1
        
        def get_segment(line):
            temp_seg = []
            spliter = ""
            for i in self.splitregex.split(line):
                if i == "":
                    continue
                if (self.splitregex.search(i)):
                    spliter += i
                else:
                    temp_seg.append(spliter)
                    spliter = ""
            return temp_seg

        log_messages = []
        count = 0
        fail_count = 0
        headers = dict()
        header_delimer = dict()
        for i in range(0, self.headLength):
            headers[i] = []
            header_delimer[i] = []
        if (self.isMulti):
            start = True
            now_res = ""
            for line in lines:
                if not line.strip():
                    fail_count += 1
                    continue
                
                line_seg = self.splitregex.split(line.strip())
                match = self.headRegex.search(line_seg[0])
                content_line = get_content(line)
                if match: #New start
                    get_head(line_seg, headers, header_delimer)
                    if(start):
                        start = False
                        now_res = content_line
                    else:
                        if (len(now_res) > self.maxLength):
                            fail_count += 1
                            continue
                        log_messages.append(now_res)
                        now_res = content_line
                    count += 1
                else: #Continue
                    if(start):
                        fail_count += 1
                        continue
                    else:
                        now_res += "\n" + line.strip()
        else:
            for line in lines:
                line_seg = self.splitregex.split(line.strip())
                if not line.strip():
                    fail_count += 1
                    continue
                get_head(line_seg, headers, header_delimer)
                content_line = get_content(line)
                if (len(content_line) > self.maxLength):
                    fail_count += 1
                    continue
                log_messages.append(content_line)
                count += 1

        return log_messages, fail_count, headers, header_delimer
    
    def load_to_dataframe(self, log_filepath):
        """ Function to transform log file to dataframe 
        """
        print('Loading log messages to dataframe...')
        t1 = time.time()
        lines = []
        with open(log_filepath, 'r', encoding="utf-8", errors="ignore") as fid:
            lines = fid.readlines()
        print("Total lines {}".format(len(lines)))
        log_messages = []
        log_messages, failed_size, headers, head_delimer = self.formalize_message(lines)
        log_dataframe = pd.DataFrame(log_messages, columns=['Content'])
        print("Success load logs#: {}, Failed load lines#: {}".format(len(log_messages), failed_size))
        
        t2 = time.time()
        print('Time taken {:.2f}s'.format(t2-t1))
        return log_dataframe, headers, head_delimer


