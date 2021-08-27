import re
import os
import pandas as pd
from itertools import groupby
class Header:
    def __init__(self, head_length=0, is_multi=0, head_bound="", template_path="", heads=dict(), delimer=dict()):
        self.output_path = os.path.join(template_path + "head.format")
        self.heads = heads
        self.headLength = head_length
        self.is_multi = is_multi
        self.head_bound = head_bound
        self.threshold = 0.95
        self.delimers = delimer

    def genFormat(self, head):
        Hcount = 0
        Hpart = []
        HDic = dict()
        
        for h in head:
            hs = [''.join(list(g)) for k,g in groupby(h, key=lambda x:x.isdigit())]
            Hpart.append(hs)
#print(head)
#        print(Hpart) 
        Hpart_len = list(map(lambda x:len(x), Hpart))
        Hcount = max(Hpart_len, key=Hpart_len.count)
        if (pd.value_counts(Hpart_len)[Hcount] < len(Hpart_len) * self.threshold): #Different lenght -> Regular string
            return 1, 0, "%s", [-1], []
        
        for i in range(0, Hcount):
            HDic[i] = []
        for i in Hpart:
            try:
                for t in HDic.keys():
                    HDic[t].append(i[t])
            except:
                continue
        
        str_count = 0
        num_count = 0
        str_length = []
        num_length = []
        Hformat = ""

        for t in HDic.keys():
            pivot = max(HDic[t], key=HDic[t].count) #Most common part
            if not pivot.isdigit(): #String
                if (len(HDic) == 1): #Only one string
                    return  1, 0, "%s", [-1], []
                if (pd.value_counts(HDic[t])[pivot] < len(HDic[t]) * self.threshold): #Dynamic string -> Regular string
                    return  1, 0, "%s", [-1], []
                else:
                    Hformat += pivot
                    str_count += 1
                    str_length.append(len(pivot))
            else: #Int
                n_len = list(map(lambda x:len(x), HDic[t]))
                len_pivot = max(n_len, key=n_len.count)
                if (pd.value_counts(n_len)[len_pivot] < len(HDic[t]) * self.threshold): # Variable length
                    num_length.append(-1)
                else: # Fix length
                    num_length.append(len_pivot) 
                Hformat += "%d"
                num_count += 1
        return str_count, num_count, Hformat, str_length, num_length


    def outputFormat(self):
        fw = open(self.output_path, "w")
        fw.write(str(self.headLength) + "\n")
        if(self.is_multi):
            fw.write(str(1) + "\n")
            fw.write(str(self.head_bound) + "\n")
        else:
            fw.write(str(0) + "\n")
        for i in range(0, self.headLength):
            str_count, num_count, Hformat, str_length, num_length = self.genFormat(self.heads[i][0:min(5000, len(self.heads[i]))])
            fw.write(str(str_count) + " ")
            fw.write(str(num_count) + " ")
            fw.write(Hformat + " ")
            for t in range(0, str_count):
                fw.write(str(str_length[t]) + " ")
            for t in range(0, num_count):
                fw.write(str(num_length[t]) + " ")
            fw.write("\n")
        for i in range(0, self.headLength):
            fw.write(max(self.delimers[i], key=self.delimers[i].count) + "\n")
        fw.close()
