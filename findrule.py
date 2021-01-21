#input: DF
#output: rule.txt
import re
import argparse
import sys
import math
import pandas as pd
from pandas import Series, DataFrame
from itertools import combinations, permutations
import numpy as np
import time
import sys
import util
import os
from subprocess import call

class RuleFinder:
    def __init__(self, Input = pd.DataFrame(), Type = "", Template = "", Opt=0.8, WorkingPath="", Output = "", kernal= "", level="N", metric=0):
        self.df = Input
        self.type = Type
        self.template = Template
        self.opt = Opt
        self.working_path = util.path_pro(WorkingPath)
        self.output_path = Output
        self.path = "./"
        self.ndf = []
        self.sdf = []
        self.number = []
        self.string = []
        self.operation = []
        self.unoperation = []
        self.id = []
        self.fid = set()
        self.kernal = kernal
        self.corr_map = dict()
        self.num_map = dict()
        self.matric_dict = dict()
        self.level = level
        self.metric = metric

        self.unique_path = "matrics.msg"
        self.mapping_path = "mapping.msg"

        if (not os.path.exists(self.working_path)):
            os.makedirs(self.working_path)
    
    def find_prepare(self):
        self.gen_basic_info()
        self.gen_numberial()
    
    def gen_algorithm_rule(self, matric, form_tot):
        def possible_form(col):
            fo = []
            start = (col // form_tot)*form_tot
            for i in range(0, form_tot):
                fo.append(start + i)
            return fo

        def bi_comb(col1, col2):
            fo = []
            if (col1 // form_tot == col2 // form_tot):
                return fo

            for a in possible_form(col1):
                for b in possible_form(col2):
                    fo.append(str(max(a,b)) + "-" + str(min(a,b)))
            return fo
            
        def tri(a,b,c):
            return str(a) + "-" + str(max(b,c)) + "-" + str(min(b,c))

        def tri_comb(col1, bi_pair):
                #print("bi_pair:{}".format(bi_pair))
            number = bi_pair.split("-")
            fo = []
            b = int(number[0])
            c = int(number[1])
                
            ocol1 = col1 // form_tot
            ocol2 = b // form_tot
            ocol3 = c // form_tot

            if ((ocol1 == ocol2) or (ocol2 == ocol3) or (ocol1 == ocol3)):
                return fo

            for a in possible_form(col1):
                fo.append(tri(a,b,c))
                fo.append(tri(b,a,c))
                fo.append(tri(c,a,b))
            return fo

        def get_max(now_set):
            return sorted(list(now_set), key=lambda x:matric[x])[0]
            
            
        result_set = set()
        bi_res_set = set()
        now_set = set()
        now_rule = []
        now_matric = 0
        for n in range(0, len(self.number)):
            for i in possible_form(n * form_tot):
                now_set.add(str(i))
            
        while(len(result_set) < len(self.number)):
            print("Now size: {}, Result set: {}, Now matric: {}".format(len(result_set), result_set, now_matric))
            pair = get_max(now_set)
            now_matric += matric[pair]
            print("Get best pair: {}, now matric: {}".format(pair, matric[pair]))
            numbers = pair.split("-")
            now_rule.append(numbers)
            new_member = -1
            for n in numbers:
                original_form = (int(n) // form_tot) * form_tot
                if not (original_form in result_set):
                    new_member = original_form
                    break
            if (new_member == -1):
                print("Error!")
                exit(0)
            # Remove members in now_set
            uni_remove_set = []
            bi_remove_set = []
            tri_remove_set = []
            for i in possible_form(new_member):
                uni_remove_set.append(str(i))
            for i in result_set:
                bi_remove_set += bi_comb(i, new_member)
            for t in bi_res_set:
                tri_remove_set += tri_comb(new_member, t)
            remove_candidate = uni_remove_set + bi_remove_set + tri_remove_set

            for ele in remove_candidate:
                if ele in now_set:
                    now_set.remove(ele)
                    
            #Update result_set
            for ele in bi_remove_set:
                bi_res_set.add(ele)
            result_set.add(new_member)

            # Add members to now_set
            for i in range(0, len(self.number)):
                bi_add_set = []
                tri_add_set = []
                add_ele = i * form_tot
                if add_ele in result_set:
                    continue
                for t in result_set:
                    bi_add_set += bi_comb(add_ele, t)
                for t in bi_res_set:
                    tri_add_set += tri_comb(add_ele,t)
                for ele in bi_add_set + tri_add_set:
                    if (ele in matric.keys()):
                        now_set.add(ele)
            #print("Add candidate: {}".format(bi_add_set + tri_add_set))

                    
        return now_rule, now_matric

    def find_number(self): #Get numberial rule according to unique.
        def gen_vector(number, form_tot):
            vector = [0 for i in range(0, len(self.number))]
            for ind, n in enumerate(number):
                now_form = int(n) // form_tot
                if ind == 0:
                    vector[now_form] = 1
                else:
                    vector[now_form] = -1
                #print("Generate Vector: #{} n:{} now_form:{}, now_vector:{}\n".format(ind, n, now_form, vector))
            return vector

        def gen_temp_vector(array, vector):
            temp = []
            for v in array:
                temp.append(v)
            temp.append(vector)
            return temp

        def gen_algorihtm_output_string(now_rule):
            output_string = ""
            
            rules = set()
            for u in self.unoperation:
                if u in self.ndf:
                    now_rule_entry = "direct " + str(u) + "\n"
                    if (not now_rule_entry in rules):
                        output_string += now_rule_entry
                        rules.add(now_rule_entry)
            
            for rule in now_rule:
                for n in rule:
                    nf = self.num_map[n].split("_")
                    now_rule_entry = ""
                    if (len(nf) == 2):
                        now_rule_entry = nf[1] + " " + nf[0] + "\n"
                    if (len(nf) == 3):
                        now_rule_entry = nf[1] + "i " + nf[0] + " " + nf[2] + "\n"
                        self.fid.add(nf[2])
                    if (not now_rule_entry in rules):
                        output_string += now_rule_entry
                        rules.add(now_rule_entry)
            
            for rule in now_rule:
                now_rule_entry = ""
                if (len(rule) > 1):
                    now_rule_entry += "diff " + str(len(rule)) + " "
                    for n in rule:
                        now_rule_entry += self.num_map[n] + " "
                    now_rule_entry += "\n"
                else:
                    now_rule_entry = "direct " + self.num_map[rule[0]] + "\n"
                if (not now_rule_entry in rules):
                    output_string += now_rule_entry
                    rules.add(now_rule_entry)
        

            return output_string



        if (len(self.ndf) == 0):
            return 
        
        if (self.level != "N"):
            output_string = ""
            for col in self.ndf:
                output_string += "direct " + str(col) + "\n"
            fo = open(self.output_path + self.template + "num.rule", 'w')
            fo.write(output_string)
            fo.close()
            return
        
        fo = open(self.working_path + self.mapping_path, 'r')
        for line in fo.readlines():
            key,value = line.split(":")
            try:
                self.num_map[key] = value.split('/')[-1].split(".")[0]
            except:
                print("Load mapping Error: {}".format(value))
        fo.close()
        
        form_tot = 3 + 2 * len(self.id)
        
        fo = open(self.working_path + "Metric.msg",'r')
        lines = fo.readlines()
        fo.close()
        for line in lines:
            key,value = line.strip().split(":")
            self.matric_dict[key] = float(value)
        
        now_rule,now_matric = self.gen_algorithm_rule(self.matric_dict, form_tot)

        for rule in now_rule:
            string = ""
            string_1 = ""
            for n in rule:
                string += self.num_map[n] + " "
                string_1 += str(n) + " "
        
        output_string = gen_algorihtm_output_string(now_rule)
        
        fo = open(self.output_path + self.template + "num.rule", 'w')
        fo.write(output_string)
        fo.close()

    def compress_trial(self, input_path, output_path):
        command = ""
        if (self.kernal == "7z"):
            command = "7za a " + output_path + " " + input_path + " -m0=LZMA"
        else:
            command = "tar -czf " + output_path + " " + input_path
        print(command)
        os.system(command)
    
    def gen_basic_info(self):
        ndf = []
        sdf = []
        if (len(self.df.columns) == 0):
            output_string = str(len(self.df.columns)) + " " + str(len(self.ndf)) + " " + str(len(self.sdf)) + "\n"
            fo = open(self.output_path + self.template + "basic.rule", 'w')
            fo.write(output_string)
            fo.close()
            print("No columns! Log: {}, Template: {}, numerial column count: {}, string column count: {}".format(self.type, self.template, str(len(self.ndf)), str(len(self.sdf))))
            return [],[]

        for column in self.df.columns:
            lens = len(self.df[column])
            count = 0
            for i in self.df[column]:
                if str(i).isdigit() and abs(int(i)) < (1 << 31) and (int(i) == float(i)): #Not float number, not overflow
                    count +=1
            if count == lens:
                ndf.append(column)
            else:
                sdf.append(column)

        operation_columns_number = int(len(self.df.columns) * (1-self.opt)) + 1
        o_uni = self.df.nunique()
        sort_column = list(o_uni.sort_values().iloc[0:].index)
        
        self.ndf = ndf
        self.sdf = sdf

        for n,i in enumerate(sort_column):
            if (n < operation_columns_number):
                self.unoperation.append(i)
            else:
                self.operation.append(i)

        self.number = list(set(ndf) - set(self.unoperation))
        self.string = list(set(sdf) - set(self.unoperation))
        self.id = self.gen_id()

        print("Log: {}, Template: {}, numerial column count: {}, string column count: {}".format(self.type, self.template, str(len(self.ndf)), str(len(self.sdf))))
        print("number: {}, string: {}, operation: {}, unoperation: {}".format(str(self.number), str(self.string), str(self.operation), str(self.unoperation)))

        output_string = str(len(self.df.columns)) + " " + str(len(self.ndf)) + " " + str(len(self.sdf)) + "\n"
        for i in ndf:
            output_string += str(i) + " "
        output_string += "\n"
        for i in sdf:
            output_string += str(i) + " "
        output_string += "\n"
        
        fo = open(self.output_path + self.template + "basic.rule", 'w')
        fo.write(output_string)
        fo.close()
        
        return self.ndf, self.sdf

    def gen_correlation_col(self, num_col, id_col):
        #Output id columns
        for col in id_col:
            util.list_write(self.working_path + str(col) + ".col", list(self.df[col]))
        #Output numerial columns
        for col in num_col:
            util.list_write(self.working_path + str(col) + '.col', list(self.df[col]))

        correlation_col = []
        form_tot = 3 + 2 * len(id_col)
        #Call C Calculate numberial columns up and down diff
        for n, col in enumerate(num_col):
            correlation_col.append(str(col))
            self.corr_map[str(col)] = n * form_tot
            call(util.genCall(self.path + "Rule/C/Numdiff", [self.working_path + str(col) + ".col", self.working_path + str(col) + "_up.col", self.working_path + str(col) + "_do.col"]), shell=True)
            correlation_col.append(str(col) + "_up")
            self.corr_map[str(col) + "_up"] = n * form_tot + 1
            correlation_col.append(str(col) + "_do")
            self.corr_map[str(col) + "_do"] = n * form_tot + 2

        #Call C Calculate numberial columns up and down diff according to id columns
        for n1, col in enumerate(num_col):
            for n2, iid in enumerate(id_col):
                call(util.genCall(self.path + "Rule/C/Iddiff", [self.working_path + str(col) + ".col", self.working_path + str(iid) + ".col", self.working_path + str(col) + "_up_" + str(iid) + ".col", self.working_path + str(col) + "_do_" + str(iid) + ".col"]), shell=True)
                correlation_col.append(str(col) + "_up_" + str(iid))
                self.corr_map[str(col) + "_up_" + str(iid)] = n1 * form_tot + 3 + n2 * 2
                correlation_col.append(str(col) + "_do_" + str(iid))
                self.corr_map[str(col) + "_do_" + str(iid)] = n1 * form_tot + 3 + n2 * 2 + 1

        correlation_col = sorted(correlation_col, key = lambda x: self.corr_map[x])
        
        fo = open(self.working_path + self.mapping_path, "w")
        
        for col in correlation_col:
            fo.write(str(self.corr_map[col])+":"+ self.working_path + col + ".col" + "\n")
            self.num_map[str(self.corr_map[col])] = col
        fo.close()

        return correlation_col

    def gen_id(self):
        id_col = []
        for n,col in enumerate(self.df.columns):
            if (len(id_col) >= 3):
                break
            if (col in self.string):
                id_col.append(col)

        print("string_col: {}, id_col: {}".format(str(self.string), str(id_col)))
        return id_col

    def gen_numberial(self): #Generate single.msg and unique.msg
        def get_group(col):
            group = -1
            original = col.split("_")[0]
            if (len(col.split("_")) < 3):
                group =  -1
            else:
                group = col.split("_")[2]
            return original, group

        id_col = self.id
    
        correlation_col = self.gen_correlation_col(self.number, id_col)

        corr_map = self.corr_map

        diff_two = []
        diff_three = []
        diff_two_size = 0
        diff_three_size = 0
        tot_size = len(correlation_col)

        fo = open(self.working_path + "diff_two.msg", "w")
        for col1 in correlation_col:
            for col2 in correlation_col:
                n1 = self.corr_map[col1]
                n2 = self.corr_map[col2]
                o1,g1 = get_group(col1)
                o2,g2 = get_group(col2)
                if (g1 != -1 and g2 != -1 and g1 != g2): continue
                if (n2 >= n1): continue
                if (o1 == o2): continue
                diff_two.append(self.working_path + str(corr_map[col1]) + "-" + str(corr_map[col2]))
                fo.write(str(corr_map[col1]) + " " + str(corr_map[col2]) + "\n")
                diff_two_size += 1
        fo.close()
        
        fw = open(self.working_path + "diff_three.msg", "w")
        if (self.metric == 0):
            for col1 in correlation_col:
                for col2 in correlation_col:
                    for col3 in correlation_col:
                        o1, g1 = get_group(col1)
                        o2, g2 = get_group(col2)
                        o3, g3 = get_group(col3)
                        n1 = self.corr_map[col1]
                        n2 = self.corr_map[col2]
                        n3 = self.corr_map[col3]
                        if (n2 == n1 or n3 == n1): continue
                        if (n3 >= n2): continue
                        if (o1 == o2 or o2 == o3 or o1 == o3): continue
                        if (g1 != g2 or g1 != g3 or g2 != g3): continue
                        diff_three.append(self.working_path + str(corr_map[col1]) + "-" + str(corr_map[col2]) + "-" + str(corr_map[col3]))
                        fw.write(str(corr_map[col1]) + " " + str(corr_map[col2]) + " " + str(corr_map[col3]) + "\n")
                        diff_three_size += 1
        fw.close()

        tot_size += diff_two_size + diff_three_size
        print("Diff two Size: {}, Diff three Size: {}, Tot Size: {}".format(diff_two_size, diff_three_size, tot_size))
        
        metricTime = util.timeCount("Metric time")
        metricTime.countBegin()
        res = call(util.genCall(self.path + "./Rule/C/Entropy", [self.working_path + "Metric.msg", self.working_path + self.mapping_path, self.working_path + "diff_two.msg", self.working_path + "diff_three.msg"]), shell=True)
        if (res != 0):
            print("Calculate Metric Error!")
            exit(0)
        
        metricTime.countEnd()
        metricTime.output()
    
    def find_basic_rule(self):
        output_string = ""
        for col in self.ndf:
            output_string += "direct " + str(col) + "\n"
        fo = open(self.output_path + self.template + "num.rule", 'w')
        fo.write(output_string)
        fo.close()
        
        output_string = ""
        for col in self.sdf:
            output_string += "direct " + str(col) + "\n"
        fo = open(self.output_path + self.template + "string.rule", 'w')
        fo.write(output_string)
        fo.close()    


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-I", "--Input", help="Input sampel dictionary(E*_parameter.csv)")
    parser.add_argument("-Tp", "--Type", help="The type of log")
    parser.add_argument("-E", "--Template", help="The template of log")
    parser.add_argument("--OpT", type=float, default=0.8, help="The threshold of operation")
    parser.add_argument("--WorkingPath", "-W", default="./tmp/", help="The output path")
    parser.add_argument("--Output", "-O", help="The message file output path")
    args = parser.parse_args()

    timer = util.timeCount("Tot Time")
    timer.countBegin()
    df = pd.read_csv(args.Input)

    log_type = args.Type
    log_template = args.Template
    working_path = args.WorkingPath
    output = args.Output
    kerner = "7z"
    finder = RuleFinder(df, log_type, log_template, 0.8, working_path, output)
    finder.gen_basic_info()

    finder.find_number()
    
    timer.countEnd()
    timer.output()
