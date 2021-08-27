import argparse
import re
import os
import time
import util
import sys
import json
from subprocess import call
splitregex = re.compile(r'(\s+|:|<\*>|,|=)')

_TemplateLevel = 0
_DiffLevel = 1
_EncoderLevel = 2

def load_tempaltes(template_path):
    Template_file = open(template_path,encoding="ISO-8859-1")
    Template_lines = Template_file.readlines()
    Templates = {}
    Index = {}
    now_template = []
    now_index = []
    first = True
    for line in Template_lines:
        if (re.search('\[\d+\]\\n',line) and len(line) < 10):
            num = int(line[1:-2])
            if (num == 0):
                continue
            Templates[num] = now_template
            Index[num] = now_index
            now_template = []
            now_index = []
            first = True
            continue
        
        if (first):
            first = False
        else:
            now_template.append('\n')
        for n, s in enumerate(splitregex.split(line.strip())):
            if (s == "<*>"):
                now_index.append(n)
            now_template.append(s)
    # num = num + 1
    # Templates[num] = now_template
    # Index[num] = now_index
    #print(Templates[19])
    return Templates, Index

def get_info(head_path):
    #Head length 9
    #is multiple 1
    #Head regex "\d{4}-\d{2}-\d{2}"
    #Head type map dict[0] = [1, 0], dict[7] = [6,7]
    #Head format map dict[0] = "%s", dict[7] = "%d-%d-%d-%d.%d.%d.%d"
    #Head string length dict[0] = [-1], dict[7] = [1,1,1,1,1,1]
    #Head number length dict[0] = [], dict[7] = [4,2,2,2,2,2,6]
    #Head delimer []
    fo = open(head_path, 'r')
    headLength = int(fo.readline().strip())
    isMulti = int(fo.readline().strip())
    if isMulti == 1:
        headRegex = fo.readline().strip()
    else:
        headRegex = None
    headType = dict()
    headFormat = dict()
    headString = []
    headNumber = []
    headDelimer = []
    for i in range(0, headLength):
        parts = fo.readline().strip().split()
        headType[i] = [int(parts[0]), int(parts[1])]
        headFormat[i] = parts[2]
        if (headType[i][0] == 1 and headType[i][1] == 0): #only string
            headNumber.append(-1)
            continue
        for t in range(0, headType[i][0]):
            headString.append(int(parts[3 + t]))
        for t in range(0, headType[i][1]):
            headNumber.append(int(parts[3 + headType[i][0] + t]))
    for i in range(0, headLength):
        headDelimer.append(fo.readline()[0:-1])
    return headLength, isMulti, headRegex, headType, headFormat, headString, headNumber, headDelimer
    
def restore_heads(path, tempalte_path, level_msg):
    def restore_diff(array,is_diff):
        if not is_diff:
            return array
        new_array = []
        new_array.append(array[0])
        now_size = array[0]
        for n in range(1, len(array)):
            now_size += array[n]
            new_array.append(now_size)
        return new_array
   
    def restore_dict(array, dict):
        new_array = []
        for i in array:
            new_array.append(dict[i])
        return new_array
    
    def build_dict(array):
        dic = dict()
        for idx, a in enumerate(array):
            dic[idx] = a
        return dic

    def getValue():
        headValue = {}
        numSize = 0
        for i in range(0, headLength):
            if (headType[i][0] == 1 and headType[i][1] == 0): #Only string
                headValue[numSize] = restore_diff(util.load_array(util.decoder(path + "Head" + str(numSize) + ".head", is_encoder)), is_diff)
                numSize += 1
                continue
            for t in range(0, headType[i][1]): #Number and string
                headValue[numSize] = restore_diff(util.load_array(util.decoder(path + "Head" + str(numSize)  + ".head",is_encoder)), is_diff)
                numSize += 1
        return headValue
    
    def pendding(num, length):
        if length == -1:
            return str(num)
        else:
            temp = str(num)
            while len(temp) < length:
                temp = "0" + temp
            return temp
        
    is_diff = level_msg[_DiffLevel]
    is_encoder = level_msg[_EncoderLevel]
    headLength, isMulti, headRegex, headType, headFormat, headString, headNumber, headDelimer = get_info(os.path.join(template_path, "head.format"))
    print(headType)
    HeadDict = build_dict(util.load_array(os.path.join(path,"Header_dictionary.headDict"), False))
    HeadValue = getValue()
    totLength = len(HeadValue[0])
    for key in HeadValue.keys():
        assert(len(HeadValue[key]) == totLength)
    now_num = 0
    now_str = 0
    Heads = ["" for i in range(0, totLength)]
    #Restore each part
    for i in range(0, headLength):
        if (headType[i][0] == 1 and headType[i][1] == 0):
            print(i)
            print(now_num)
            #print(HeadValue[8])
            part = restore_dict(HeadValue[now_num], HeadDict)
            Heads = [Heads[t] + part[t] + headDelimer[i] for t in range(0, totLength)]
            now_num += 1
            continue
        now_format = headFormat[i]
        fidx = 0
        while fidx < len(now_format):
            if (now_format[fidx] == '%' and now_format[fidx+1] == 'd'): #Num
                try:
                    Heads = [Heads[t] + pendding(HeadValue[now_num][t],headNumber[now_num]) for t in range(0, totLength)]
                except:
                    print("now_num:{}, len(headValue):{}, len(Heads):{}, len(headValue):{}, tot_length:{}, len(headNumber): {}".format(now_num, len(HeadValue), len(Heads),len(HeadValue[now_num]), totLength, len(headNumber)))
                    exit(-1)
                now_num += 1
                fidx += 2
            else: #String start
                Heads = [Heads[t] + now_format[fidx] for t in range(0, totLength)]
                fidx += 1
        Heads = [Heads[t] + headDelimer[i] for t in range(0, totLength)]
    return Heads, [isMulti, headRegex]

def restore_variables(template_path, file_path, tid, level): #Restore varibales for each template
    variables = {}
    template_level = level[_TemplateLevel]
    is_encoder = level[_EncoderLevel]
    
    def get_str_variables(strVariables, variables):
        for var in strVariables:
            if (not os.path.exists(file_path + tid + "_" + var + ".str")):
                print("File:{} does not exist".format(file_path + tid + "_" + var + ".str"))
                return variables
            lines = util.load_array(file_path + tid + "_" + var + ".str", False)
            variables[var] = lines
    
    def get_num_variables(numVariables, variables):
        for var in numVariables:
            if (not os.path.exists(file_path + tid + "_" + var + ".dat")):
                print("File:{} does not exist".format(file_path + tid + "_" + var + ".dat"))
                return variables
            array = util.load_array(util.decoder(file_path + tid + "_" + var + ".dat", is_encoder), False)
            variables[var] = array
        
    #Read basic rules
    basicRules = util.load_array(os.path.join(template_path, tid + "basic.rule"), False)
    [v_tot, n_tot, s_tot] = basicRules[0].split()
    numVariables = []
    strVariables = []
    if (int(v_tot) == 0):
        return variables 
    if (int(n_tot) != 0):
        numVariables = basicRules[1].split()
    if (int(s_tot) != 0):
        strVariables = basicRules[2].split()
    #String variables
    get_str_variables(strVariables, variables)
    #Numerial variables
    get_num_variables(numVariables, variables)
    return variables
    

def load_log(path, head_msg):
    Logs = []
    if (head_msg[0] == 1): #Is multiline
        headregex = re.compile(head_msg[1])
    lines = util.load_log(path)
#    lines = util.load_array(path, False)
    now_line = ""
    start = True 
    #print(len(lines))
    for line in lines:
        lined = line.decode()
        if (head_msg[0] != 1 or headregex.search(lined)):
            if (start):
                start = False
                now_line = lined.strip()
                continue
            
            Logs.append(now_line)
            now_line = lined.strip()
        else:
            #print("read failed at: " + lined + " Now line is" + now_line)
            #print(line)
            if (start):
                Logs.append(lined.strip())
                continue
            if (lined == "\n"):
                Logs.append(now_line)
                Logs.append(lined.strip())
                #print("append now line at: " + now_line + " lined at: ")
                #Logs.append(now_line)
                start = True
                continue
            
            now_line += '\n' + lined.strip()
        
    Logs.append(now_line)
    return Logs


    
if __name__ == "__main__":
    #Target, success restore 0-D-Z state
    parser = argparse.ArgumentParser()
    parser.add_argument("--Input", "-I", help="The input .7z file")
    parser.add_argument("--Output", "-O", default="./restore_result/", help="The output path of the restored log file")
    parser.add_argument("--Template", "-T",  help="The template path")
    parser.add_argument("--Temp", "-t", default="./temp/", help="The temp output path")
    parser.add_argument("--TemplateLevel", "-TL", default="0", choices=["0","N"], help="The level of tempalte")
    parser.add_argument("--DiffLevel", "-D", default="D", choices=["ND", "D"], help="The level of diff")
    parser.add_argument("--EncoderLevel", "-E", default="Z", choices=["NZ", "Z"], help="The level of encoder")
    args = parser.parse_args()

    template_path = args.Template
    input_path = args.Input
    output_path = args.Output
    temp_path = args.Temp
    
    #level_msg -> [TemplateLevel, DiffLevel, EncoderLevel]
    level_msg = []
    if (args.TemplateLevel == "N"):
        level_msg.append(True)
    else:
        level_msg.append(False)
    if (args.DiffLevel == "D"):
        level_msg.append(True)
    else:
        level_msg.append(False)
    if (args.EncoderLevel == "Z"):
        level_msg.append(True)
    else:
        level_msg.append(False)
    
    print(level_msg)


    if (not os.path.exists(temp_path)):
        os.mkdir(temp_path)
    else:
        call("rm -rf " + temp_path, shell=True)
        os.mkdir(temp_path)
    
    #Load templates
    Templates, Index = load_tempaltes(os.path.join(template_path, "template.col"))
    #print(Templates)
    #print(Index)
    
    #Decompression into temp 
    call("7za x " + input_path + " -o" + temp_path, shell=True)
    
    #Restore Head
    Heads, head_msg = restore_heads(temp_path, template_path, level_msg)
    #print(Heads[0:100])
    
    #Restore individual templates variables
    Varibales = dict()
    for key in Templates.keys():
        #print(key)
        Varibales[key] = restore_variables(template_path, temp_path, "E" + str(key), level_msg)
    #print(Varibales[1])
    
    #Load load_failed log
    LoadFailed = load_log(os.path.join(temp_path, "load_failed.log"), head_msg)
    
    #Load match_failed log
    MatchFailed = load_log(os.path.join(temp_path, "match_failed.log"), head_msg)
    
    print("Load failed #: {}, Match failed #:{}".format(len(LoadFailed), len(MatchFailed)))
    #Use Eid, load_failed, match_failed, Tempaltes, Index, Variables
    match_idx = 0
    load_idx = 0
    head_idx = 0
    template_idx = {}
    for t in Templates.keys():
        template_idx[t] = 0

    Eids = util.load_array(util.decoder(os.path.join(temp_path,"Eid.eid"), level_msg[_EncoderLevel]))
    fw = open(output_path, "wb")
    for eid in Eids:
        if (eid == -1):
            fw.write((LoadFailed[load_idx] + '\n').encode())
            #print(load_idx)
            load_idx += 1
            continue
        if (eid == 0):
            #try:
            fw.write((MatchFailed[match_idx] + '\n').encode())
            #except:
            #print("tot length: {}, match index: {}".format(len(MatchFailed), match_idx))
            match_idx += 1
            continue
        
        #Fill up
        head = Heads[head_idx]
        head_idx += 1
        template = Templates[eid]
        variables = Varibales[eid]
        now_idx = template_idx[eid]
        now_var = 0
        fs = head

        for s in template:
            if (s == "<*>"):
                fs += str(variables[str(now_var)][now_idx])
                now_var += 1
            else:
                fs += s
        fw.write((fs + '\n').encode())
        template_idx[eid] += 1

    fw.close()
    #Clean up
    #call("rm -rf " + temp_path + "*", shell=True)
