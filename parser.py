import re
import os
import json
import numpy as np
import pandas as pd
from datetime import datetime
from util import timeCount
import util
import random
from findrule import RuleFinder
from itertools import combinations, permutations

class Logcluster:
    def __init__(self, logTemplate='' ,Eid='E0'):#E0 means no match 
        self.logTemplate = logTemplate
        self.Eid = Eid

class Node:
    def __init__(self, childD=None, depth=0, digitOrtoken=None):
        if childD is None:
            childD = dict()
        self.childD = childD
        self.depth = depth
        self.digitOrtoken = digitOrtoken

class LogParser:
    def __init__(self, template_path="", Type='LOG', depth=4, maxChild=100, st=0.6, correlation_threshold=600, level="A"):
        """
        Attributes
        ----------
            depth : depth of all leaf nodes
            st : similarity threshold
            outdir : the output path stores the file containing structured logs
            correlation_threashold : the threashold to find correlation
            mode: Training or Applying
            level: A for all tech(number, workflow), N only for number, W only for workflow, 0 for nothing
        """
        self.depth = depth - 2
        self.st = st
        self.maxChild = maxChild

        self.template_path = template_path

        self.type = Type
        
        self.splitregex = re.compile(r'(\s+|:|<\*>|,|=)')
        self.threshold = correlation_threshold
        self.level = level
        self.template_dict = dict()
    
    def parseUnityEntry(self, log_dataframe):
        """
        the unity entry of all parse and match function,
        @param log_dataframe : the log need to match ,if there is no sample ,this file will be parsed
        @param size : the sample size, size%of the dataframe
        @return : is still not comfirmed, maybe a dataframe of all messages and a dict of parameters
        """
        event, templates = self.parse(log_dataframe) #Get templates
        #print(templates)
        # print("Tree is: ")
        # self.printTree(self.rootNode, 0)
        eventNum = self.match(log_dataframe, event) #Get rules
        self.out_templates(templates, eventNum) #Output Template

        return
    def parse(self, log_dataframe):
        """
        the parce method 
        parse sample data,get templates and build a match tree
        
        @param log_dataframe : the sample dataframe used for parse
        @return : a dataframe that have been parsed , but is useless now
        
        PS:preprocess is a O(n) algorithm ,the spend is the token count of all logs, 
            so if the test result show that the preprocess indeed cost too much ,
            maybe we can do this part in the parse procedure
        """
        def getTemplate(seq1, seq2):
            assert len(seq1) == len(seq2)
            retVal = []
            for n, word in enumerate(seq2):
                if word == seq1[n]:
                    retVal.append(word)
                else:
                    retVal.append('<*>')
            return retVal

        def preprocess(line):
            compile_rule = re.compile(r'\d+[\.]\d+[\.]\d+[\.]\d+') #IP Address
            for index, item in enumerate(line):
                if str.isdigit(item):
                    line[index]="<*>"
                if compile_rule.match(item):
                    line[index]="<*>"
            return line

        def addSeqToPrefixTree(rn, logClust):
            def hasNumbers(s):
                return any(char.isdigit() for char in s)
            
            def insert(parent, token, currentDepth):
                newNode = Node(depth=currentDepth + 1, digitOrtoken=token)
                parent.childD[token] = newNode
                return newNode
                
            seqLen = len(logClust.logTemplate)
            if seqLen not in rn.childD:
                firtLayerNode = Node(depth=1, digitOrtoken=seqLen)
                rn.childD[seqLen] = firtLayerNode
            else:
                firtLayerNode = rn.childD[seqLen]

            parentn = firtLayerNode

            currentDepth = 1
            for token in logClust.logTemplate:

                #Add current log cluster to the leaf node
                if currentDepth >= self.depth or currentDepth >= seqLen:
                    if len(parentn.childD) == 0:
                        parentn.childD = [logClust]
                    else:
                        parentn.childD.append(logClust)
                    break

                #If token not matched in this layer of existing tree. 
                if token not in parentn.childD:
                    if not hasNumbers(token) and len(parentn.childD) + 1 < self.maxChild:
                        parentn = insert(parentn, token, currentDepth)
                    else:
                        if '<*>' in parentn.childD:
                            parentn = parentn.childD['<*>']
                        else:
                            parentn = insert(parentn, "<*>", currentDepth)
                #If the token is matched
                else:
                    parentn = parentn.childD[token]

                currentDepth += 1
                
        print('Parsing dataframe')
        start_time = datetime.now()
        self.rootNode = Node()
        rootNode=self.rootNode

        logCluL = []
        logEvent = []

        for idx, line in log_dataframe.iterrows():
            #print("Now process: {}".format(line))
            logmessageL = self.splitregex.split(line['Content'].strip())
            logmessageL = preprocess(logmessageL)
            logmessageL = list(filter(lambda x: x != '',logmessageL))#remove the empty items
            matchCluster = self.treeSearch(rootNode, logmessageL)
            matchEid = ""
            #Match no existing log cluster
            if matchCluster is None:
                Eid='E'+str(len(logCluL)+1)
                matchEid = Eid
                newCluster = Logcluster(logTemplate=logmessageL, Eid=Eid)
                logCluL.append(newCluster)
                addSeqToPrefixTree(rootNode, newCluster)
               
            #Add the new log message to the existing cluster
            else:
                matchEid = matchCluster.Eid
                newTemplate = getTemplate(logmessageL, matchCluster.logTemplate)
                if ''.join(newTemplate) != ''.join(matchCluster.logTemplate): 
                    matchCluster.logTemplate = newTemplate
            
            logEvent.append(matchEid)

        print('Parsing step1 done. [Time taken: {!s}]'.format(datetime.now() - start_time))
        
        template_mapping = dict()
        for t in logCluL:
            template = "".join(t.logTemplate)
            self.template_dict[t.Eid] = t.logTemplate
            template_mapping[t.Eid]= template
        
        print('Parsing step2 done. [Time taken: {!s}]'.format(datetime.now() - start_time))
        
        return logEvent, template_mapping
    
    def match(self, log_dataframe, event):#return a list of dataframe, sorted by Eid
        def get_new_rule(rules):
            new_rule = []
            direct = ""
            count = 0
            for li in rules:
                if (li.strip().split()[0] != "direct"):
                    new_rule.append(li)
                    continue
                for t in li.strip().split()[1:]:
                    direct += " " + t
                    count += 1
            new_rule.append("direct_m " + str(count) + direct)
            return new_rule
        
        def output_rule(rules, path):
            fo = open(path, "w")
            for r in rules:
                fo.write(r + "\n")
            fo.close()
            
        """
        the singleprocess match fuction
        match all logs ,and output the needed informations

        @param log_dataframe : the dataframe with all logs needed to match 
        @return :a list of parameter lists,which is useless now
        """
        print('Matching dataframe')
        start_time = datetime.now()
        self.state='Match'
        #print(self.template_dict)
        length = len(log_dataframe)
        #print(log_dataframe['Content'])
        log_dataframe['EventId'] = event
        s = log_dataframe.apply(lambda x: self.matchapply(x['Content'], x['EventId']), axis = 1)
        log_dataframe['ParameterList'] = s.apply(lambda s:s[1])
        #print(log_dataframe['ParameterList'])
        print('Matching done. [Time taken: {!s}]'.format(datetime.now() - start_time))
        
        
        eventNum=dict()
        
        allVariables = dict()
        strInfo = dict()
        numInfo = dict()
        #For each templates, generating tempaltes correlations. And Get all string varibales
        print("Start numerical correlation mining..")
        print(np.unique(event))
        for E in np.unique(event):
            #Search possible correlation for each template
            count = 0
            for e in event:
                if (e == E):
                    count += 1
            eventNum[E] = count
            dftemp=(log_dataframe[log_dataframe['EventId'].isin([E])])['ParameterList'].apply(pd.Series)
            working_path = self.template_path + str(E) + "tmp/"
            if not os.path.exists(working_path):
                os.makedirs(working_path)
            
            finder = RuleFinder(Input=dftemp, Type=self.type, Template=str(E), Opt=0.8, WorkingPath=working_path, Output=self.template_path,  level=self.level)
            num_var, str_var = finder.gen_basic_info()
            strInfo[E] = str_var
            numInfo[E] = num_var
            num_tot = len(num_var)
            str_tot = len(str_var)
            if (num_tot + str_tot == 0): #No Variables
                os.system("rm -rf "+ working_path)
            else:
                if (num_tot > 32): #Too much variables or level equals B
                    finder.find_basic_rule()
                else:
                    #print("Level: {}".format(self.level))
                    if (self.level == "N"):
                        finder.gen_numberial()
                    finder.find_number()
            
            for var in str_var:
                key = str(var) + "@" + E
                allVariables[key] = list(dftemp[var])
        os.system("rm -rf "+ working_path)
            
        #Output other varibales
        RemainVar = set(allVariables.keys())
        for E in np.unique(event):
            output_path = self.template_path + E + "string.rule"
            fw = open(output_path, "w")
            for st in strInfo[E]:
                key = str(st) + "@" + E
                if (key in RemainVar):
                    fw.write("direct " + str(st) + "\n")
            fw.close()
                    
        with open(os.path.join(self.template_path, "eventNum.json"), "w") as fw:
            json.dump(eventNum, fw)

        print('Matching step3(output) done. [Time taken: {!s}]'.format(datetime.now() - start_time))
        return eventNum

    def treeSearch(self, rn, seq):
        def fastMatch(logClustL, seq):
            def seqDist(seq1, seq2):
                assert len(seq1) == len(seq2)
                simTokens = 0
                numOfPar = 0

                for token1, token2 in zip(seq1, seq2):
                    if token1 == '<*>':
                        numOfPar += 1
                        continue
                    if token1 == token2:
                        if token1 == '\t' or token1.isspace() or token1 == ':' or token1 == ',' or token1 == '=' or token1 == '\n':
                            continue
                        simTokens += 1
                        continue 
                    if token1 == '\t' or token1.isspace() or token1 == ':' or token1 == ',' or token1 == '=' or token1 == '\n':
                        return -1, 0

                retVal = float(simTokens) / len(seq1)

                return retVal, numOfPar

            retLogClust = None

            maxSim = -2
            maxNumOfPara = -1
            maxClust = None

            for logClust in logClustL:
                curSim, curNumOfPara = seqDist(logClust.logTemplate, seq)
                if curSim>maxSim or (curSim==maxSim and curNumOfPara>maxNumOfPara):
                    maxSim = curSim
                    maxNumOfPara = curNumOfPara
                    maxClust = logClust
        
            if maxSim >= self.st:
                retLogClust = maxClust 
 
            return retLogClust

        retLogClust = None

        seqLen = len(seq)
        if seqLen not in rn.childD:
            return retLogClust

        parentn = rn.childD[seqLen]
        nodeStack = []
        nodeStack.append(parentn)
        while(nodeStack):
            tempnode = nodeStack.pop()
            tempdepth = tempnode.depth#depthStack.pop()

            if tempdepth >= self.depth or tempdepth >= seqLen:
                retLogClust = fastMatch(tempnode.childD,seq)
                if retLogClust:
                    return retLogClust
                continue
                
            if seq[tempdepth-1] in tempnode.childD:
                nodeStack.append(tempnode.childD[seq[tempdepth-1]])
                continue
            if '<*>' in tempnode.childD:
                nodeStack.append(tempnode.childD['<*>'])


        return retLogClust

    def matchapply(self, line, eid):
        def get_parameter(logmessageL, template):
            ParameterList = []
            i=0
            for token in template:
                if token  =="<*>" :
                    ParameterList.append(logmessageL[i])
                i += 1
            return ParameterList
        
        logmessageL = self.splitregex.split(line.strip())
        logmessageL = list(filter(lambda x: x != '',logmessageL))#remove the empty items
        #print("LogmessageL: {}, \ntempalte: {}\n".format(logmessageL, self.template_dict[eid]))
        #print("LogmessageL Length: {}, \ntempalte Length: {}\n".format(len(logmessageL), len(self.template_dict[eid])))
        assert(len(logmessageL) == len(self.template_dict[eid]))
        return eid , get_parameter(logmessageL, self.template_dict[eid])


    
    def out_templates(self, templates, eventNum):
        """
        output the templates_map
        @param logClustL : list of all templates
        """
        tot = 0
        for key in eventNum.keys():
            tot += eventNum[key]

        with open(os.path.join(self.template_path, "template_mapping.json"), "w") as fw:
            json.dump(templates, fw)

        with open(os.path.join(self.template_path, "template.col"), "w") as f:
            f.write("[0]\n")

            for key in templates:
                if (eventNum[key] == 1):
                    continue
                i = key.split("E")[1]
                for token in templates[key]:
                    f.write(token)
                f.write("\n")
                f.write("[{}]\n".format(i))

            #f.write("[{}]\n".format(i))


    def printTree(self, node, dep):
        pStr = ''   
        for i in range(dep):
            pStr += '\t'

        if node.depth == 0:
            pStr += 'Root'
        elif node.depth == 1:
            pStr += '<' + str(node.digitOrtoken) + '>'
        else:
            pStr += node.digitOrtoken

        if node.depth == self.depth:
            return 1
        print(pStr)
        for child in node.childD:
            self.printTree(node.childD[child], dep+1)
    
