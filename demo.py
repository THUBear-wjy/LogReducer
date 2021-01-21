import logloader
import argparse
import re
import os
import time
import util
import sys
import parser as TemplateParser
import header
if __name__ == "__main__":
    t1 = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument("--Input", "-I", help="The input log sample")
    parser.add_argument("--Template", "-T", help="The path of templates and templates")
    parser.add_argument("--Type", "-Tp",  help="The type of log (Op Hdfs Windows Pangu Tubo Shennong Rpc Presto Sys Metering Monitor Ols)")
    parser.add_argument("--Level", "-l", default="0", choices=["0", "N"], help="The level of template(0 or N)")

    parser.add_argument("--Samilarity", "-s", default="0.1", help="The samilarity of template")
    
    args = parser.parse_args()

    filepath = args.Input
    Type = args.Type
    Level = args.Level
    Samilarity = float(args.Samilarity)
    log_format = ""

    template_path = util.path_pro(args.Template)
    if not (os.path.exists(template_path)):
        os.mkdir(template_path)        
    
    if Type == "LogA" or Type == "LogB" or Type == "LogG" or Type == "LogH" or Type == "LogJ" or Type == "LogM" or Type == "LogN" or Type == "LogP" or Type == "LogR":
        head_length = 4
        is_multi = True
        head_regex = r"\[\d{4}\-\d{2}\-\d{2}"
    if Type == "LogC" or Type == "LogD" or Type == "LogE" or Type == "LogF" or Type == "LogO":
        head_length = 3
        is_multi = True
        head_regex = r"\[\d{4}\-\d{2}\-\d{2}"
    if Type == "LogI" or Type == "LogK":
        head_length = 2
        is_multi = True
        head_regex = r"\[\d{4}\-\d{2}\-\d{2}"
    if Type == "LogL":
        head_length = 1
        is_multi = False
        head_regex = None
    if Type == "LogQ":
        head_length = 4
        is_multi = False
        head_regex = None

    if Type == "Apsara" or Type == "Fastcgi" or Type == "Oss" or Type == "Pangu" or Type == "Presto" or Type == "Rpc" or Type == "Shennong" or Type == "Sla" or Type == "Tubo":
        head_length = 4 #<Date> <Time> <Level> <Pid> <Content>
        is_multi = True
        head_regex = r"\[\d{4}\-\d{2}\-\d{2}"
    if Type == "Metering" or Type == "Monitor" or Type == "Ols" or Type == "Op" or Type == "Sinput":
        head_length = 3 #<Date> <Time> <Pid> <Content>
        is_multi = True
        head_regex = r"\[\d{4}\-\d{2}\-\d{2}"
    if Type == "PAudit" or Type == "PSummary":
        head_length = 2 #<Date> <Time> <Content>
        is_multi = True
        head_regex = r"\[\d{4}\-\d{2}\-\d{2}"
    if Type == "Request" or Type == "Server":
        head_length = 1 #<Time> <Content>
        is_multi = False
        head_regex = None
    if Type == "Sys":
        head_length = 4
        is_multi = False
        head_regex = None
    
    if Type == "Android":
        head_length = 5
        is_multi = False
        head_regex = None
    if Type == "Apache":
        head_length = 6
        is_multi = True
        head_regex = r"\[\w{3}"
    if Type == "Bgl":
        head_length = 9
        is_multi = False
        head_regex = None
    if Type == "Hadoop":
        head_length = 3
        is_multi = True
        head_regex = r"\d{4}\-\d{2}\-\d{2}"
    if Type == "Hdfs":
        head_length = 4
        is_multi = False
        head_regex = None
    if Type == "Healthapp":
        head_length = 3
        is_multi = False
        head_regex = None
    if Type == "Hpc":
        head_length = 2
        is_multi = False
        head_regex = None
    if Type == "Linux":
        head_length = 3
        is_multi = False
        head_regex = None
    if Type == "Mac":
        head_length = 3
        is_multi = False
        head_regex = None
    if Type == "Openstack":
        head_length = 5
        is_multi = False
        head_regex = None
    if Type == "Proxifier":
        head_length = 2
        is_multi = False
        head_regex = None
    if Type == "Spark":
        head_length = 3
        is_multi = True
        head_regex = r"\d{2}\/\d{2}\/\d{2}"
    if Type == "Ssh":
        head_length = 5
        is_multi = False
        head_regex = None
    if Type == "Thunderbird":
        head_length = 8
        is_multi = False
        head_regex = None
    if Type == "Windows":
        head_length = 4
        is_multi = False
        head_regex = None
    if Type == "Zookeeper":
        head_length = 4
        is_multi = False
        head_regex = None

    max_length = 300000

    loader = logloader.LogLoader(headLength=head_length, isMulti=is_multi, headRegex=head_regex, maxLength=max_length)
    LogData, Heads, HeadDelimers = loader.load_to_dataframe(filepath)
    # print(LogData)
    # print(Heads)
    # print(HeadDelimers)
    #Extract head format
    Header = header.Header(head_length = head_length, is_multi=is_multi, head_bound=head_regex, template_path = template_path, heads = Heads, delimer=HeadDelimers)
    Header.outputFormat()

    #Extract templates and corrlation
    Parser = TemplateParser.LogParser(
        template_path=template_path, Type=Type, 
    depth=8, maxChild=100, st=Samilarity, correlation_threshold=600, level=Level)
    Parser.parseUnityEntry(log_dataframe=LogData)

    t2 = time.time()
    print('all time taken {:.2f}s'.format(t2-t1))
