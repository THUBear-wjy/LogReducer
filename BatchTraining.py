import sys
import os
from subprocess import call
import argparse
import util
import time
#Types = ["Android"]
#Types = ["Android","Apache","Bgl","Hadoop","Hdfs","Hpc","Healthapp","Linux","Mac","Openstack","Proxifier","Spark","Ssh","Windows","Zookeeper","Thunderbird"]
#Types = ["Apsara", "Fastcgi", "Metering", "Monitor", "Ols", "Op", "Oss", "Pangu", "PAudit", "Presto", "PSummary", "Request", "Rpc", "Server", "Shennong", "Sinput", "Sla", "Sys", "Tubo"]
Types = ["LogA", "LogB", "LogC", "LogD", "LogE", "LogF", "LogG", "LogH", "LogI", "LogJ", "LogK", "LogL", "LogM", "LogN", "LogO", "LogP", "LogQ", "LogR"]
#Path = "./"
Path = "./"
if __name__ == "__main__":
    parse = argparse.ArgumentParser()
    parse.add_argument("--LogPath", "-I", help="The path of logs")
    parse.add_argument("--TemplatePath", "-T", help="The output path of log template")
    parse.add_argument("--TemplateLevel", "-TL", default="0", choices=["0", "N"], help="The template level.")
    parse.add_argument("--Mode", "-m", default="Nor", choices=["Nor", "Sam"], help="The mode of training. Nor for single large file, Sam for sample file")
    parse.add_argument("--SampleRate", "-R", default="0.001", help="Sample rate, default by 0.001")
    args = parse.parse_args()

    input_path = util.path_pro(args.LogPath)
    template_level = args.TemplateLevel
    mode = args.Mode
    template_path = util.path_pro(args.TemplatePath) 
    sample_rate = args.SampleRate
    if not os.path.exists(template_path):
        os.mkdir(template_path)
    error_num = 0
    dic = {}
    times = {}
    tot_start = time.time()
    for t in Types:
#        command = "python3 /THULR/demo.py "
        print("Start training: {}".format(t))
        s_time = time.time()
        if (mode == "Nor"):
            command = "python3 " + Path + "sampler.py "
            command += input_path + t + ".log "
            command += input_path + t + ".sample "
            command += str(sample_rate)
        
            res = call(command, shell=True)
            if (res != 0):
                print("Sample Error Occur at: {}".format(t))
                error_num += 1
                dic[t] = "Sample Error"
                continue

        command = "python3 " + Path + "demo.py "
        command += "-I " + input_path + t + ".sample "
        command += "-Tp " + t + " "
        command += "-T " + template_path + t + " "
        command += "-l " + template_level + " "
        print(command)
        res = call(command,shell=True)
        if (res != 0):
            print("Error Occur at: {}".format(t))
            error_num += 1
            dic[t] = "Error"
        else:
            dic[t] = "Success"
            #o_size = os.path.getsize("../LogIn/" + t + ".log")
            #a_size = os.path.getsize("../LogOut/" + t + "_" + compression_level + "_" + template_level + "_Z/lzma")
            #dic[t] = "Rate:" + str(o_size/a_size)
        e_time = time.time()
        times[t] = e_time - s_time
    tot_end = time.time()
    print(dic)
    print(times)
    print("Total time cost: {}".format(tot_end - tot_start))

