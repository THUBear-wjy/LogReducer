import sys
import os
import argparse
import util
import time
import datetime
import threading
import logging
from subprocess import call
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED  
from os.path import join, getsize

#var define
#Types = ["Android"]
Types = ["LogA", "LogB", "LogC", "LogD", "LogE", "LogF", "LogG", "LogH", "LogI", "LogJ", "LogK", "LogL", "LogM", "LogN", "LogO", "LogP", "LogQ", "LogR"]
#Types = ["Android","Apache","Bgl","Hadoop","Hdfs","Healthapp","Hpc","Linux","Mac","Openstack","Proxifier","Spark","Ssh","Thunderbird","Windows","Zookeeper"]
lock = threading.RLock()
gl_threadTotTime = 0
gl_errorNum = 0

#func define
def add_argument(parse):
    parse.add_argument("--Input", "-I", help="The input directory include log files(such as LogA.log, LogB.log, etc...)")
    parse.add_argument("--Output", "-O", help="The output directory inclues output zip(each input file corresponds to a directory(such as LogA/0.7z,1.7z,.. LogB/0.7z,1.7z,..)")
    parse.add_argument("--Template", "-T", default="/apsarapangu/disk7/LogTemplate/", help="The template directory inclues needed templates(each input file corresponds to a directory(such as LogA/ LogB/ ..)")
    
    parse.add_argument("--TemplateLevel", "-TL", default="0", choices=["0", "N"], help="The template level. 0 for nothing, N for number correlation.")
    parse.add_argument("--MatchPolicy", "-P", default="L", choices=["L", "T"], help="The match policy after parsing")
    parse.add_argument("--TimeDiff", "-D", default="D", choices=["D", "ND"], help="Open time diff policy")
    parse.add_argument("--EncoderMode", "-E", default="Z", choices=["Z", "NE", "P"], help="Encoder type ")
    
    parse.add_argument("--MaxThreadNum", "-TN", default="4", help="The max thread running num")
    parse.add_argument("--ProcFilesNum", "-FN", default="0", help="The max block num a single thread can process, 0 for dynamic distrib.")
    parse.add_argument("--BlockSize", "-B", default="100000", help="The size of lines in single block(100000 for each block)")
    parse.add_argument("--Mode", "-m", default="Tot", choices=["Tot", "Seg"], help="The mode of compression(Tot for single large file, Seg for multiple blocks default for Tot)")
    
def check_args(args):
    print("Now testing mode: {}, Now testing type: {}".format(args.TemplateLevel + "_" + args.MatchPolicy + "_" + args.TimeDiff + "_" + args.EncoderMode, Types))
    if (not os.path.exists(args.Input)):
        print("No input path. Quit")
        return 0

    if (not os.path.exists(args.Template)):
        print("No template path. Quit")
        return 0

    if (not os.path.exists(args.Output)):
        print("No output path. Will make new directory at {}".format(args.Output))
    else:
        call("rm -rf " + args.Output,shell=True)
    os.mkdir(args.Output)
    return 1
    
def atomic_addTime(step):
    lock.acquire()
    global gl_threadTotTime
    gl_threadTotTime += step
    lock.release()

def atomic_addErrnum(step):
    lock.acquire()
    global gl_errorNum
    gl_errorNum += step
    lock.release()

def writeLog(fname, message, levelStr):
    logging.basicConfig(filename=fname,
                           filemode='a',
                           format = '%(asctime)s - %(message)s')
    logger = logging.getLogger(__name__)
    if (levelStr =='WARNING'):
        logger.warning(message)
    elif (levelStr =='INFO'):
        logger.info(message)   

#return exec time (t2-t1)
def procFiles(typename, fileBeginNo, fileEndNo, now_input, now_output, now_temp, type_template):
    t1 = time.time()
    #parser
    now_temp += threading.current_thread().name  + "/"
    if (not os.path.exists(now_temp)):
        os.mkdir(now_temp)
    order = "./THULR -I " + now_input + " -X " + str(fileBeginNo) + " -Y " + str(fileEndNo) + " -O " + now_temp + " -T " + type_template + " -E " + encoder_mode + " -D " + time_diff + " -F " + type_template + "head.format"
    print(order + " " + threading.current_thread().name)
    res = call(order,shell=True)
    if (res != 0):
        tempStr = "Error Occur at: {} thread: {}, fileNo: {} to {}".format(typename, threading.current_thread().name, fileBeginNo, fileEndNo)
        print (tempStr)
        writeLog(str(output_path) + "Log_{}".format(datetime.date.today()), tempStr,'WARNING')
        atomic_addErrnum(1)
    #compress
    for i in range(fileBeginNo, fileEndNo + 1):
        filename = "{}".format(i)
        zip_path = str(now_output) + str(filename) + ".7z"
        compression_order = "7za a " + zip_path + " " + now_temp + filename + "/*" + " -m0=LZMA"
        call(compression_order, shell=True)
        call("rm -rf " + now_temp + filename + "/*", shell=True)
    t2 = time.time()
    tempStr = "thread:{}, type:{}, fileNo: {} to {} , cost time: {}".format(threading.current_thread().name, typename, fileBeginNo, fileEndNo, t2 - t1)
    print (tempStr)
    writeLog(str(output_path) + "Log_{}".format(datetime.date.today()), tempStr,'WARNING')

    return t2 - t1

def procFiles_result(future):
    atomic_addTime(future.result())

# calculate the reduce rate of each type file
def getdirsize(dir):
    size = 0
    for root, dirs, files in os.walk(dir):
        size += sum([getsize(join(root, name)) for name in files])
    return size

# calculate the reduce rate of each type file
def calcuReduceRate(inputPath, outputPath, typename):
    inFileSize = getdirsize(inputPath)
    outFileSize = getdirsize(outputPath)
    rate = inFileSize / outFileSize
    inFileSize = inFileSize / 1024
    outFileSize = outFileSize / 1024
    tempStr = "Type:{}, In_OutSize: {} _ {} Kb, Rate: {} , InFilePath:{}, OutFilePath:{}".format(typename, float('%.3f' % inFileSize), float('%.3f' % outFileSize), float('%.3f' % rate), inputPath, outputPath)
    print (tempStr)
    writeLog(str(output_path) + "Log_{}".format(datetime.date.today()), tempStr,'WARNING')

def threadsToExecTasks(typename, files, now_input, now_output, now_temp, type_template):
    fileListLen = len(files)
    curFileNumBegin = 0
    curFileNumEnd = 0
    step = maxSingleThreadProcFilesNum
    if (step == 0):# dynamic step
        step = fileListLen // maxThreadNum
        if(step == 0):
            step = 1 # make sure the step is bigger than 0
    
    threadPool = ThreadPoolExecutor(max_workers = maxThreadNum, thread_name_prefix="LR_")
    while curFileNumBegin < fileListLen:
        if (curFileNumBegin + step > fileListLen):
            curFileNumEnd = fileListLen - 1
        else:
            curFileNumEnd = curFileNumBegin + step - 1

        future = threadPool.submit(procFiles, typename, curFileNumBegin, curFileNumEnd, now_input, now_output, now_temp, type_template)
        future.add_done_callback(procFiles_result)
        curFileNumBegin = curFileNumEnd + 1
    #wait(future, return_when=ALL_COMPLETED)
    threadPool.shutdown(wait=True)


if __name__ == "__main__":
    parse = argparse.ArgumentParser()
    add_argument(parse)
    args = parse.parse_args()
    if (not check_args(args)):
       exit(1)

    #init params
    input_path = util.path_pro(args.Input)
    template_path = util.path_pro(args.Template)
    output_path = util.path_pro(args.Output)
    template_level = args.TemplateLevel
    time_diff = args.TimeDiff
    encoder_mode = args.EncoderMode
    match_policy = args.MatchPolicy
    mode = args.Mode
    maxThreadNum = int(args.MaxThreadNum)
    maxSingleThreadProcFilesNum = int(args.ProcFilesNum)
    blockSize = int(args.BlockSize)
    #threadPool = ThreadPoolExecutor(max_workers = maxThreadNum, thread_name_prefix="test_")
    time1 = time.time()
    if (mode == "Tot"):
        seg_path = input_path + "Segment/"
        if not os.path.exists(seg_path):
           os.mkdir(seg_path)
    else:
        seg_path = input_path

    for t in Types:
        if (mode == "Tot"):
            if os.path.exists(seg_path + t):
                call("rm -rf "+seg_path+t,shell=True)
            os.mkdir(seg_path + t)
            f = open(input_path + t + ".log",encoding = "ISO-8859-1")
            cou = 0
            count = 0
            buffer = []
            while True:
                line = f.readline()
                if not line:
                    util.list_write(seg_path + t + "/" + str(cou) + ".col", buffer, True)
                    break
                
                buffer.append(line)
                count += 1
                
                if count == blockSize:
                    count = 0
                    util.list_write(seg_path + t + "/"  + str(cou) + ".col", buffer, True)
                    buffer = []
                    cou += 1
                
        time_t1 = time.time()
        all_files = os.listdir(seg_path + t)
        type_template = template_path + t + "/"
        temp_path = output_path + "tmp/"
        if (not os.path.exists(temp_path)):
            os.mkdir(temp_path)

        now_temp = temp_path + t + "/"
        if (not os.path.exists(now_temp)):
            pass
        else:
            call("rm -rf " + now_temp, shell=True)
        os.mkdir(now_temp)
        
        now_input = seg_path + t + "/"
        now_output = output_path + t + "/"
        if (not os.path.exists(now_output)):
            os.mkdir(now_output)
        
        ###ThreadPool to Proc Files
        threadsToExecTasks(t, all_files, now_input, now_output, now_temp, type_template)
        
        time_t2 = time.time()
        tempStr = "{} finished, total time cost: {} , thread accum time: {}".format(now_output, time_t2 - time_t1, gl_threadTotTime)
        print(tempStr)
        writeLog(str(output_path) + "Log_{}".format(datetime.date.today()), tempStr,'WARNING')
        gl_threadTotTime = 0 # reset
        calcuReduceRate(now_input, now_output, t)

    time2 = time.time() 
    tempStr = "{} Main finished, total time cost: {} , error num: {}".format(output_path, time2 - time1, gl_errorNum)
    print(tempStr)
    writeLog(str(output_path) + "Log_{}".format(datetime.date.today()), tempStr,'WARNING')

   


