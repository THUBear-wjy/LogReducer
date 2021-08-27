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
    print("Now mode: {}, input file: {}".format(args.TemplateLevel + "_" + args.MatchPolicy + "_" + args.TimeDiff + "_" + args.EncoderMode, args.Input))
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
    thread_temp = os.path.join(now_temp, threading.current_thread().name  + "/")
    if (os.path.exists(thread_temp)):
        call("rm -rf " + thread_temp)
    os.mkdir(thread_temp)    
    for t in range(fileBeginNo, fileEndNo+1):
        order = "python3 ./restore.py -I " + os.path.join(now_input,str(t)+".7z") + " -O " + os.path.join(now_temp,str(t)+".col") + " -T " + type_template + " -t " + thread_temp
        print(order + " " + threading.current_thread().name)
        res = call(order,shell=True)
        if (res != 0):
            tempStr = "Error Occur at: {} thread: {}, fileNo: {} to {}".format(typename, threading.current_thread().name, fileBeginNo, fileEndNo)
            print (tempStr)
            writeLog(str(output_path) + "Log_{}".format(datetime.date.today()), tempStr,'WARNING')
            atomic_addErrnum(1)
            continue
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
        step = (fileListLen // maxThreadNum) + 1
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
    input_path = args.Input
    template_path = util.path_pro(args.Template)
    output_path = args.Output
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
    all_files = []
    for f in os.listdir(input_path):
        try:
            if (f.split(".")[1] == "7z"):
                all_files.append(f)
        except:
            continue

    type_template = template_path
    
    filename = output_path.split("/")[-1]
    path = util.path_pro(output_path.split(filename)[0])
    print("filename: {}, path: {}".format(filename, path))

    now_input = input_path
    now_output = path
    if (not os.path.exists(path)):
        os.mkdir(path)

    now_temp = os.path.join(path,"tmp/")
    if (not os.path.exists(now_temp)):
        pass
    else:
        call("rm -rf " + now_temp, shell=True)
    os.mkdir(now_temp)

    now_type = filename.split(".")[0]
    print(len(all_files))
    threadsToExecTasks(now_type, all_files, now_input, now_output, now_temp, type_template)
    
    #Merge
    if (mode == "Tot"):
        fw = open(output_path, 'w')
        for i in range(0, len(all_files)):
            now_path = os.path.join(now_temp, str(i)+".col")
            if not (os.path.exists(now_path)):
                print(now_path + " does not exist")
                continue
            fo = open(now_path, 'r')
            while True:
                line = fo.readline()
                if not line:
                    break
                fw.write(line)
        fw.close()
     
    time_t1 = time.time()
        
    time_t2 = time.time()
    tempStr = "{} finished, total time cost: {} , thread accum time: {}".format(now_output, time_t2 - time_t1, gl_threadTotTime)
    print(tempStr)
    writeLog(str(output_path) + "_Log_{}".format(datetime.date.today()), tempStr,'WARNING')
    gl_threadTotTime = 0 # reset
    calcuReduceRate(now_input, now_output, input_path)

    time2 = time.time() 
    tempStr = "{} Main finished, total time cost: {} , error num: {}".format(output_path, time2 - time1, gl_errorNum)
    print(tempStr)
    writeLog(str(output_path) + "_Log_{}".format(datetime.date.today()), tempStr,'WARNING')

   


