import os
from subprocess import call
import sys
Name = ["Apsara", "Fastcgi", "Metering","Monitor","Ols","Op","Oss", "Pangu", "PAudit", "Presto", "PSummary", "Request", "Rpc", "Shennong", "Sinput", "Sla", "Sys", "Tubo"]
def makeDir():
    for n in Name:
        os.mkdir(os.path.join("./out/", n))

def training(path):
    for n in Name:
        input_path = os.path.join(path, n+".log")
        template_path = os.path.join("./template/", n)
        command = "python3 training.py " + "-I " + input_path + " -T " + template_path
        print(command)
        call(command, shell=True)

def compression(path):
    for n in Name:
        input_path = os.path.join(path, n+".log")
        template_path = os.path.join("./template/", n)
        output_path = os.path.join("./out/", n)
        command = "python3 LogReducer.py " + "-I " + input_path + " -T " + template_path + " -O " + output_path
        print(command)
        call(command, shell=True)

if __name__ == "__main__":
    input_path = sys.argv[1]
    makeDir()
    training(input_path)
    compression(input_path)
    
