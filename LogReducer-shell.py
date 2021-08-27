import sys
import os
from subprocess import call
TYPE = ["Apsara", "Fastcgi", "K8S", "Metering", "Monitor", "Nginx", "Ols", "Op", "Oss", "Pangu", "PAudit", "Presto", "PSummary", "Request", "Rpc", "Server", "Shennong", "Sinput", "Sla", "Slsf", "Slss", "Sys", "Tail", "Tubo"]
input_path = sys.argv[1]
template_path = sys.argv[2]
output_path = sys.argv[3]
for i in TYPE:
    inputFile = os.path.join(input_path, i+"/0.log")
    templateFile = os.path.join(template_path, i)
    command = "python3 training.py -I " + inputFile + " -T " + templateFile
    print(command)
    call(command,shell=True)
    outputFile = os.path.join(output_path, i + ".zip")
    command_t = "python3 LogReducer.py -I " + inputFile + " -T " + templateFile + " -O " + outputFile
    print(command_t)
    call(command_t,shell=True)
