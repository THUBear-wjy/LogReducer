import os
from subprocess import call
class timeCount:
    def __init__(self, name):
        self.name = name
        self.countTime = time.time() - time.time()
        self.start_time = 0
        self.call_time = 0
   
    def output(self):
        if (self.call_time == 0):
            print(self.name + "No call")
            return

        countTime = float(self.countTime * 1000000)
        
        print(self.name + 'Spend time count {:.2f}us, call time {:.0f}, average time count {:.2f}us'.format(countTime, self.call_time, (float(countTime) / float(self.call_time))))
            
    def countBegin(self):
        self.start_time = time.time()

    def countEnd(self):
        self.call_time += 1
        self.countTime += time.time() - self.start_time
        

def list_write(file_path, li, append=False):
    print("Outputing " + file_path)
    if (append):
        fw = open(file_path, 'a')
    else:
        fw = open(file_path, "w")
    for i in li:
        fw.write(str(i).strip() + "\n")
    fw.close()
    
def make_up(num, length):
    s = str(num)
    while (len(s) < length):
        s = "0" + s
    return s
def path_pro(path):
    if(path[-1] != '/'):
        path += '/'
    return path

def load_array(path, is_int=True):
    fo = open(path, "r")
    array = []
    for line in fo.readlines():
        if (is_int):
            array.append(int(line.strip()))
        else:
            array.append(line.strip())
    return array
def load_log(path):
    fo = open(path, "rb")
    array = []
    s = b""
    while True:
        char = fo.read(1)
        s = s + char
        if char == b'\n':
            array.append(s)
            s = b""
        if char == b"":
            break
    fo.close()
    return array
def decoder(path, is_encoder=True):
    if not is_encoder:
        return path
    new_path = path + ".txt"
    res = call("./Elastic d " + path + " " + new_path + " O", shell=True)
    if (res != 0):
        print("Error")
    return new_path

def get_file_name(path):
    return path.split("/")[-1].split(".")[0]
