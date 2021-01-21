import sys
import random
if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    sample_rate = float(sys.argv[3])
    fo = open(input_path, "r",encoding = "ISO-8859-1")
    line = fo.readline()
    tot_count = 1
    while line:
        line = fo.readline()
        tot_count += 1
    fo.close()
    
    trucate_size = 20
    N = range(tot_count // trucate_size)
    m = min(max(int(len(N)*sample_rate),50),50000)
    sample_count = m * trucate_size
    print("Sample for {}, tot size: {}, sample size: {}".format(input_path, tot_count, sample_count))

    sample = sorted(random.sample(N,m))
    
    fo = open(input_path, "r", encoding = "ISO-8859-1")
    fw = open(output_path, "w",encoding = "ISO-8859-1")
    line = fo.readline()
    now_start = 0
    writeable = False
    count = 0
    s_count = 0
    while line:
        if (now_start < len(sample) and count // trucate_size == sample[now_start]):
            writeable = True
            s_count = 0
            now_start += 1
        if (writeable):
            fw.write(line)
            s_count += 1
            if (s_count >= trucate_size):
                writeable = False
        count += 1
        line = fo.readline()
    fw.close()
                
