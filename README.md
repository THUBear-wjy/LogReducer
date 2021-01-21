# LogReducer

Open-source code for "On the Feasibility of Parser-based Log Compression in Large-Scale Cloud Systems" (USENIX FAST 2021)

## Dependency
python >= 3.8.5

pandas >= 1.1.1

six >= 1.15

numpy >= 1.19

gcc >= 7.4.0

7z >= 16.02
## Log Sample
Samples of large scale cloud logs can be found at: 

https://github.com/THUBear-wjy/openSample

## Compile
`make`

## Execution
Assume the path of target log file is <em>/path/xx.log</em>

Step 1: Training(Generate template at <em>./template/</em>)

`python3 training.py -I /path/xx.log -T ./template/`

Step 2: Compression(Using template at <em>./template/</em> and generate result at <em>./out/</em>)

`python3 LogReducer.py -I /path/xx.log -T ./template/ -O ./out/`