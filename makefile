cc = g++
EXEC = THULR
SRCS = main.cpp template.cpp LengthSearch.cpp
OBJS = $(SRCS:.cpp=.o)

start:$(OBJS)
	$(cc) -o $(EXEC) $(OBJS)
	$(cc) -std=c++11 iddiff.cpp -o Iddiff
	$(cc) -std=c++11 numdiff.cpp -o Numdiff
	$(cc) -std=c++11 entropy.cpp -o Entropy
	$(cc) -std=c++11 elastic.cpp -o Elastic

.cpp.o:
	$(cc) -std=c++11 -o $@ -c $<
clean:
	rm -rf $(OBJS) $(EXEC)
	rm -rf Iddiff
	rm -rf Numdiff
	rm -rf Entropy  
	rm -rf Elastic 
