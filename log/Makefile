HHOME= /opt/hadoop/1.2.1
HADOOP = ${HHOME}/bin/hadoop
HADOOPLIB = ${HHOME}
 

JFLAGS = -d target
JAVAC = javac
CLASSPATH= ${HADOOPLIB}/hadoop-core-1.2.1.jar:${HADOOPLIB}/hadoop-client-1.2.1.jar 


all: loganalyzer.jar

loganalyzer.jar: 
	mkdir -p target
	$(JAVAC) -classpath $(CLASSPATH)   $(JFLAGS) src/*.java
	jar -cvf $@  -C target/ .

submit:
	sbatch submit-log-comet.sh
compile:
	sbatch compile-hadoop-comet.sh
clean:
	rm target/*.class *.jar *.out

