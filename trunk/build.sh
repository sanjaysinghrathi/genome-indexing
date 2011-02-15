USERNAME=`whoami`
HADOOP_HOME=/usr/lib/hadoop-0.20
HADOOP_LIB=$HADOOP_HOME/lib/
g++ -fPIC -shared -I/usr/lib/jvm/java-6-openjdk/include/ psort.cpp -o libpsort.so -O3
g++ -O3 sampler.cpp -o sampler
javac -classpath $HADOOP_HOME/hadoop-core.jar:$HADOOP_HOME/hadoop-tools.jar:$HADOOP_LIB/commons-cli-1.2.jar:$HADOOP_LIB/commons-logging-api-1.0.4.jar -d classes/ *.java
jar -cvf SimpleBucketSorter.jar -C classes/ .
hadoop fs -rm /user/$USERNAME/psort/libraries/libpsort.so
hadoop fs -copyFromLocal libpsort.so /user/$USERNAME/psort/libraries/libpsort.so
