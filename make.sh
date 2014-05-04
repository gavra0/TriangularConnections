# Compile the classes to the .class files
javac -source 1.6 -target 1.6 -classpath /Users/ivan/Dev/hadoop/hadoop-core-1.2.1.jar -d ./build src/ch/epfl/data/bigdata/hw2/Run.java

#Locate to build
cd build

# Put all .class files to jar
jar cvf hw2.jar ./ch/epfl/data/bigdata/hw2/*.class

# Remove output directory
hadoop fs -rmr output

# ch.epfl.data.bigdata.hw2.Run hadoop job
hadoop jar hw2.jar ch.epfl.data.bigdata.hw2.Run dataset/users.txt output