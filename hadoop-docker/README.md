docker network create hadoop || true

yarn jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar pi 10 15

hdfs dfs -mkdir /data
hdfs dfs -put data.txt /data

wget https://zenodo.org/records/3360392/files/D1.7GB.zip
unzip D1.7GB.zip
cat D1.7GB/*.txt > big-data.txt
hdfs dfs -put big-data.txt /data


# SIMPLE
```sh
$ cd simple
$ yarn jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar -files mapper.py,reducer.py -mapper mapper.py -reducer reducer.py -input /data/data.txt -output /output_simple
$ hdfs dfs -cat /output_simple/part-00000
```

# ADVANCE
```sh
$ cd advance
$ HADOOP_STREAMING=/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar
$ yarn jar $HADOOP_STREAMING -files mapper.py,reducer.py -mapper mapper.py -reducer reducer.py -input /data/data.txt -output /output_advance
$ yarn jar $HADOOP_STREAMING -files mapper.py,reducer.py -mapper mapper.py -reducer reducer.py -numReduceTasks 2 -input /data/data.txt -output /output_advance_2
$ yarn jar $HADOOP_STREAMING -files mapper.py,reducer.py -mapper mapper.py -reducer reducer.py -input /data/big-data.txt -output /output_advance_3
$ hdfs dfs -cat /output_advance/part-00000
```

# MRJOB
```sh
$ curl https://bootstrap.pypa.io/pip/2.7/get-pip.py -o get-pip.py
$ sudo python get-pip.py
$ pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org pip setuptools
$ pip install mrjob
$ python MRWordCount.py -r hadoop hdfs:///data/data.txt -c mrjob.conf --output-dir hdfs:///output_mrjob/
$ hdfs dfs -cat /output_mrjob/part-00000
$ python MRMostUsedWord.py -r hadoop hdfs:///data/big-data.txt -c mrjob.conf --output-dir hdfs:///output_mrjob_2/
$ hdfs dfs -cat /output_mrjob_2/part-00000
```


wget https://filedn.eu/l8b10yrRGnQjVGoMcMYVGTY/tweets_348MB.json

docker cp hadoop_mapper.py hadoop-docker-resourcemanager-1:/opt/hadoop/wordcount
docker cp hadoop_reducer.py hadoop-docker-resourcemanager-1:/opt/hadoop/wordcount
wget https://raw.githubusercontent.com/nivdul/spark-in-practice-scala/master/data/wordcount.txt

hdfs dfs -mkdir /wordcountdata
hdfs dfs -put wordcount.txt /wordcountdata

cat wordcount.txt | ./hadoop_mapper.py | sort -t 1 | ./hadoop_reducer.py

yarn jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar -files hadoop_mapper.py,hadoop_reducer.py -mapper hadoop_mapper.py -reducer hadoop_reducer.py -input /wordcountdata/wordcount.txt -output output.txt

pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org mrjob==0.5.6
pip install --upgrade pip --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org

Job Counters 
    Launched map tasks=2
    Launched reduce tasks=1
    Rack-local map tasks=2
    Total time spent by all maps in occupied slots (ms)=6147
    Total time spent by all reduces in occupied slots (ms)=2242
    Total time spent by all map tasks (ms)=6147
    Total time spent by all reduce tasks (ms)=2242
    ...
Map-Reduce Framework
    Map input records=44
    Map output records=810
    Map output bytes=6542
    Map output materialized bytes=8174
    Input split bytes=162
    Combine input records=0
    Combine output records=0
    Reduce input groups=381
    Reduce shuffle bytes=8174
    Reduce input records=810
    Reduce output records=381
    ...