yarn jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar pi 10 15

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
