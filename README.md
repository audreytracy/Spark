# Spark Chromosome Count

## How to run  
*  Get Spark jars with `wget https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz`
`tar xvfz spark-3.1.2-bin-hadoop3.2.tgz`
*  Compile program with  `javac -classpath /home/username/spark-3.1.2-bin-hadoop3.2/jars/spark-core_2.12-3.1.2.jar:/home/username/spark-3.1.2-bin-hadoop3.2/jars/spark-sql_2.12-3.1.2.jar:/home/username/spark-3.1.2-bin-hadoop3.2/jars/scala-library-2.12.10.jar BinCount.java`
*  Create jar file with `jar -cvf bincount.jar BinCount*.class`  
*  Copy input file to Spark with `hadoop fs -copyFromLocal filename /user/username/filename`  
*  Execute jar file with `spark-submit --class BinCount --master local ./bincount.jar filename`

List output files with `hadoop fs -ls /user/username/output/`

To get single local output use `hadoop fs -getmerge /user/username/output merged`. View with `cat merged`


## How to run 

https://github.com/user-attachments/assets/711aebf6-b998-4b03-9061-dad7732130a4


