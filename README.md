An InputFormat for Apache Hive that will return both (Text) keys and (Text) values when querying against SequenceFiles.

I needed to parse a lot of text files in their entirety without splitting up the contents. HDFS gives the best throughput when dealing with large contiguous files, so I used Apache Mahout to turn directories full of text files into SequenceFiles with their filenames as keys and the contents as values. When using the default SequenceFileInputFormat with Hive, Hive only provides values, not the keys.

I needed access to the filenames, so I wrote SequenceFileKeyValueInputFormat.

**Warning**: Hive uses the Ctrl+A character ("\001") to recognize separate columns. This impl of RecordReader will replace all "\001"s with "\000"s. If your SequenceFile contains Ctrl+A chars in its keys or values, this may not work for you.

Example:
```
cd example
hadoop fs -put data .
mahout seqdirectory --input data --output seqtest
hive -f test.hql
```
