add jar ../target/SequenceFileKeyValueInputFormat-0.1.0-SNAPSHOT.jar;

drop table if exists seqtest;
create external table seqtest(filename string, text string)
stored as inputformat 'com.github.randerzander.SequenceFileKeyValueInputFormat'
outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/user/dev/seqtest/';

!echo select * from seqtest;
select * from seqtest;

!echo select filename from seqtest;
select filename from seqtest;

!echo select text from seqtest;
select text from seqtest;

!echo select filename, text from seqtest;
select filename, text from seqtest;
