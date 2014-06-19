add jar ../target/SequenceFileKeyValueInputFormat-0.1.0-SNAPSHOT.jar;

drop table if exists seqtest;
create external table seqtest(filename string, text string)
stored as inputformat 'org.randerzander.SequenceFileKeyValueInputFormat'
outputformat 'org.apache.hadoop.mapred.SequenceFileOutputFormat'
location '/user/dev/seqtest/';

set hive.execution.engine=${hiveconf:ENGINE};
set hive.execution.engine;

!echo select * from seqtest;
select * from seqtest;

!echo select filename from seqtest;
select filename from seqtest;

!echo select text from seqtest;
select text from seqtest;

!echo select filename, text from seqtest;
select filename, text from seqtest;
