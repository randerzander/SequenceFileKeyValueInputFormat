add jar ../target/SequenceFileKeyValueInputFormat.jar;

drop table if exists test;
create external table test(filename string, text string)
stored as inputformat 'SequenceFileKeyValueInputFormat'
outputformat 'org.apache.hadoop.mapred.SequenceFileOutputFormat'
location '/user/dev/seqtest/';

select * from test;
