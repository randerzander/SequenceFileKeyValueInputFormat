package com.github.randerzander;

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class SequenceFileKeyValueInputFormat<K, V> extends FileInputFormat<K, V> {
  public SequenceFileKeyValueInputFormat() {
    setMinSplitSize(SequenceFile.SYNC_INTERVAL);
  }
  
  @Override
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    FileStatus[] files = super.listStatus(job);
    for (int i = 0; i < files.length; i++) {
      FileStatus file = files[i];
      if (file.isDir()) {     // it's a MapFile
        Path dataFile = new Path(file.getPath(), MapFile.DATA_FILE_NAME);
        FileSystem fs = file.getPath().getFileSystem(job);
        // use the data file
        files[i] = fs.getFileStatus(dataFile);
      }
    }
    return files;
  }

  public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
    throws IOException {
    reporter.setStatus(split.toString());
    return (RecordReader<K, V>) new SequenceFileKeyValueRecordReader<K, V>(job, (FileSplit) split);
  }
}
