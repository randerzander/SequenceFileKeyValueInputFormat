package org.randerzander;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileKeyValueRecordReader<K, V> implements RecordReader<K, V> {
  private SequenceFile.Reader in;
  private long start;
  private long end;
  private boolean more = true;
  protected Configuration conf;

  public SequenceFileKeyValueRecordReader(Configuration conf, FileSplit split) throws IOException {
    Path path = split.getPath();
    FileSystem fs = path.getFileSystem(conf);
    this.in = new SequenceFile.Reader(fs, path, conf);
    this.end = split.getStart() + split.getLength();
    this.conf = conf;

    if (split.getStart() > in.getPosition()) in.sync(split.getStart());
    this.start = in.getPosition();
    more = start < end;
  }

  @Override
  public K createKey() { return (K) ReflectionUtils.newInstance(in.getKeyClass(), conf); }
  @Override
  public V createValue(){ return (V) ReflectionUtils.newInstance(in.getValueClass(), conf); }
  @Override
  public float getProgress() throws IOException {
    if (end == start) return 0.0f;
    else return Math.min(1.0f, (in.getPosition() - start) / (float)(end - start));
  }

  public synchronized long getPos() throws IOException { return in.getPosition(); }
  protected synchronized void seek(long pos) throws IOException { in.seek(pos); }
  public synchronized void close() throws IOException { in.close(); }

  @Override
  public boolean next(K key, V value) throws IOException {
    if (!more) return false;
    long pos = in.getPosition();
    boolean remaining = in.next((Writable) key, (Writable) value);
    
    if (remaining){
      //Hive ignores keys and gives access only to their value
      //Prepend the key onto the value to trick Hive into giving access to the key
      //Hive's internal column separator is Ctrl-A "\001"
      //Separating the key and value with "\001" makes Hive interpret the modified value as two columns: key & value
      ((Text)value).set(key.toString() + "\001" + value.toString()); 
    }
    if (pos >= end && in.syncSeen()) more = false;
    else more = remaining;
    return more;
  }
}
