package bucket_sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

/*
 * Partitions the key space uniformly (assumes acgt as alphabets)
 */
public class BucketSortTotalOrderPartitioner<V extends Writable> extends Partitioner<IntWritable, V> implements Configurable {
  private Configuration conf;
  private char lastChar;
  private long prevIndex = 0;
  private long numRepeats = 0;
  private HashMap<Character, Integer> startIdxHashMap;
  public static DNAString dnaString;

  public class Sample {
    public class CharRun {
      CharRun(char d, long l) {
        data = d;
        len = l;
      }
      public char data;
      public long len;
    }
    List<CharRun> sample = new ArrayList<CharRun>();
    Sample(String sampleStr) {
      String[] charRuns = sampleStr.split(",");
      for (int i = 0; i < charRuns.length; ++i) {
        String[] tokens = charRuns[i].split("-");
        sample.add(new CharRun(tokens[0].charAt(0),
              Long.parseLong(tokens[1])));
      }
    }
  }

  private List<Sample> splitPoints = new ArrayList<Sample>();

  @Override
  public int getPartition(IntWritable index, V value, int numPartitions) {
    long longIndex = index.get() & 0xffffffffL;
    int partition = getHashValue(longIndex, numPartitions);
    return partition;
  }

  private int compareWithSample(Long index, Sample splitPoint) {	
    int sampleIdx = 0;
    long dnaIdx = 0, len = splitPoint.sample.get(0).len;
    long newNumRepeats = 0;
    if (index > prevIndex && (index-prevIndex) < numRepeats) {
      newNumRepeats = numRepeats - (index - prevIndex);

      // Check with big skips if the data at index is lesser/greater than
      // the given sample point. For repeat regions, the skips are high
      // enough to make them look like a 15mer.
      long sampleSize = 0;
      for (sampleIdx = 0; sampleIdx < splitPoint.sample.size(); ++sampleIdx) {
        if (lastChar < splitPoint.sample.get(sampleIdx).data) {
          return -1;
        } else if (lastChar > splitPoint.sample.get(sampleIdx).data) {
          return 1;
        }
        sampleSize += splitPoint.sample.get(sampleIdx).len;
        if (newNumRepeats <= sampleSize) {
          break;
        }
      }
      // If we have exhausted the sample point we know that sample point
      // is equal.
      if (sampleIdx == splitPoint.sample.size()) {
        return 0;
      }
      dnaIdx = newNumRepeats;
      len = (sampleSize - newNumRepeats);
      // If the first character len and newNumRepeats match, we need to
      // increment sampleIdx.
      if (len == 0) {
        ++sampleIdx;
        len = splitPoint.sample.get(sampleIdx).len;
      }
    }
    long tmpIdx = dnaIdx;
    // Normal comparison of an index with a sample point.
    while (index + dnaIdx < dnaString.length() &&
        sampleIdx < splitPoint.sample.size() &&
        (dnaString.charAt(index + dnaIdx)) ==
         splitPoint.sample.get(sampleIdx).data) { 
      ++dnaIdx;
      --len;
      if (len == 0) {
        ++sampleIdx;
        if (sampleIdx < splitPoint.sample.size()) {
          len = splitPoint.sample.get(sampleIdx).len;
        }
      }
    }
    if (len == 0 && sampleIdx == splitPoint.sample.size()) {
      return 0;
    } else if (index + dnaIdx == dnaString.length()) {
      return -1;
    } else {
      if (dnaString.charAt(index + dnaIdx) <
          splitPoint.sample.get(sampleIdx).data) {
        return -1;
      } else {
        return 1;
      }
    }
  }

  private int getHashValue(Long index, int numPartitions) {
    int partition = 0;
    int startIdx = 0;
    char indexChar = dnaString.charAt(index);
    if (startIdxHashMap.containsKey(indexChar)) {
      startIdx = startIdxHashMap.get(indexChar);
      partition = startIdx;
    } else {
      return 0;
    }
    for (int i = startIdx; i < splitPoints.size(); ++i) {//Sample splitPoint : splitPoints) {
      if (compareWithSample(index, splitPoints.get(i)) <= 0)
        break;
      ++partition;
    }
    // Set the last character and repeat length.
    if (index > prevIndex && (index-prevIndex) < numRepeats) {
      prevIndex = index;
      numRepeats--;
    } else {
      // Calculate newNumRepeats.
      lastChar = indexChar;
      prevIndex = index;
      numRepeats = 1;
      for (long i = index+1; i < dnaString.length() &&
          lastChar == dnaString.charAt(i); ++i) {
        ++numRepeats;
      }
    }
    return partition;
  }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      String partitionFile = conf.get("partition_file", "_partition_");
      System.out.println("READ FILE: " + partitionFile);
      Path path = new Path(partitionFile);
      FSDataInputStream dis = null;
      startIdxHashMap = new HashMap<Character, Integer>();
      try {
        dis = FileSystem.get(conf).open(path);
        while (dis.available() != 0) {
          String s = dis.readLine();
          if (!startIdxHashMap.containsKey(s.charAt(0))) {
            startIdxHashMap.put(s.charAt(0), splitPoints.size());
          }
          splitPoints.add(new Sample(s));
        }
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        if (dis != null) {
          try {
            dis.close();
            for (Character key : startIdxHashMap.keySet()) {
              System.out.println(key + " => " + startIdxHashMap.get(key));
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    }
  }
