package bucket_sort;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reads prefix, index records and sorts them using a SortedSet.  
 */
public class PSort
extends Reducer<IntWritable, NullWritable, String, NullWritable> implements Configurable {

  private Configuration conf;
  private String dna_filename;
  private int partition;
  private int numIndices = 0;
  BufferedWriter ofs;
  String suffixFilename;
  String jobName;

  enum ReducerTime {
    PSORT_TIME
  };

  public native void do_sort(String dna_file, String suffix_file, String output_file, int numIndices, int partition);
  /* Use static intializer */
  static {
    System.loadLibrary("psort");
  }

  public void reduce(IntWritable key, Iterable<NullWritable> values, Context context)
    throws IOException, InterruptedException {
      long longIndex = key.get() & 0xffffffffL;
      ofs.write(Long.toString(longIndex) + "\n");
      ++numIndices;
    }

  @Override
    public void setup(Context context)
    throws IOException, InterruptedException {
      jobName = new String(context.getJobID().toString());
      String partitionStr = conf.get("mapred.task.partition");
      partition = Integer.parseInt(partitionStr);
      suffixFilename = new String("/tmp/" + partition + "_" + jobName + "_suffix_filename"); 
      try {
        ofs = new BufferedWriter(new FileWriter(suffixFilename));
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }

    }

  @Override
    public void cleanup(Context context)
    throws IOException, InterruptedException {
      ofs.close();
      if (numIndices == 0) return;
      String outputFilename = "/tmp/" + partition + "_" + jobName + "_suffix_array_filename"; 
      long start = System.currentTimeMillis();
      this.do_sort(this.dna_filename, suffixFilename, outputFilename, numIndices, partition);
      long elapsedTime = System.currentTimeMillis()-start;
      context.getCounter(ReducerTime.PSORT_TIME).increment(elapsedTime);
      String outputDir = conf.get("mapred.output.dir");
      String hdfsOutputFilename = String.format("%s/suffix_array-%05d", outputDir, partition);
      FileSystem.get(conf).copyFromLocalFile(false, true,
          new Path(outputFilename),
          new Path(hdfsOutputFilename));
      File suffixFile = new File(suffixFilename);
      suffixFile.delete();
    }

  @Override
    public Configuration getConf() {
      return conf;
    }

  @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      dna_filename = "dna_filename";
    }
}
