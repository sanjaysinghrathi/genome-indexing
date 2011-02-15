package bucket_sort;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * Reads a file containing <suffix index> records and outputs 
 * prefix as key and the index value.
 */
public class BucketSortMapper
	extends Mapper<Object, Text, IntWritable, NullWritable> implements Configurable {

	private Configuration conf;

	public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
		String[] tokens = value.toString().split(","); 
		Long startIndex = Long.parseLong(tokens[0]);
		Long endIndex = Long.parseLong(tokens[1]);
		for (long i = startIndex; i <= endIndex; ++i) {
			context.write(new IntWritable((int)i), NullWritable.get());
		}
	}

	@Override
	public void cleanup(Context context)
		throws IOException, InterruptedException {
		BucketSortTotalOrderPartitioner.dnaString = null;

	}
	
	@Override
	public Configuration getConf() {
		return conf;
	}

  private void cleanupTempFiles() {
    File tmpDir = new File("/tmp/");
    String[] fileList = tmpDir.list(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return (name.endsWith("_filename"));
      }
    });
    for (String filename : fileList) {
      File tmpFile = new File("/tmp/", filename);
      System.out.println(filename);
      tmpFile.delete();
    }
  }

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
    cleanupTempFiles();
		String dnaFileName = "dna_filename";
    File file = new File(dnaFileName);
    BufferedInputStream stream = null;
		try {
      stream = new BufferedInputStream(new FileInputStream(file));
      DNAString dnaString = new DNAString(stream, file.length());
			BucketSortTotalOrderPartitioner.dnaString = dnaString;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
