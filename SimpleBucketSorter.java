package bucket_sort;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Instantiates jobs to sort suffixes in parallel. 
 */
public class SimpleBucketSorter {
  
  public static Job runBucketSortMapReduce(Path inputPath, Path outputPath,
                                        int reducerTasks, String dna_filepath,
                                        String psort_path, String partitionPath,
                                        String genomeName)
    throws Exception {
    Configuration conf = new Configuration();
    DistributedCache.createSymlink(conf);
    DistributedCache.addCacheFile(new URI(psort_path), conf);
    DistributedCache.addCacheFile(new URI(dna_filepath), conf);
    if (partitionPath != null) {
    	conf.set("partition_file", partitionPath);
    }

    conf.set("mapred.child.java.opts", "-Xmx2000M -Xms100M");    
    conf.set("mapred.task.timeout", "12000000");    

    Job job = new Job(conf, genomeName);
    
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    
    job.setJarByClass(SimpleBucketSorter.class);

    job.setMapperClass(BucketSortMapper.class);
    job.setReducerClass(PSort.class);
    job.setNumReduceTasks(reducerTasks);
    NLineInputFormat.setNumLinesPerSplit(job, 1);
    job.setInputFormatClass(NLineInputFormat.class);

    if (partitionPath != null) {
    	job.setPartitionerClass(BucketSortTotalOrderPartitioner.class);
    }
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputKeyClass(String.class);
    job.setOutputValueClass(NullWritable.class);
    
    job.waitForCompletion(true);
    return job;
  }

  public static void main(String[] args) throws Exception {
    String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
    if (otherArgs.length != 7) {
      System.err.println("Usage: SimpleBucketSort <in> <out> <reducers> <dna_filename> <psort-path> <partition-file>");
      System.err.println("Typical psort-path: 'hdfs://localhost:9000/libraries/libpsort.so#libpsort.so'");
      System.exit(2);
    }
    
    int reducerTasks = Integer.parseInt(otherArgs[2]);
    String partitionFile = null;
    if (otherArgs[5].compareToIgnoreCase("null") != 0) {  // if passed string is "null"
    	partitionFile = otherArgs[5];
    }
    Job job = runBucketSortMapReduce(new Path(otherArgs[0]), new Path(otherArgs[1]), reducerTasks,
    		otherArgs[3], otherArgs[4], partitionFile, otherArgs[6]);
    
    System.exit(0);
  }
}
