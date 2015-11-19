/**
 * Following sample is adopted from original wordcount sample from 
 * http://wiki.apache.org/hadoop/WordCount. 
 */
package kee.data;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FaultyWordCount {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
   
    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      Path[] files = DistributedCache.getLocalCacheFiles(conf);
      Path[] archives = DistributedCache.getLocalCacheArchives(conf);
      System.out.println("Files=" + Arrays.toString(files));
      System.out.println(archives);
      
      System.out.println(((JobConf)conf).getWorkingDirectory());
      System.out.println("Debug script =" + conf.get("mapred.map.task.debug.script"));
  }
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
      throw new IOException("Intentional Failure"); 
      //throw new RuntimeException("Intentional Failure"); 

    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    conf.setMaxMapTaskFailuresPercent(1);
    conf.setMaxReduceTaskFailuresPercent(1);
    conf.setMaxMapAttempts(5);
    
    conf.set("mapred.max.map.failures.percent", "1");
    Job job = new Job(conf, "word count");
    job.setJarByClass(FaultyWordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


