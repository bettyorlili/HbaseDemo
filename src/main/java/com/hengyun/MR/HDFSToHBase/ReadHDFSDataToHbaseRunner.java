package com.hengyun.MR.HDFSToHBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReadHDFSDataToHbaseRunner extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        
        int run = ToolRunner.run(new ReadHDFSDataToHbaseRunner(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] arg0) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.defaultFS", "hdfs://myha01/");
        conf.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop2:2181,hadoop3:2181");
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        FileSystem fs = FileSystem.get(conf);
//        conf.addResource("config/core-site.xml");
//        conf.addResource("config/hdfs-site.xml");
        
        Job job = Job.getInstance(conf);
        
        job.setJarByClass(ReadHDFSDataToHbaseRunner.class);
        
        job.setMapperClass(HDFSToHbaseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        TableMapReduceUtil.initTableReducerJob("student", HDFSToHbaseReducer.class, job,null,null,null,null,false);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);
        
        Path inputPath = new Path("/student/input/");
        Path outputPath = new Path("/student/output/");
        
        if(fs.exists(outputPath)) {
            fs.delete(outputPath,true);
        }
        
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        boolean isDone = job.waitForCompletion(true);
        
        return isDone ? 0 : 1;
    }
    
}