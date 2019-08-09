package com.hengyun.MR.HBaseToHDFS;
/*
 * MapReduce从HBase读取数据计算平均年龄并存储到HDFS中
 * 作者：卞燕如 2018年10月31日
 * */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReadHbaseDataToHDFSRunner  extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        
        int run = ToolRunner.run(new ReadHbaseDataToHDFSRunner(), args);
        System.exit(run);
        
    }

    @Override
    public int run(String[] arg0) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.defaultFS", "hdfs://192.168.241.129/");
        conf.set("hbase.zookeeper.quorum", "192.168.241.129");
        System.setProperty("HADOOP_USER_NAME", "byr");
        FileSystem fs = FileSystem.get(conf);
//        conf.addResource("config/core-site.xml");
//        conf.addResource("config/hdfs-site.xml");
        
        Job job = Job.getInstance(conf);
        
        job.setJarByClass(ReadHbaseDataToHDFSRunner.class);
        
        
        // 取对业务有用的数据 course,math
        Scan scan = new Scan();
        scan.addColumn("course".getBytes(), "math".getBytes());
        
        TableMapReduceUtil.initTableMapperJob(
                "scores".getBytes(), // 指定表名
                scan, // 指定扫描数据的条件
                HbaseToHDFSMapper.class, // 指定mapper class
                Text.class,     // outputKeyClass mapper阶段的输出的key的类型
                IntWritable.class, // outputValueClass mapper阶段的输出的value的类型
                job, // job对象
                false
                );
    

        job.setReducerClass(HbaseToHDFSReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        Path outputPath = new Path("/user/byr/hbase/avgmath/");
        
        if(fs.exists(outputPath)) {
            fs.delete(outputPath,true);
        }
        
        FileOutputFormat.setOutputPath(job, outputPath);
        
        boolean isDone = job.waitForCompletion(true);
        
        return isDone ? 0 : 1;
    }
}
