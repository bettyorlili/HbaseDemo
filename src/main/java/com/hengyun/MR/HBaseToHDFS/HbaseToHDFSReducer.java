package com.hengyun.MR.HBaseToHDFS;
/*
 * MapReduce从HBase读取数据计算平均年龄并存储到HDFS中
 * 作者：卞燕如 2018年10月31日
 * */
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class HbaseToHDFSReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>{
    
    DoubleWritable outValue = new DoubleWritable();
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,Context context)
            throws IOException, InterruptedException {
        
        int count = 0;
        int sum = 0;
        for(IntWritable value : values) {
            count++;
            sum += value.get();
        }
        
        double avgAge = sum * 1.0 / count;
        outValue.set(avgAge);
        context.write(key, outValue);
        System.out.println("reduce result->"+key.toString()+":"+outValue.toString());
    }
    
}
