package com.hengyun.MR.HBaseToHDFS;

/*
 * MapReduce从HBase读取数据计算平均年龄并存储到HDFS中
 * 作者：卞燕如 2018年10月31日
 * */
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
public class HbaseToHDFSMapper extends TableMapper<Text, IntWritable>{
    
    Text outKey = new Text("math");
    IntWritable outValue = new IntWritable();
    // key是hbase中的行键
    // value是hbase中的行键的所有数据
    @Override
    protected void map(ImmutableBytesWritable key, Result value,Context context)
            throws IOException, InterruptedException {
        
        boolean isContainsColumn = value.containsColumn("course".getBytes(), "math".getBytes());
    
        if(isContainsColumn) {
            
            List<Cell> listCells = value.getColumnCells("course".getBytes(), "math".getBytes());
            System.out.println("listCells:\t"+listCells);
            Cell cell = listCells.get(0);
            System.out.println("cells:\t"+cell);
            
            byte[] cloneValue = CellUtil.cloneValue(cell);
            String ageValue = Bytes.toString(cloneValue);
            outValue.set(Integer.parseInt(ageValue));
            
            context.write(outKey,outValue);
            System.out.println("map result->"+outKey.toString()+":"+outValue.toString());
        }
    }
}
