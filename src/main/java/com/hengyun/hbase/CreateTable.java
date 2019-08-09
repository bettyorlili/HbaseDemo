package com.hengyun.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class CreateTable {
	public static void main(String[] args) throws IOException
	{
			   // Instantiating configuration class 初始化配置文件
			   Configuration conf = HBaseConfiguration.create();

			   Connection conn = ConnectionFactory.createConnection(conf);// 创建连接
			   // Instantiating HbaseAdmin class  初始化HbaseAdmin 
			   HBaseAdmin admin = (HBaseAdmin) conn.getAdmin(); // hbase 表管理类
			   
			   // Instantiating table descriptor class  设置表名
			   HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("member"));

			   // Adding column families to table descriptor  设置列族名（可设置多个）
			   tableDescriptor.addFamily(new HColumnDescriptor("personal"));
			   tableDescriptor.addFamily(new HColumnDescriptor("professional"));

			   // Execute the table through admin
			   admin.createTable(tableDescriptor);
			   System.out.println(" Table created ");
	}
}
