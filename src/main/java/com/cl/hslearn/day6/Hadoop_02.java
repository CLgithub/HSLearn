package com.cl.hslearn.day6;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by L on 18/2/13.
 */
public class Hadoop_02 {
    public static void main(String[] args) throws Exception {
        hdfsClientDemo1();
    }


    public static void hdfsClientDemo1() throws Exception{
        Configuration conf= new Configuration();
//        conf.set("fs.defaultFS","hdfs://us1:9000");       //设置后会自动变成hdfs文件系统，否则是本地文件系统
//        FileSystem fs=FileSystem.get(conf);

        conf.set("dfs.replication","5");    //配置文件加载顺序，会先加载jar包下的，然后加载classpath下的，最后加载代码里的，所以最终是5
        FileSystem fs= FileSystem.get(new URI("hdfs://us1:9000"), conf, "hadoop");

        //增
        fs.copyFromLocalFile(new Path("/Users/L/Downloads/t1"),new Path("/Users/L/Downloads/t2"));

        //下载
//        fs.copyToLocalFile(new Path("/Users/L/Downloads/t2"),new Path("/Users/L/Downloads/t2_down"));


        //删除
//        fs.delete(new Path("/Users/L/Downloads/t2"),true);

        fs.close();
    }
}
