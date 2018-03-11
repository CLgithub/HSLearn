package com.cl.hslearn.day6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 对hadoop_0的结果进行排序
 13480253104	180	180	360
 13502468823	7335	110349	117684
 13560436666	1116	954	2070
 13560439658	2034	5892	7926
 */
public class Hadoop_05_sort {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        long l1=System.currentTimeMillis();

        Configuration conf=new Configuration();
        conf.set("mapreduce.framework.name","yarn");
        conf.set("yarn.resourcemanager.hostname","us1");
        Job job= Job.getInstance();

        //设置maptask和reducertask使用的业务类
        job.setMapperClass(H05sM.class);
        job.setReducerClass(H05sR.class);

        //设置mapper输出数据的kv类型
        job.setMapOutputKeyClass(Hadoop_05_Value.class);
        job.setMapOutputValueClass(Text.class);
        //设置最终输出数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Hadoop_05_Value.class);

        //指定job的输入原始文件所在的目录
        FileInputFormat.setInputPaths(job, new Path("/Users/L/Downloads/dnslogout5/"));
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定job的输出结果
        String path="/Users/L/Downloads/dnslogout5s";
        FileOutputFormat.setOutputPath(job, new Path(path));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //设置该程序的jar包
//        job.setJar("/home/hadoop/wc.jar");
        job.setJarByClass(Hadoop_04.class);     //根据类路径来设置

        //将job中配置的相关参数，以及job所在的java类所在的jar包提交给yarn运行
//        job.submit();
        boolean b = job.waitForCompletion(true);
        long l2=System.currentTimeMillis();
        System.out.println(l2-l1);
        System.exit(b?0:1);
    }
}


class H05sM extends Mapper<LongWritable,Text,Hadoop_05_Value,Text>{
    Hadoop_05_Value hadoop_05_value=new Hadoop_05_Value();
    Text text=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split("\t");
        hadoop_05_value.set(Long.parseLong(split[1]),Long.parseLong(split[2]));
        text.set(split[0]);
        context.write(hadoop_05_value,text);
    }
}


class H05sR extends Reducer<Hadoop_05_Value,Text,Text,Hadoop_05_Value>{
    @Override
    protected void reduce(Hadoop_05_Value key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(values.iterator().next(),key);
    }
}
