package com.cl.hslearn.day6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 实现索引
 a.txt
    hello tom
    hello jerry
    hello tom
 b.txt
    hello jerry
    hello jerry
    tom jerry
 c.txt
    hello jerry
    hello tom
 输出：
    hello a.txt-->3 b.txt-->2 c.txt-->2
    jerry a.txt-->1 b.txt-->3 c.txt-->1
    ...
 */
public class Hadoop_08 {
    public static void main(String[] args) throws Exception {
        task1();
    }

    public static void task1() throws Exception {
        long l=System.currentTimeMillis();
        Job job= Job.getInstance();

        //设置maptask和reducertask使用的业务类
        job.setMapperClass(Hadoop08Map1.class);
        job.setReducerClass(Hadoop08Reduce.class);

        //设置mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //设置最终输出数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path("hdfs://us1:9000/hadoop08/"));
        //指定job的输出结果
//        FileOutputFormat.setOutputPath(job, new Path("/hadoop06out_1"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/L/Downloads/hadoop08out_1"));

        //设置该程序的jar包
//        job.setJar("/home/hadoop/wc.jar");
//        job.setJar("/Users/l/develop/clProject/HSLearn/out/artifacts/HSLearn/HSLearn.jar");     //集群运行时必须指定明确路径
        job.setJarByClass(Hadoop_08.class);     //根据类路径来设置    本地运行时可以通过类路径查找

        //将job中配置的相关参数，以及job所在的java类所在的jar包提交给yarn运行
//        job.submit();
        boolean b = job.waitForCompletion(true);
        long l2=System.currentTimeMillis();
        System.out.println(l2-l);
        System.exit(b?0:1);
    }
}


class Hadoop08Map1 extends Mapper<LongWritable, Text, Text, LongWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        FileSplit fileSplit= (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        for(String w:split){
            Text text = new Text(fileName + "-" + w);
            context.write(text,new LongWritable(1));
        }
    }
}

class Hadoop08Reduce extends Reducer<Text, LongWritable, Text, LongWritable>{
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count=0;
        for (LongWritable longWritable:values){
            count+=longWritable.get();
        }
        context.write(key, new LongWritable(count));
    }
}