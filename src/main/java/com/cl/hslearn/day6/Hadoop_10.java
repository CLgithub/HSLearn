package com.cl.hslearn.day6;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 排序与分组实践
 a,2
 b,3
 a,3
 a,1
 c,2
 b,2

 按两列分组，按第二列排序
 */
public class Hadoop_10 {
    public static void main(String[] args) throws Exception{
        task1();
    }
    public static void task1() throws Exception{
        long l=System.currentTimeMillis();
        Job job= Job.getInstance();

        //设置maptask和reducertask使用的业务类
        job.setMapperClass(Hadoop10map.class);
        job.setReducerClass(Hadoop10reudce.class);

        //设置mapper输出数据的kv类型
        job.setMapOutputKeyClass(Hadoop10keybean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置最终输出数据的kv类型
        job.setOutputKeyClass(Hadoop10reudce.class);
        job.setOutputValueClass(NullWritable.class);

        //设置分组器
        job.setGroupingComparatorClass(Hadoop10GroupingCom.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/l/Downloads/hadoop10/"));
        //指定job的输出结果
        FileOutputFormat.setOutputPath(job, new Path("/Users/l/Downloads/hadoop10out_1"));

        //设置该程序的jar包
//        job.setJar("/home/hadoop/wc.jar");
//        job.setJar("/Users/l/develop/clProject/HSLearn/out/artifacts/HSLearn/HSLearn.jar");     //集群运行时必须指定明确路径
        job.setJarByClass(Hadoop_10.class);     //根据类路径来设置    本地运行时可以通过类路径查找

        //将job中配置的相关参数，以及job所在的java类所在的jar包提交给yarn运行
//        job.submit();
        boolean b = job.waitForCompletion(true);
        long l2=System.currentTimeMillis();
        System.out.println(l2-l);
        System.exit(b?0:1);
    }
}

class Hadoop10map extends Mapper<LongWritable, Text, Hadoop10keybean, NullWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        context.write(new Hadoop10keybean(split[0],Integer.parseInt(split[1])),NullWritable.get());
    }
}

class Hadoop10reudce extends Reducer<Hadoop10keybean, NullWritable, Hadoop10keybean, NullWritable>{
    @Override
    protected void reduce(Hadoop10keybean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}

class Hadoop10keybean implements WritableComparable {

    @Override
    public int compareTo(Object o) {
        Hadoop10keybean hadoop10keybean= (Hadoop10keybean) o;
        int i=this.one.compareTo(hadoop10keybean.one);
        if(i==0){
            i=-this.tow.compareTo(hadoop10keybean.tow);
        }
        return i;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(one);
        dataOutput.writeInt(tow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        one=dataInput.readUTF();
        tow=dataInput.readInt();
    }

    private String one;
    private Integer tow;

    public Hadoop10keybean() {
    }

    @Override
    public String toString() {
        return one+"--"+tow;
    }

    public Hadoop10keybean(String one, Integer tow) {
        this.one = one;
        this.tow = tow;
    }

    public String getOne() {
        return one;
    }

    public void setOne(String one) {
        this.one = one;
    }

    public Integer getTow() {
        return tow;
    }

    public void setTow(Integer tow) {
        this.tow = tow;
    }
}

class Hadoop10GroupingCom extends WritableComparator {
    public Hadoop10GroupingCom(){
        super(Hadoop10keybean.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Hadoop10keybean h1= (Hadoop10keybean) a;
        Hadoop10keybean h2= (Hadoop10keybean) b;
        return ((Hadoop10keybean) a).getOne().compareTo(((Hadoop10keybean) b).getOne());
//        return super.compare(a, b);
    }
}
