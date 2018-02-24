package com.cl.hslearn.day6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;

/**
 * 自定义输出结果，统计每个手机好的上下行流量总和
 * 数据存储到hdfs中，会按块（128m）存储，处理数据时，会分为多个切片来处理，一个切片对应一个mapTask，
 * 切片大小决定了切片数量
 * 源码（FileInputFormat）中切片的大小=Math.max(minSize,Math.min(maxSize,blockSize))
 * 在最大大小与块大小间取一个小的，
 *  如果 maxSize < blockSize 则切片大小 = maxSize
 *  如果 maxSize > blockSize 则切片大小 = blockSize
 *  切片大小至少是最小大小，最大是blockSize大小
 *
 */
public class Hadoop_05 {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        long l1=System.currentTimeMillis();

        Configuration conf=new Configuration();
        conf.set("mapreduce.framework.name","yarn");
        conf.set("yarn.resourcemanager.hostname","us1");
        Job job= Job.getInstance();

        //设置maptask和reducertask使用的业务类
        job.setMapperClass(Hadoop05_Map1.class);
        job.setReducerClass(Hadoop05_Reduce1.class);

        //设置分割类型(数据分区器)
        job.setPartitionerClass(Hadoop05_Partitionner.class);
        //同时指定相应分区数量的reduceTask
        job.setNumReduceTasks(Hadoop05_Partitionner.pDictMap.size()+1);

        //设置mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Hadoop05_Value.class);
        //设置最终输出数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Hadoop05_Value.class);

        //指定job的输入原始文件所在的目录
//        FileInputFormat.setInputPaths(job, new Path("hdfs://us1:9000/datatest/"));
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定job的输出结果
//        FileOutputFormat.setOutputPath(job, new Path("/Users/L/Downloads/dnslogout5"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

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

class Hadoop05_Map1 extends Mapper<LongWritable, Text, Text, Hadoop05_Value>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        String phoneNbr=split[1];
        long upData=Long.parseLong(split[split.length-3]);
        long dData=Long.parseLong(split[split.length-2]);
        Hadoop05_Value hadoop05_value = new Hadoop05_Value(upData,dData);
        context.write(new Text(phoneNbr),hadoop05_value);
    }
}

class Hadoop05_Reduce1 extends Reducer<Text, Hadoop05_Value, Text, Hadoop05_Value>{
    @Override
    protected void reduce(Text key, Iterable<Hadoop05_Value> values, Context context) throws IOException, InterruptedException {
        long sumu=0;
        long sumd=0;
        for(Hadoop05_Value hadoop05_value:values){
            sumd += hadoop05_value.getDownNumber();
            sumu += hadoop05_value.getUpNumber();
        }
        Hadoop05_Value hadoop05_value = new Hadoop05_Value(sumu,sumd);
        context.write(key,hadoop05_value);
    }
}


/**
 * 自定义数据处理分区器，两个泛型与mapTask输出类型对应
 * 数据处理分区器确定了mapTask输出的数据分为几个区存放，每个区会有一个reduceTask去处理，
 * 从而决定了会产生几个reduceTask，每个reduceTask处理的数据存储到各种的结果文件
 */
class Hadoop05_Partitionner extends Partitioner<Text, Hadoop05_Value> {

    public static HashMap<String, Integer> pDictMap=new HashMap<>();
    static {
        pDictMap.put("134",0);
        pDictMap.put("135",1);
        pDictMap.put("136",2);
        pDictMap.put("137",3);
    }
    @Override
    public int getPartition(Text text, Hadoop05_Value hadoop05_value, int numPartitions) {
        String phoneN = text.toString();
        Integer integer = pDictMap.get(phoneN.substring(0, 3));
        return integer==null?4:integer;
    }
}
