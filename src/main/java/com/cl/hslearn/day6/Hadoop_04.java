package com.cl.hslearn.day6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.security.Key;
import java.util.HashMap;


/**
 * 第二个mapreduce程序：自定义输出结果中key与value
 */
public class Hadoop_04 {
    public static void main(String[] args) throws Exception {
        long l=System.currentTimeMillis();
        Configuration conf=new Configuration();
        conf.set("mapreduce.framework.name","yarn");
        conf.set("yarn.resourcemanager.hostname","us1");
        Job job= Job.getInstance();

        //设置maptask和reducertask使用的业务类
        job.setMapperClass(DnsLogMap.class);
        job.setReducerClass(DnsLogReduce.class);

        //设置分割类型(数据分区器)
//        job.setPartitionerClass(DnsDomainPartitionner.class);
//        //同时指定相应分区数量的reduceTask
//        job.setNumReduceTasks(DnsDomainPartitionner.domainTypeDict.size()+1);

        //设置mapper输出数据的kv类型
        job.setMapOutputKeyClass(Hadoop_04_DnsKey.class);
        job.setMapOutputValueClass(Hadoop_04_DnsValue.class);
        //设置最终输出数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //指定job的输入原始文件所在的目录
//        FileInputFormat.setInputPaths(job, new Path("hdfs://us1:9000/datatest/"));
//        FileInputFormat.setInputPaths(job, new Path("hdfs://us1:9000/javaAPI/upload/dnslog/"));
        FileInputFormat.setInputPaths(job, new Path("hdfs://us1:9000/javaAPI/upload/dnslog2/"));
//        FileInputFormat.setInputPaths(job, new Path("/Users/L/Downloads/dnslog/log/"));
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定job的输出结果
        FileOutputFormat.setOutputPath(job, new Path("/Users/L/Downloads/dnslogout4"));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //设置该程序的jar包
//        job.setJar("/home/hadoop/wc.jar");
        job.setJarByClass(Hadoop_04.class);     //根据类路径来设置

        //将job中配置的相关参数，以及job所在的java类所在的jar包提交给yarn运行
//        job.submit();
        boolean b = job.waitForCompletion(true);
        long l2=System.currentTimeMillis();
        System.out.println(l2-l);
        System.exit(b?0:1);
    }
}


// 输出key是符合类型时，key需要自定义,应当重写，输出结果为符合类型时，输出value需要自定义
class DnsLogMap extends Mapper<LongWritable, Text, Hadoop_04_DnsKey, Hadoop_04_DnsValue> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line= value.toString();
        // 117.135.235.176|WWW.SINA.COM.CN|20171128031003|117.135.252.140;117.135.252.141|0
        String[] split = line.split("\\|");//切分
        String domain=split[1];
        for(String sip:split[3].split(";")){    //有可能之一一个ip，注意能否切割
            Hadoop_04_DnsKey hadoopp_04_dnsKey = new Hadoop_04_DnsKey();
            hadoopp_04_dnsKey.setDomain(domain);
            hadoopp_04_dnsKey.setSip(sip);
            hadoopp_04_dnsKey.setTimeStr(split[2]);
//            System.out.println(hadoopp_04_dnsKey);

            Hadoop_04_DnsValue hadoop_04_dnsValue=new Hadoop_04_DnsValue();
            hadoop_04_dnsValue.setDomain(domain);
            hadoop_04_dnsValue.setSip(sip);
            hadoop_04_dnsValue.setTimeStr(split[2]);
            hadoop_04_dnsValue.setSum(1);
//            System.out.println(hadoop_04_dnsValue);
//            context.write(new Text(domain), hadoop_04_dnsValue);
            context.write(hadoopp_04_dnsKey, hadoop_04_dnsValue);
        }
    }
}

//class DnsLogReduce extends Reducer<Text, Hadoop_04_DnsValue, Text, Hadoop_04_DnsValue>{
class DnsLogReduce extends Reducer<Hadoop_04_DnsKey, Hadoop_04_DnsValue, Text, LongWritable>{
    @Override
    protected void reduce(Hadoop_04_DnsKey key, Iterable<Hadoop_04_DnsValue> values, Context context) throws IOException, InterruptedException {
        int count=0;
        for(Hadoop_04_DnsValue l:values){
//            count+=l.get();
            count+=l.getSum();
//            l.setSum(count);
        }
        context.write(new Text(key.toString()),new LongWritable(count));
    }
}


/**
 * 继承partitioner，实现根据map的key分类，将不同分类的结果保存到不同文件(多个reduceTask)
 * 两个泛型对应map的输出类型kv
 */
class DnsDomainPartitionner extends Partitioner<Text,Hadoop_04_DnsValue> {
    public static HashMap<String,Integer> domainTypeDict=new HashMap<>();

    static {
        domainTypeDict.put(".com",0);
        domainTypeDict.put(".cn",1);
        domainTypeDict.put(".hk",2);
    }

    @Override
    public int getPartition(Text text, Hadoop_04_DnsValue hadoop_04_dnsValue, int i) {
        String domain=text.toString();
        int i1 = domain.lastIndexOf(".");
        String type = domain.substring(i1, domain.length()).toLowerCase();
        System.out.println(type);
        Integer domainId = domainTypeDict.get(type);
        return domainId==null?4:domainId;
    }
}
