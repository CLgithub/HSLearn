package com.cl.hslearn.day6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * Created by L on 18/2/16.
 * 第一个mapreduce程序计算出文件中每个单词出现的数量
 *
 * 在提交之前会先分析数据，形成数据处理规划 job.split，配合job.xml及wc.jar，提交给yarn
 * yarn根据resourceManager启动mr appMaster，再根据数据处理规划启动mapTask，
 *      mapTask的数量由切片大小确定
 *      切片大小=Math.max(minSize,Math.min(maxSize,blockSize))
 *          在最大大小与块大小间取一个小的，
 *          如果 maxSize < blockSize 则切片大小 = maxSize
 *          如果 maxSize > blockSize 则切片大小 = blockSize
 *          切片大小至少是最小大小，最大是blockSize大小
 * mapTask调用InputFormat读取数据kv（偏移量,数据内容），交给我们写的WordcountMapper，WordcountMapper自己的输出交给outputCollector（输出收集器），
 * 输出到一个文件，这个文件可以分区(分区由Partitioner确定)，根据分区可以交给不同的reduceTask来处理不同分区的数据，结果写到不同文件
 * mapTask运行完成后myappMaster开始启动reduceTask，reduceTask调用我们写的WordcountReducer，WordcountReducer通过outputFormat将
 * 结果数据写出
 *
 */
public class Hadoop_03 {

//    static FileSystem fs;
//
//    static {
//        try {
//            fs= FileSystem.get(new URI("hdfs://us1:9000"), new Configuration(), "hadoop");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    /**
     * 相当于yarn集群的客户端，需要封装我们的mr程序的相关运行参数，指定jar包
     * *unx可以放到本地就执行，也可以打包放到us1上去执行，可以使用hadoop jar来加载hadoop所需jar包，配合两个参数
     * hadoop jar wordCount1.jar com.cl.hslearn.day6.Hadoop_03 /javaAPI/upload/dnslog/ /javaAPI/upload/dnslogout2
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        long l=System.currentTimeMillis();
        Configuration conf=new Configuration();
        //设置运行模式
        conf.set("mapreduce.framework.name","yarn");    //yarn 或 local
//        conf.set("yarn.defaultFS","file:///");
        conf.set("fs.defaultFS","hdfs://us1:9000/");    //当文件系统设置为hdfs后，要是设置用户，在环境变量里设置export HADOOP_USER_NAME="hadoop"
        conf.set("yarn.resourcemanager.hostname","us1");
        Job job= Job.getInstance(conf);

        //设置maptask和reducertask使用的业务类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);
        //指定combiner在map到reduce之间先做聚合，由于业务逻辑和reduce一样切使用后不会影响业务逻辑正确性，所有直接使用reduce
        job.setCombinerClass(WordcountReducer.class);

        //设置mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置最终输出数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //如果文件小而多时，可以采用另一种inputformat（combineFileOutputFormat）来将小文件在逻辑上规划到一个切片中
        job.setInputFormatClass(CombineTextInputFormat.class);//默认使用textInputFormat
        CombineTextInputFormat.setMaxInputSplitSize(job,1048576*256);   //256m
        CombineTextInputFormat.setMinInputSplitSize(job,2097152);   //2m

        //设置切片大小
//        FileInputFormat.setMaxInputSplitSize(job,1048576*128);
//        FileInputFormat.setMinInputSplitSize(job,1);  //1k
        //指定job的输入原始文件所在的目录
//        FileInputFormat.setInputPaths(job, new Path("hdfs://us1:9000/javaAPI/upload/dnslog/"));
        FileInputFormat.setInputPaths(job, new Path("/javaAPI/upload/dnslog/"));
//        FileInputFormat.setInputPaths(job, new Path("/Users/L/Downloads/t3"));
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定job的输出结果
        FileOutputFormat.setOutputPath(job, new Path("/Users/L/Downloads/dnslogout3_3"));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //设置该程序的jar包
//        job.setJar("/home/hadoop/wc.jar");
        job.setJar("/Users/l/develop/clProject/HSLearn/out/artifacts/HSLearn/HSLearn.jar");     //集群运行时必须指定明确路径
//        job.setJarByClass(Hadoop_03.class);     //根据类路径来设置    本地运行时可以通过类路径查找

        //将job中配置的相关参数，以及job所在的java类所在的jar包提交给yarn运行
//        job.submit();
        boolean b = job.waitForCompletion(true);
        long l2=System.currentTimeMillis();
        System.out.println(l2-l);
        System.exit(b?0:1);
    }


}

/**
 * map task 实现类
 KEYNIN：    默认情况下，是mr框架所读到的一行文本的起始偏移量，Long，但是再hadoop中呦自己的根精简的序列化接口，所以不直接用Long，而用LongWritable
 VALUEIN：   默认情况下，是mr框架所读到的一行文本的内容，String，同上，用Text
 KEYOUT：    是用户自定义逻辑完成之后输出数据中的key，此处是单词，string，用Text
 VALUEOUT：  是用户自定义逻辑完成之后输出数据中的value，此处是数量，Integer，用IntWritable
 */
class WordcountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

    /**
     * map阶段的业务逻辑
     * maptask会对每一行输入数据调用一次map()方法
     * @param key 当前行的输入偏移量
     * @param value 当前行的内容
     * @param context map输出的封装
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line= value.toString();
        String[] split = line.split("\\|");//切分
        // 117.135.235.176|WWW.SINA.COM.CN|20171128031003|117.135.252.140;117.135.252.141|0
        //将单词统计为<单词,1>
        String domain=split[1].toLowerCase();
        context.write(new Text(domain),new IntWritable(1));
    }

}

/**
 * reduce task 实现类
 KEYNIN,VALUEIN：    reduce的输入类型和map的输出类型对应
 KEYOUT：            单词
 VALUEOUT：          某单词的总次数
 */
class WordcountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

    /**
     * reduce 阶段业务逻辑
     * @param key 一组相同单词kv对的key     <www.google.com,1><wwww.gogle.com,1>...
     * @param values 上面key的可迭代值的集合
     * @param context reducetask 的输出封装
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;
//        Iterator<IntWritable> iterator = values.iterator();
//        while(iterator.hasNext()){iterator.next();}
        for(IntWritable value:values){
            count+=value.get();
        }

        context.write(key,new IntWritable(count));
    }
}





