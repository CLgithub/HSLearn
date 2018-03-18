package icp;

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

public class DomainNum {

    public static void main(String[] args) throws Exception {
        task2();
    }

    public static void task2() throws Exception {
        long l=System.currentTimeMillis();
        Configuration conf=new Configuration();
        conf.set("mapreduce.framework.name","yarn");    //yarn 或 local
        conf.set("fs.defaultFS","hdfs://us1:9000/");    //当文件系统设置为hdfs后，要是设置用户，在环境变量里设置export HADOOP_USER_NAME="hadoop"
        conf.set("yarn.resourcemanager.hostname","us1");
        Job job= Job.getInstance(conf);

        //设置maptask和reducertask使用的业务类
        job.setMapperClass(DomainNumMap.class);
        job.setReducerClass(DomainNumReduce.class);

        //设置mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //设置最终输出数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/icp/"));
        //指定job的输出结果
        FileOutputFormat.setOutputPath(job, new Path("/icpout"));

        //设置该程序的jar包
//        job.setJar("/home/hadoop/wc.jar");
//        job.setJar("/Users/l/develop/clProject/HSLearn/out/artifacts/HSLearn/HSLearn.jar");     //集群运行时必须指定明确路径
        job.setJarByClass(DomainNum.class);     //根据类路径来设置    本地运行时可以通过类路径查找

        //将job中配置的相关参数，以及job所在的java类所在的jar包提交给yarn运行
//        job.submit();
        boolean b = job.waitForCompletion(true);
        long l2=System.currentTimeMillis();
        System.out.println(l2-l);
//        System.exit(b?0:1);
    }

}

class DomainNumMap extends Mapper<LongWritable, Text, Text, LongWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        context.write(new Text(split[0].toString()),new LongWritable(1));
    }
}

class DomainNumReduce extends Reducer<Text, LongWritable, Text, LongWritable>{
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count=0;
        for(LongWritable longWritable:values){
            count+=1;
        }
        context.write(key,new LongWritable(count));
    }
}