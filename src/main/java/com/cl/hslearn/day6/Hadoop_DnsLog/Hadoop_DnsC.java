package com.cl.hslearn.day6.Hadoop_DnsLog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 hadoop
    map reduce 总结
 主要业务逻辑在map及reduce中完成，map负责将各种类型的数据抽象成key values形式，交给reduce对抽象过的数据进行运算，从而可以处理各种形式的数据


 */
public class Hadoop_DnsC {
    public static void main(String[] args) throws Exception{
        task1();
    }

    public static void task1() throws Exception{
        Configuration conf=new Configuration();
        conf.set("mapreduce.framework.name","yarn");    //设置运行模式    yarn 或 local
        conf.set("fs.defaultFS","hdfs://us1:9000/");    //当文件系统设置为hdfs后，要是设置用户，在环境变量里设置export HADOOP_USER_NAME="hadoop"
        conf.set("yarn.resourcemanager.hostname","us1");
        Job job= Job.getInstance();

        job.setMapperClass(XMap1.class);    //设置mapclass
        job.setReducerClass(XReduce1.class);    //设置reduceclass
        job.setMapOutputKeyClass(XKey1.class);      //设置map的key
        job.setMapOutputValueClass(XValue1.class);      //设置map的value
        job.setOutputKeyClass(XKey1.class);         //设置最终输出的key
        job.setOutputValueClass(XValue1.class);     //设最终输出的value

        job.setGroupingComparatorClass(XKey1Group1.class);      //设置分组器
        job.setCombinerClass(XReduce1.class);                   //在不影响业务逻辑的前提写，可以使用combinerClass先合并部分map输出

        job.setOutputFormatClass(DBOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/l/Downloads/hadoopX/"));    //设置源数据
//        FileOutputFormat.setOutputPath(job, new Path("/Users/l/Downloads/hadoopXout_1"));   //设置目的数据

        //设置该程序的jar包
//        job.setJar("/home/hadoop/wc.jar");
//        job.setJar("/Users/l/develop/clProject/HSLearn/out/artifacts/HSLearn/HSLearn.jar");     //集群运行时必须指定明确路径
        job.setJarByClass(Hadoop_DnsC.class);     //根据类路径来设置    本地运行时可以通过类路径查找


        //将job中配置的相关参数，以及job所在的java类所在的jar包提交给yarn运行
//        job.submit();
        boolean b = job.waitForCompletion(true);
//        System.exit(b?0:1);
    }
}

/**
 * mapperClass1 继承mapper类，四个泛型分别对应
 KEYNIN：    默认情况下，是mr框架所读到的一行文本的起始偏移量，Long，但是再hadoop中呦自己的根精简的序列化接口，所以不直接用Long，而用LongWritable
 VALUEIN：   默认情况下，是mr框架所读到的一行文本的内容，String，同上，用Text
 KEYOUT：    map的key的类型
 VALUEOUT：  map的value的类型
 */
class XMap1 extends Mapper<LongWritable, Text, XKey1, XValue1>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\\|");
        String[] ips=split[3].split(";");
        for(String ip:ips){
            context.write(new XKey1(split[1],split[2],ip),new XValue1(1));
        }
    }
}

/**
 * reduceClass1 继承reduce
 * 四个泛型分别对应<map的key类型，map的values类型，reduce输出的key类型，reduce输出的value类型>
 */
class XReduce1 extends Reducer<XKey1, XValue1, XKey1, XValue1>{
    @Override
    protected void reduce(XKey1 key, Iterable<XValue1> values, Context context) throws IOException, InterruptedException {
        int count=0;
        for(XValue1 xValue1:values){
            count+=xValue1.getNum();
        }
//        System.out.println(key+", "+count);
        context.write(key,new XValue1(count));
    }
}

class XKey1 implements WritableComparable{
    //用于排序
    @Override
    public int compareTo(Object o) {
        XKey1 xKey1= (XKey1) o;
        return (xKey1.getDomain()+xKey1.getTimeStr()).compareTo(this.getDomain()+this.getTimeStr());  //忽略ip
//        return xKey1.toString().compareTo(this.toString());
    }
    //定义对象写出时的序列化结构
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(domain);
        dataOutput.writeUTF(timeStr);
        dataOutput.writeUTF(ip);
    }
    //定义对象在读入时的序列化结构
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        domain=dataInput.readUTF();
        timeStr=dataInput.readUTF();
        ip=dataInput.readUTF();
    }

    private String domain;
    private String timeStr;
    private String ip;

    public XKey1() {
    }

    public XKey1(String domain, String timeStr, String ip) {
        this.domain = domain;
        this.timeStr = timeStr;
        this.ip = ip;
    }

    @Override
    public String toString() {
        return domain +", " + timeStr + ", " + ip ;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getTimeStr() {
        return timeStr;
    }

    public void setTimeStr(String timeStr) {
        this.timeStr = timeStr;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
}

class XKey1Group1 extends WritableComparator{
    public XKey1Group1() {
        super(XKey1.class,true);    //暂时不太懂
    }
    // 用于分组
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        XKey1 xKey1a= (XKey1) a;
        XKey1 xKey1b= (XKey1) b;
        return xKey1a.compareTo(xKey1b);
    }
}

class XValue1 implements WritableComparable{

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(num);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        num=dataInput.readInt();
    }
    private Integer num;

    public XValue1() {
    }

    public XValue1(Integer num) {
        this.num = num;
    }

    @Override
    public String toString() {
        return "XValue1{" +
                "num=" + num +
                '}';
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }
}

/**
 * 自定义数据处理分区器，两个泛型与mapTask输出类型对应
 * 数据处理分区器确定了mapTask输出的数据分为几个区存放，每个区会有一个reduceTask去处理，
 * 从而决定了会产生几个reduceTask，每个reduceTask处理的数据存储到各种的结果文件
 */
class XPartitionner extends Partitioner<XKey1, XValue1>{
    @Override
    public int getPartition(XKey1 xKey1, XValue1 xValue1, int i) {
        if(xKey1.getDomain().startsWith("www")){
            return 1;
        }else{
            return 0;
        }
    }
}
