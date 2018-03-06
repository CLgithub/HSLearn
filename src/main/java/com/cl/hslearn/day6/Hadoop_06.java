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
import org.apache.log4j.Logger;
import org.ietf.jgss.Oid;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * mapreduce实现类似sql的join功能，联合查询
 订单数据表 t_order;
 +------+----------+-------+--------+
 | id   | date     | pid   | amount |
 +------+----------+-------+--------+
 | 1001 | 20150710 | P0001 |      2 |
 | 1002 | 20150710 | P0001 |      3 |
 | 1003 | 20150710 | P0002 |      3 |
 +------+----------+-------+--------+

 商品信息表 t_product;
 +-------+---------------+-------------+-------+
 | id    | name          | category_id | price |
 +-------+---------------+-------------+-------+
 | P0001 | 魅族mx2       | C01         |     2 |
 | P0002 | 苹果iphone7   | C01         |     3 |
 +-------+---------------+-------------+-------+

 task2
 1001,pd001,300
 1001,pd002,20
 1002,pd003,40
 1003,pd002,50
 */
public class Hadoop_06 {
    public static void main(String[] args) throws Exception {
//        task1();
        task2();
    }
    //利用缓存文件，在map task中实现join，不需要reduce
    public static void task2() throws Exception{
        long l1=System.currentTimeMillis();
        Configuration conf=new Configuration();
        conf.set("mapreduce.framework.name","yarn");    //yarn 或 local
        conf.set("fs.defaultFS","hdfs://us1:9000/");    //当文件系统设置为hdfs后，要是设置用户，在环境变量里设置export HADOOP_USER_NAME="hadoop"
        conf.set("yarn.resourcemanager.hostname","us1");
        Job job= Job.getInstance(conf);
        //设置maptask和reducertask使用的业务类
        job.setMapperClass(Hadoop06_Map2.class);
//        job.setReducerClass(Hadoop06_Reduce1.class);
        //设置mapper输出数据的kv类型
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(NullWritable.class);
        //设置最终输出数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //加载某个指定的文件或jar，到所有maptask运行节点工作目录
//        job.addArchiveToClassPath();    //加载jar包到task运行节点classpath中
//        job.addFileToClassPath();     //加载普通文件到task的classpath中
//        job.addCacheArchive();        //加载压缩文件到task运行节点的工作目录
//        job.addCacheFile();           //加载普通文件到task运行节点的工作目录

//        job.addCacheFile(new URI("file:///Users/L/Downloads/t_product"));    //将产品信息加载到maptask工作目录
        job.addCacheFile(new URI("/hadoop06/t_product"));    //将产品信息加载到maptask工作目录

        job.setNumReduceTasks(0);   //不需要reduce，设置为0提高效率


        //指定job的输入原始文件所在的目录
        FileInputFormat.setInputPaths(job, new Path("/hadoop06/"));
//        FileInputFormat.setInputPaths(job, new Path("hdfs://us1:9000/hadoop06/"));
        //指定job的输出结果
        FileOutputFormat.setOutputPath(job, new Path("/hadoop06out_5"));
//        FileOutputFormat.setOutputPath(job, new Path("file:///Users/L/Downloads/hadoop06out_2"));

        //设置该程序的jar包
//        job.setJar("/home/hadoop/wc.jar");
        job.setJar("/Users/l/develop/clProject/HSLearn/out/artifacts/HSLearn/HSLearn.jar");     //集群运行时必须指定明确路径
//        job.setJarByClass(Hadoop_06.class);     //根据类路径来设置    本地运行时可以通过类路径查找

        //将job中配置的相关参数，以及job所在的java类所在的jar包提交给yarn运行
//        job.submit();
        boolean b = job.waitForCompletion(true);
        long l2=System.currentTimeMillis();
        System.out.println(l2-l1);
        System.exit(b?0:1);
    }

    //在reduce中实现join
    public static void task1() throws Exception{
        long l=System.currentTimeMillis();
        Configuration conf=new Configuration();
        //设置运行模式
        conf.set("mapreduce.framework.name","yarn");    //yarn 或 local
//        conf.set("yarn.defaultFS","file:///");
        conf.set("fs.defaultFS","hdfs://us1:9000/");    //当文件系统设置为hdfs后，要是设置用户，在环境变量里设置export HADOOP_USER_NAME="hadoop"
        conf.set("yarn.resourcemanager.hostname","us1");
        Job job= Job.getInstance();

        //设置maptask和reducertask使用的业务类
        job.setMapperClass(Hadoop06_Map1.class);
        job.setReducerClass(Hadoop06_Reduce1.class);

        //设置mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Hadoop06_bean1.class);
        //设置最终输出数据的kv类型
        job.setOutputKeyClass(Hadoop06_bean1.class);
        job.setOutputValueClass(NullWritable.class);

        //如果文件小而多时，可以采用另一种inputformat（combineFileOutputFormat）来将小文件在逻辑上规划到一个切片中
//        job.setInputFormatClass(CombineTextInputFormat.class);//默认使用textInputFormat
//        CombineTextInputFormat.setMaxInputSplitSize(job,1048576*256);   //256m
//        CombineTextInputFormat.setMinInputSplitSize(job,2097152);   //2m

        //指定job的输入原始文件所在的目录
//        FileInputFormat.setInputPaths(job, new Path("/hadoop06/"));
        FileInputFormat.setInputPaths(job, new Path("hdfs://us1:9000/hadoop06/"));
        //指定job的输出结果
//        FileOutputFormat.setOutputPath(job, new Path("/hadoop06out_1"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/L/Downloads/hadoop06out_1"));

        //设置该程序的jar包
//        job.setJar("/home/hadoop/wc.jar");
//        job.setJar("/Users/l/develop/clProject/HSLearn/out/artifacts/HSLearn/HSLearn.jar");     //集群运行时必须指定明确路径
        job.setJarByClass(Hadoop_06.class);     //根据类路径来设置    本地运行时可以通过类路径查找

        //将job中配置的相关参数，以及job所在的java类所在的jar包提交给yarn运行
//        job.submit();
        boolean b = job.waitForCompletion(true);
        long l2=System.currentTimeMillis();
        System.out.println(l2-l);
        System.exit(b?0:1);
    }
}


class Hadoop06_Map1 extends Mapper<LongWritable,Text,Text,Hadoop06_bean1>{
    Text text=new Text();
    Hadoop06_bean1 hadoop06_bean1=new Hadoop06_bean1();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        InputSplit inputSplit = context.getInputSplit();    //可以通过context得到切片信息
        FileSplit fileSplit= (FileSplit) inputSplit;    //但是inputSplit是抽象的是所有切片信息的抽象，需要转换为具体的实现类才能得到具体信息
        String fileName = fileSplit.getPath().getName();


        String[] split = value.toString().split(",");
        if(fileName.endsWith("order")){
            String pid=split[2];
            text.set(pid);
            hadoop06_bean1.set(split[0],split[1],pid,Integer.parseInt(split[3]),"","",0,0);
        }else if(fileName.endsWith("product")){
            String pid=split[0];
            text.set(pid);
            hadoop06_bean1.set("", "",pid,0,split[1],split[2],Integer.parseInt(split[3]),1);
        }
        context.write(text, hadoop06_bean1);
    }
}

class Hadoop06_Reduce1 extends Reducer<Text,Hadoop06_bean1,Hadoop06_bean1,NullWritable>{
    @Override
    protected void reduce(Text key, Iterable<Hadoop06_bean1> values, Context context) throws IOException, InterruptedException {
//        Hadoop06_bean1 phb=new Hadoop06_bean1();
        List<Hadoop06_bean1> orderlist=new ArrayList<>();   //商品对订单是一对多，所以此处只有一种商品
        List<Hadoop06_bean1> productlist=new ArrayList<>();   //商品对订单是一对多，所以此处只有一种商品
//        System.out.println("k----"+key);
        for(Hadoop06_bean1 hadoop06_bean1:values){  //一类型的商品
//            System.out.println("  v----"+hadoop06_bean1);
            int flag = hadoop06_bean1.getFlag();
            if(flag==1){    //product 一
                Hadoop06_bean1 phb=new Hadoop06_bean1();
                phb.setName(hadoop06_bean1.getName());
                phb.setCategoryId(hadoop06_bean1.getCategoryId());
                phb.setpId(hadoop06_bean1.getpId());
                phb.setPrice(hadoop06_bean1.getPrice());
                phb.setFlag(1);
                productlist.add(phb);
            }else if(flag==0){//order 多
                Hadoop06_bean1 ohb=new Hadoop06_bean1();
                ohb.setoId(hadoop06_bean1.getoId());
                ohb.setDateStr(hadoop06_bean1.getDateStr());
                ohb.setAmount(hadoop06_bean1.getAmount());
                ohb.setpId(hadoop06_bean1.getpId());
                ohb.setFlag(0);
                orderlist.add(ohb);
            }
        }

        System.out.println("pl------"+productlist);
        System.out.println("ol------"+orderlist);
        for(Hadoop06_bean1 bp:productlist){
            for(Hadoop06_bean1 bo:orderlist){
                bo.setName(bp.getName());
                bo.setCategoryId(bp.getCategoryId());
                bo.setPrice(bp.getPrice());
                context.write(bo,NullWritable.get());
            }
        }
    }
}

class Hadoop06_Map2 extends Mapper<LongWritable, Text, Text, NullWritable>{
    List<Hadoop06_bean1> plist=new ArrayList<>();
    //最先调用setup方法
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //也可以直接写文件名，因为就在工作目录下 /tmp/hadoop-l/mapred/local/1520243595996/t_product    ,但不知为何突然不行了，成功过一次
        BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(new FileInputStream("t_product"),"utf-8"));
//        BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(new FileInputStream(new File(context.getCacheFiles()[0])),"utf-8"));
        String line="";
        while((line=bufferedReader.readLine())!=null){
            Hadoop06_bean1 hadoop06_bean1 = new Hadoop06_bean1();
            String[] split = line.split(",");
            hadoop06_bean1.set("", "", split[0],0,split[1],split[2],Integer.parseInt(split[3]),1);
            plist.add(hadoop06_bean1);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");

        FileSplit fileSplit=(FileSplit) context.getInputSplit();
        String fileName=fileSplit.getPath().getName();
        if(fileName.endsWith("order")){
            for(Hadoop06_bean1 hadoop06_bean1:plist){   //hadoop06_bean1只包含产品信息，需要添加订单信息，订单信息在split中
                if(hadoop06_bean1.getpId().equals(split[2])){   //如果pid相同
                    hadoop06_bean1.setoId(split[0]);
                    hadoop06_bean1.setDateStr(split[1]);
                    hadoop06_bean1.setAmount(Integer.parseInt(split[3]));
//                    System.out.println(hadoop06_bean1);
                    context.write(new Text(hadoop06_bean1.toString()),NullWritable.get());
                }
            }
        }
    }
}