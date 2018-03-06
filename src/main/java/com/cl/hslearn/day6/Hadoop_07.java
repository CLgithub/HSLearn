package com.cl.hslearn.day6;

import com.sun.xml.internal.xsom.impl.scd.Iterators;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.swing.*;
import java.io.IOException;
import java.util.*;

/**
 查找两两共同好友： 好友是单向的
 A:B,C,D,F,E,O
 B:A,C,E,K
 C:F,A,D,I
 E:B,C,D,M,L
 F:A,B,C,D,E,O,M
 G:A,C,D,E,F
 H:A,C,D,E,O
 I:A,O
 J:B,O
 K:A,C,D
 L:D,E,F
 M:E,F,G
 O:A,H,I,J

 比如：
    A-B:C,E
    ...
 */
public class Hadoop_07 {
    public static void main(String[] args) throws Exception {
        task2();
        task2_2();
    }

    public static void task2() throws Exception {
        long l=System.currentTimeMillis();
        Job job= Job.getInstance();

        //设置maptask和reducertask使用的业务类
        job.setMapperClass(Hadoop07Map2.class);
        job.setReducerClass(Hadoop07Reduce2.class);

        //设置mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置最终输出数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("hdfs://us1:9000/hadoop07/"));
        //指定job的输出结果
//        FileOutputFormat.setOutputPath(job, new Path("/hadoop06out_1"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/L/Downloads/hadoop07out_1"));

        //设置该程序的jar包
//        job.setJar("/home/hadoop/wc.jar");
//        job.setJar("/Users/l/develop/clProject/HSLearn/out/artifacts/HSLearn/HSLearn.jar");     //集群运行时必须指定明确路径
        job.setJarByClass(Hadoop_07.class);     //根据类路径来设置    本地运行时可以通过类路径查找

        //将job中配置的相关参数，以及job所在的java类所在的jar包提交给yarn运行
//        job.submit();
        boolean b = job.waitForCompletion(true);
        long l2=System.currentTimeMillis();
        System.out.println(l2-l);
//        System.exit(b?0:1);
    }

    public static void task2_2() throws Exception {
        long l=System.currentTimeMillis();
        Job job= Job.getInstance();

        //设置maptask和reducertask使用的业务类
        job.setMapperClass(Hadoop07Map2_2.class);
        job.setReducerClass(Hadoop07Reduce2_2.class);

        //设置mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置最终输出数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/L/Downloads/hadoop07out_1"));
        //指定job的输出结果
//        FileOutputFormat.setOutputPath(job, new Path("/hadoop06out_1"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/L/Downloads/hadoop07out_1_2"));

        //设置该程序的jar包
//        job.setJar("/home/hadoop/wc.jar");
//        job.setJar("/Users/l/develop/clProject/HSLearn/out/artifacts/HSLearn/HSLearn.jar");     //集群运行时必须指定明确路径
        job.setJarByClass(Hadoop_07.class);     //根据类路径来设置    本地运行时可以通过类路径查找

        //将job中配置的相关参数，以及job所在的java类所在的jar包提交给yarn运行
//        job.submit();
        boolean b = job.waitForCompletion(true);
        long l2=System.currentTimeMillis();
        System.out.println(l2-l);
        System.exit(b?0:1);
    }
}

class Hadoop07Map2_2 extends Mapper<LongWritable, Text, Text, Text>{
    //A-F	D
    //A-F	E
    //...
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        context.write(new Text(split[0]),new Text(split[1]));
    }
}
class Hadoop07Reduce2_2 extends Reducer<Text, Text, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> list=new ArrayList<>();
        for(Text text:values){
            list.add(text.toString());
        }
        context.write(key,new Text(list.toString()));
    }
}


//先找出，某人是哪些人的好友，然后把这些人两两拼结起来就行
class Hadoop07Map2 extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] user_f= value.toString().split(":");
        String user=user_f[0];
        String[] fs=user_f[1].split(",");
        for(String f:fs){
            context.write(new Text(f),new Text(user));
        }
    }
}
class Hadoop07Reduce2 extends Reducer<Text, Text, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> users=new ArrayList<>();
        for(Text user:values){
            users.add(user.toString());
        }
        //将两两用户和在一起，但要注意A-B 和 B-A
        for(int i=0;i<users.size()-1;i++){
            for(int j=i+1;j<users.size();j++){
                int c = users.get(i).compareTo(users.get(j));
                Text k=null;
                if(c<0){
                    k=new Text(users.get(i)+"-"+ users.get(j));
                }else{
                    k=new Text(users.get(j)+"-"+ users.get(i));
                }
                context.write(k,key);
            }
        }
    }
}

class Hadoop07Map1 extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] user_f= value.toString().split(":");
        String user=user_f[0];
        String[] fs=user_f[1].split(",");
        for(String f:fs){
            context.write(new Text(user),new Text(f));  //以用户为key，改用户的每个好友分别为key
        }
    }
}

class Hadoop07Reduce1 extends Reducer<Text, Text, Text, Text>{
    List<Map<Text,Iterable<Text>>> list=new ArrayList<>();  //不知为何这样写里面的值会变成一样的
    private List<Map<String,List<String>>> list2=new ArrayList<>(); //但是这样还是有缺陷，如果数据两巨大，list可能会被撑破
    List<Map<String,Iterable<Text>>> list3=new ArrayList<>();
    // a b,c,f...
    // b a,c...
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        reduce1(key, values, context);
        reduce2(key, values, context);
    }

    //不知为何，这样写list里的值会变成一样的，可能是引用类型类似数组
    private void reduce1(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        HashMap<Text, Iterable<Text>> map = new HashMap<>();
        map.put(key,values);
        list.add(map);

        if(list.size()>=2){
            for(long j=0;j<=list.size()-2;j++){ //每次来一个新的，只需要找出新的和其他老的的共同好友，j代表老的的下标，新来的下标为list.size()-1
                String nk= list.get(list.size() - 1).entrySet().iterator().next().getKey().toString();
                String ok= list.get((int) j).entrySet().iterator().next().getKey().toString();
                Iterable<Text> nvs= list.get(list.size() - 1).entrySet().iterator().next().getValue();
                Iterable<Text> ovs= list.get((int) j).entrySet().iterator().next().getValue();
                Text k = new Text(nk +"-"+ ok);
                for(Text nv:nvs){
                    for(Text ov:ovs){
                        if(nv.equals(ov)){
                            context.write(k,nv);
                        }
                    }
                }
            }
        }
    }

    private void reduce2(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, List<String>> map=new HashMap<>();
        List<String> vs=new ArrayList<>();
        for(Text t:values){
            vs.add(t.toString());
        }
        map.put(key.toString(),vs);
        list2.add(map);

        if(list2.size()>=2){
            for(int j=0;j<=list2.size()-2;j++){
                String nk=list2.get(list2.size()-1).entrySet().iterator().next().getKey();
                String ok=list2.get(j).entrySet().iterator().next().getKey();
                String k=ok+"-"+nk;
                List<String> nvs=list2.get(list2.size()-1).entrySet().iterator().next().getValue();
                List<String> ovs=list2.get(j).entrySet().iterator().next().getValue();
                List<String> comlist=new ArrayList<>();
                for(String nv:nvs){
                    for(String ov:ovs){
                        if(nv.equals(ov)){
                            comlist.add(ov);
                        }
                    }
                }
                if(comlist.size()>0){
                    context.write(new Text(k),new Text(comlist.toString()));
                }
            }
        }
    }
}
