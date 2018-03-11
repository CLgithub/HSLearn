package com.cl.hslearn.day6;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 有如下订单：
 订单id   商品id    金额
 O_001   P01       222.8    ✅
 O_001   P05       25.8
 O_002   P03       522.4
 O_002   P05       25.8
 O_002   P06       732.5    ✅
 O_003   P01       222.8    ✅
 需要求出没一个订单中成交金额最大的一笔交易

 思路：
    1。可以通过相同id号，聚合同一个订单id的各个价格得到最大的
    2。可以通过自定义map输出key，compareTo排序，
 */
public class Hadoop_09 {
    public static void main(String[] args) throws Exception{
        task1();
    }
    public static void task1() throws Exception{
        long l=System.currentTimeMillis();
        Job job= Job.getInstance();

        //设置maptask和reducertask使用的业务类
        job.setMapperClass(Hadoop09Map1.class);
        job.setReducerClass(Hadoop09Reduce1.class);

        //设置mapper输出数据的kv类型
        job.setMapOutputKeyClass(Hadoop09KeyBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置最终输出数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置分组器
        job.setGroupingComparatorClass(Hadoop09GroupingCompatator.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/l/Downloads/hadoop09/"));
        //指定job的输出结果
//        FileOutputFormat.setOutputPath(job, new Path("/hadoop06out_1"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/l/Downloads/hadoop09out_1"));

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


class Hadoop09Map1 extends Mapper<LongWritable, Text, Hadoop09KeyBean, NullWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        context.write(new Hadoop09KeyBean(split[0],Float.parseFloat(split[2])), NullWritable.get());
    }
}

class Hadoop09Reduce1 extends Reducer<Hadoop09KeyBean, NullWritable, Text, Text>{
    @Override
    protected void reduce(Hadoop09KeyBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("key:"+key);
//        super.reduce(key, values, context);
        context.write(new Text(key.toString()),new Text(""));
    }
}



class Hadoop09KeyBean implements WritableComparable {

    /**
     * -1,-1  倒序
     * -1, 0  最后一个
     * -1, 1  倒序

     * 0,-1  倒序
     * 0, 0  最后一个
     * 0, 1  倒序

     * 1,-1  正序
     * 1, 0  第一个
     * 1, 1  正序
     */
    // 用于排序，-1或0：倒序，1：正序    但是要注意先排序，后分组
    @Override
    public int compareTo(Object o) {
        Hadoop09KeyBean hb= (Hadoop09KeyBean) o;
        int i = this.oid.compareTo(hb.oid); //先按oid正序排
        if(i==0){                           //如果oid相同
            i = -this.price.compareTo(hb.price);        //则按照价格倒序排
        }
        return i;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(oid);
        dataOutput.writeFloat(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        oid = dataInput.readUTF();
        price = dataInput.readFloat();
    }


    private String oid;
    private Float price;

    public Hadoop09KeyBean() {
    }



    public Hadoop09KeyBean(String oid, Float price) {
        this.oid = oid;
        this.price = price;
    }

    @Override
    public String toString() {
        return "Hadoop09KeyBean{" +
                "oid='" + oid + '\'' +
                ", price=" + price +
                '}';
    }

    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public Float getPrice() {
        return price;
    }

    public void setPrice(Float dj) {
        this.price = dj;
    }

}

class Hadoop09GroupingCompatator extends WritableComparator{
    public Hadoop09GroupingCompatator() {
        super(Hadoop09KeyBean.class, true);
    }

    //用于分组
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Hadoop09KeyBean h1= (Hadoop09KeyBean) a;
        Hadoop09KeyBean h2= (Hadoop09KeyBean) b;
        return h1.getOid().compareTo(h2.getOid());  //按oid来分组
//        return 0;
//        return super.compare(a, b);
    }
}