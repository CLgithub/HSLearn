package com.cl.hslearn.day6;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.util.Iterator;
import java.util.Map;
import java.util.Observable;

/**
 * Created by L on 18/2/13.
 */
public class Hadoop_02 {
    static FileSystem fs;
    static {
        Configuration conf= new Configuration();
        try {
            fs= FileSystem.get(new URI("hdfs://us1:9000"), conf, "hadoop");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public static void main(String[] args) throws Exception {
        hdfsClientDemo1();
//        hdfsClientDemo2();
//        hdfsStreamAcces();
//        hdfsStreamAccesDown();
    }


    public static void hdfsClientDemo1() throws Exception{
        Configuration conf= new Configuration();
//        conf.set("fs.defaultFS","hdfs://us1:9000");       //设置后会自动变成hdfs文件系统，否则是本地文件系统
//        FileSystem fs=FileSystem.get(conf);
        conf.set("dfs.replication","5");    //配置文件加载顺序，会先加载jar包下的，然后加载classpath下的，最后加载代码里的，所以最终是5
        FileSystem fs= FileSystem.get(new URI("hdfs://us1:9000"), conf, "hadoop");
        //增
//        fs.copyFromLocalFile(new Path("/Users/L/Downloads/t1"),new Path("/javaAPI/upload/t3"));
        //下载
//        fs.copyToLocalFile(new Path("/Users/L/Downloads/t2"),new Path("/Users/L/Downloads/t2_down"));
        //删除
//        fs.delete(new Path("/Users"),true);  //第二个参数代表是否递归删除
        //创建文件夹mkdir
//        boolean b= fs.mkdirs(new Path("/javaAPI/mkdir1/aaa/bbb"));
//        System.out.println(b);

        //查询
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()){
            LocatedFileStatus locatedFileStatus= listFiles.next();  //得到每个文件的状态
//            System.out.println(locatedFileStatus);    //文件状态
            System.out.println("文件名："+locatedFileStatus.getPath());
            BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();//获取文件的偏移量
            for(BlockLocation blockLocation:blockLocations){
                System.out.println("块起始偏移量："+blockLocation.getOffset());
                System.out.println("块长度："+blockLocation.getLength());
                //块所在的datanode节点
                for(String datanode: blockLocation.getHosts()){
                    System.out.println("datenode:"+datanode);
                }
            }
            System.out.println();
        }
        //listStatus
//        FileStatus[] fileStatuses = fs.listStatus(new Path("/javaAPI"));
//        for(FileStatus fileStatus:fileStatuses){
////            System.out.println(fileStatus);
//            System.out.print("name:"+fileStatus.getPath().getName());
//            System.out.println(fileStatus.isFile()?" file":" directory");
//        }

        fs.close();
    }




    //流上传
    public static void hdfsStreamAcces() throws Exception{
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/javaAPI/streamtest/text2"), true);
        FileInputStream fileInputStream = new FileInputStream(new File("/Users/L/Downloads/text2"));
//        int i=0;
//        byte[] buf=new byte[1024];
//        while((i=fileInputStream.read(buf))!=-1){
//            fsDataOutputStream.write(buf,0,i);
//        }
        long copy = IOUtils.copy(fileInputStream, fsDataOutputStream);
        System.out.print(copy);
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
        fileInputStream.close();
    }

    //流下载
    public static void hdfsStreamAccesDown() throws Exception{
        FSDataInputStream fsDataInputStream = fs.open(new Path("/javaAPI/streamtest/text2"));
//        FileOutputStream fileOutputStream = new FileOutputStream(new File("/Users/L/Downloads/text2_hdfs2"));
        OutputStream fileOutputStream=System.out;       //拷贝到系统输出流
        fsDataInputStream.seek(3);      //设置inputstream从第3个字节开始读取
        long copy = IOUtils.copy(fsDataInputStream,fileOutputStream);
        System.out.print(copy);
        fileOutputStream.flush();
        fileOutputStream.close();
        fsDataInputStream.close();
    }


    //迭代查看conf
    public static void hdfsClientDemo2() throws Exception{
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","hdfs://us1:9000");
        Iterator<Map.Entry<String, String>> iterator = conf.iterator();
        while (iterator.hasNext()){
            Map.Entry<String, String> map= iterator.next();
            System.out.println(map);
        }
    }

}
