package com.cl.hslearn.day3;

import com.sun.org.apache.xml.internal.security.Init;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 zookeeper java API学习

 */
public class Zookeeper_02 {

    private static final String connectString="us1:2181,us2.2181,us3.2181";  //一个一个尝试连接
    private static final int sessionTimeout=2000;   //会话超时时间

    public static ZooKeeper connectZK() throws IOException {
        //监听器
        Watcher watcher = new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("watcher："+watchedEvent);
                //可以在process方法内部再次注册监听，就会持续监听
//                try {
//                    zooKeeper.getChildren("/",true);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
            }
        };
        zooKeeper=new ZooKeeper(connectString, sessionTimeout, watcher);
        return  zooKeeper;
    }

    public static ZooKeeper zooKeeper=null;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        zooKeeper=connectZK();
//        testGet(zooKeeper,"/");
        testCreate(zooKeeper,"/aa","testdata");

    }

    // 查
    public static void testGet(ZooKeeper zooKeeper,String path) throws KeeperException, InterruptedException {
        //查
        // 节点
        List<String> children = zooKeeper.getChildren(path, Boolean.TRUE);
        System.out.println(children);
        // 数据
        byte[] data = zooKeeper.getData(path, Boolean.TRUE, new Stat());
        String s = new String(data);
        System.out.println(s);
    }

    // 增
    public static void testCreate(ZooKeeper zooKeeper, String path, String data) throws KeeperException, InterruptedException {
        //增
        // 永久   参数：节点路径，节点数据，节点权限，节点类型
//        String create1 = zooKeeper.create("/javaApiApp2", "this is javaAPIData".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        System.out.println(create1);
//        String create2 = zooKeeper.create("/javaApiApp2/ps1", "this is javaAPIData/javaApiApp2/ps1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
//        System.out.println(create2);
        // 临时
        String create = zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println(create);
        String create2 = zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(create2);
        List<String> children1 = zooKeeper.getChildren(path, Boolean.FALSE);
        System.out.println(children1);
    }
    // 删
    public static void testDel(ZooKeeper zooKeeper,String path){
//        zooKeeper.delete(path,);
    }
    // 改
    public static void testSet(){
    }


}
