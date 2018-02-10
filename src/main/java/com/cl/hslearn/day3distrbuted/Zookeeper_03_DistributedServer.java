package com.cl.hslearn.day3distrbuted;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * Created by L on 18/2/10.
 * zookeeper 应用练习一：
 * 设计一个程序，能自动感知集群中多台服务器的上下线情况
 *      思路：服务器上线时就在zookeeper中创建临时节点并注册监听器，一台服务器对应一个节点，客户端去监听事件
 */
public class Zookeeper_03_DistributedServer {
    private static ZooKeeper zooKeeper;
    private static final String connectString="us1:2181,us2.2181,us3.2181";  //一个一个尝试连接
    private static final int sessionTimeout=2000;   //会话超时时间
    private static final String parentNode ="/servers";

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        //获取zk连接，并注册监听器
        getConnect();
        //利用zk连接注册服务器信息
        registerServer("u3");
        //启动业务功能
        handleBusiness("u3");
    }

    /**
     * 业务功能
     */
    private static void handleBusiness(String hostName) throws InterruptedException {
        System.out.println(hostName+" start working...");
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 向zk集群注册服务器信息
     * @param hostname
     * @throws KeeperException
     * @throws InterruptedException
     */
    private static void registerServer(String hostname) throws KeeperException, InterruptedException {
        String s = zooKeeper.create(parentNode + "/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname+"上线。。。"+s);
    }

    /**
     * 获取zk连接
     * @return
     */
    public static void getConnect() throws IOException {
        zooKeeper=new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
//                System.out.println("监听到："+watchedEvent);
//                try {
//                    zooKeeper.getChildren(parentNode,true);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
            }
        });
    }

}
