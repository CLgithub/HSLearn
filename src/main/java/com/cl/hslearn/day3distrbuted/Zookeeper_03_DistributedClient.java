package com.cl.hslearn.day3distrbuted;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by L on 18/2/10.
 */
public class Zookeeper_03_DistributedClient {
    private static ZooKeeper zooKeeper;
    private static final String connectString="us1:2181,us2.2181,us3.2181";  //一个一个尝试连接
    private static final int sessionTimeout=2000;   //会话超时时间
    private static final String parentNode ="/servers";
    //加volatile的意义：直接在堆内存中修改
    private static  volatile List<String> serverList;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        // 获取zk连接，并注册监听器，当监听到孩子改变事件后，获取子节点列表，并且再次监听子节点改变事件
        getConnect();
        //业务线程启动，确保程序活着
        handleBusiness();
    }

    /**
     * 获取zk连接，并注册监听器，当监听到孩子改变事件后，获取子节点列表，并且再次监听子节点改变事件
     * @return
     */
    public static void getConnect() throws IOException {
        zooKeeper=new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                try {
                    getServerList();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 获取子节点信息
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void getServerList() throws KeeperException, InterruptedException {
        //获取子节点列表，并且再次监听父节点
        List<String> list= zooKeeper.getChildren(parentNode, true);
        List<String> servers=new ArrayList<String>();
        for(String child:list){
            byte[] data = zooKeeper.getData(parentNode + "/" + child, false, null);
            String s = new String(data);
            servers.add(s);
        }
        serverList=servers;
        System.out.println(serverList);
    }

    /**
     * 业务功能
     */
    private static void handleBusiness() throws InterruptedException {
        System.out.println("client start working...");
        Thread.sleep(Long.MAX_VALUE);
    }
}
