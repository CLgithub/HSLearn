package com.cl.hslearn.day3;

/**
zookeeper 是一个分布式协调服务，就说为用户分布式应用程序提供协调服务
zookeeper集群搭建：
    0.linux网卡设置：
        1。静态ip，修改/etc/network/interfaces,添加：
        iface eth0 inet static  //static为静态的
        address 192.168.3.101   //ip地址
        netmask 255.255.255.0   //子网掩码
        gateway 192.168.3.1     //网关
        broadcast 192.168.3.255 //广播
        2。配置dns，修改/etc/resolv.conf,添加：
        nameserver xxx.xxx.xxx.xxx
        3。重启网卡
        /etc/init.d/networking restart
    1.多台机器，配置好java，放好包
    2.修改conf目录下zoo.cfg文件
        # The number of milliseconds of each tick
        tickTime=2000
        # The number of ticks that the initial
        # synchronization phase can take
        initLimit=10
        # The number of ticks that can pass between
        # sending a request and getting an acknowledgement
        syncLimit=5
        # the directory where the snapshot is stored.
        # do not use /tmp for storage, /tmp here is just
        # example sakes.
        #dataDir=/tmp/zookeeper
        dataDir=/home/hadoop/zkdata
        # the port at which the clients will connect
        clientPort=2181
        # the maximum number of client connections.
        # increase this if you need to handle more clients
        #maxClientCnxns=60
        #
        # Be sure to read the maintenance section of the
        # administrator guide before turning on autopurge.
        #
        # http://zookeeper.apache.org/doc/current/zookeeperAdmin.html#sc_maintenance
        #
        # The number of snapshots to retain in dataDir
        #autopurge.snapRetainCount=3
        # Purge task interval in hours
        # Set to "0" to disable auto purge feature
        #autopurge.purgeInterval=1
        server.1=us1:2888:3888
        server.2=us2:2888:3888
        server.3=us3:2888:3888
        server.4=us4:2888:3888
        server.5=us5:2888:3888
    3.启动
        ./zookeeper-3.4.11/bin/zkServer.sh start	启动
        ./zookeeper-3.4.11/bin/zkServer.sh status	查看状态
        ./zookeeper-3.4.11/bin/zkServer.sh stop		停止

    当集群里有多台机器时，必须有超过半数的机器还或者才能正常，因此，不会有一个集群裂变成两个集群的现象

zookeeper命令行客户端使用：
    1.执行命令./zkCli.sh，变能连接到并机zookeeper服务，之后可以通过connect us2:2181连接到其他机器
    2.help查看帮助，create /app1 "this is app1 severs parent" 在跟目录下创建app1节点，并且赋予数据“this is ...”
    3.znode是树型结构
	有两种类型：
		1.持久(persistent)(断开连接不删除)
		2.短暂(ephemeral)(断开连接自己删除)
	四种形式的目录节点（默认是persistent）
		PERSISTENT	create /app1 "data"
		PERSISTENT_SEQUENTIAL  (持久_序列) create -s /app1 "abc"
		EPHEMERAL	create -e /app1 "data"
		EPHEMERAL_SEQUENTIAL  create -s -e /app1 "abc"
    4.get /app1 可以获取app1节点的数据，get /app1 watch可以监听app1节点数据的变化，同理ls /app1 watch 可以监听app1子节点的变化

 */

public class zookerper_01 {
}
