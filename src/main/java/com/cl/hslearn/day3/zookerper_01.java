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
 */
public class zookerper_01 {
}
