package com.cl.hslearn.day11;

/**
 高可用集群hive集群
    由于需要高可用，所以将各个模块放入集群，实现高可用
 5台机器
 us1:   nameNode    resourceManager     zkfc(状态切换,DFSZKFailoverController)
 us2:   dataNode    nodeManager     QuorumPeerMain(zookeeper)- - -journalNode(管理日志)
 us3:   dataNode    nodeManager     QuorumPeerMain(zookeeper)- - -journalNode(管理日志)
 us4:   dataNode    nodeManager     QuorumPeerMain(zookeeper)- - -journalNode(管理日志)
 us5:   nameNode    resourceManager     zkfc(状态切换,DFSZKFailoverController)

 hadoop2.0已经发布了稳定版本了，增加了很多特性，比如HDFS HA、YARN等。最新的hadoop-2.7.5又增加了YARN HA

 注意：apache提供的hadoop-2.7.5的安装包是在32位操作系统编译的，因为hadoop依赖一些C++的本地库，
 所以如果在64位的操作上安装hadoop-2.7.5就需要重新在64操作系统上重新编译
 （建议第一次安装用32位的系统，我将编译好的64位的也上传到群共享里了，如果有兴趣的可以自己编译一下）

 前期准备就不详细说了，课堂上都介绍了
 1.修改Linux主机名
 2.修改IP
 3.修改主机名和IP的映射关系 /etc/hosts
 ######注意######如果你们公司是租用的服务器或是使用的云主机（如华为用主机、阿里云主机等）
 /etc/hosts里面要配置的是内网IP地址和主机名的映射关系
 4.关闭防火墙
 5.ssh免登陆
 6.安装JDK，配置环境变量等

 集群规划：
 主机名		IP				安装的软件					运行的进程
 us1		192.168.3.31	jdk、hadoop					ResourceManager NameNode、DFSZKFailoverController(zkfc)
 us5		192.168.3.35	jdk、hadoop					ResourceManager NameNode、DFSZKFailoverController(zkfc)
 us2		192.168.3.32	jdk、hadoop、zookeeper		DataNode、NodeManager、JournalNode、QuorumPeerMain
 us3		192.168.3.33	jdk、hadoop、zookeeper		DataNode、NodeManager、JournalNode、QuorumPeerMain
 us4		192.168.3.34	jdk、hadoop、zookeeper		DataNode、NodeManager、JournalNode、QuorumPeerMain

 说明：
 1.在hadoop2.0中通常由两个NameNode组成，一个处于active状态，另一个处于standby状态。Active NameNode对外提供服务，而Standby NameNode则不对外提供服务，仅同步active namenode的状态，以便能够在它失败时快速进行切换。
 hadoop2.0官方提供了两种HDFS HA的解决方案，一种是NFS，另一种是QJM。这里我们使用简单的QJM。在该方案中，主备NameNode之间通过一组JournalNode同步元数据信息，一条数据只要成功写入多数JournalNode即认为写入成功。通常配置奇数个JournalNode
 这里还配置了一个zookeeper集群，用于ZKFC（DFSZKFailoverController）故障转移，当Active NameNode挂掉了，会自动切换Standby NameNode为standby状态
 2.hadoop-2.2.0中依然存在一个问题，就是ResourceManager只有一个，存在单点故障，hadoop-2.7.5解决了这个问题，有两个ResourceManager，一个是Active，一个是Standby，状态由zookeeper进行协调
 安装步骤：
 1.安装配置zooekeeper集群（在hadoop05上）
 1.1解压
 tar -zxvf zookeeper-3.4.11.tar.gz -C /home/hadoop/apps/
 1.2修改配置
 cd /home/hadoop/apps/zookeeper-3.4.11/conf/
 cp zoo_sample.cfg zoo.cfg
 vim zoo.cfg
 修改：dataDir=/home/hadoop/appdata/zkdata
 在最后添加：
 server.2=us2:2888:3888
 server.3=us3:2888:3888
 server.4=us4:2888:3888
 保存退出
 然后创建一个tmp文件夹
 mkdir /home/hadoop/appdata/zkdata
 echo 1 > /home/hadoop/appdata/zkdata/myid
 1.3将配置好的zookeeper拷贝到其他节点(首先分别在us3、us4根目录下创建一个hadoop目录：mkdir /hadoop)
 scp -r /home/hadoop/apps/zookeeper-3.4.11/ us3:/home/hadoop/apps/
 scp -r /home/hadoop/apps/zookeeper-3.4.11/ us4:/home/hadoop/apps/

 注意：修改us3、us4对应/home/hadoop/appdata/zkdata/myid内容
 us3：
 echo 2 > /home/hadoop/appdata/zkdata/myid
 us4：
 echo 3 > /home/hadoop/appdata/zkdata/myid

 2.安装配置hadoop集群（在us1上操作）
 2.1解压
 tar -zxvf hadoop-2.7.5.tar.gz -C /home/hadoop/apps/
 2.2配置HDFS（hadoop2.0所有的配置文件都在$HADOOP_HOME/etc/hadoop目录下）
 #将hadoop添加到环境变量中
 vim /etc/profile 或 /home/hadoop/.bashrc
 export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
 export HADOOP_HOME=/home/hadoop/apps/hadoop-2.7.5
 export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

 #hadoop2.0的配置文件全部在$HADOOP_HOME/etc/hadoop下
 cd /home/hadoop/apps/hadoop-2.7.5/etc/hadoop

 2.2.1修改hadoo-env.sh
 export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
 ###############################################################################

 2.2.2修改core-site.xml
 <configuration>
 <!-- 配置默认文件系统 -->
 <property>
 <name>fs.defaultFS</name>
 <value>hdfs://bi</value>
 </property>
 <!-- 配置hadoop临时数据目录 -->
 <property>
 <name>hadoop.tmp.dir</name>
 <value>/home/hadoop/appdata/hdpdata</value>
 </property>
 <!-- 指定zookeeper地址 -->
 <property>
 <name>ha.zookeeper.quorum</name>
 <value>us2:2181,us3:2181,us4:2181</value>
 </property>
 </configuration>
 ###############################################################################
 2.2.3修改hdfs-site.xml
 <configuration>
 <!-- 配置数据副本的数量-->
 <!--
 <property>
 <name>dfs.replication</name>
 <value>2</value>
 </property>
 <property>
 <name>dfs.name.dir</name>
 <value>/home/hadoop/name1,/home/hadoop/name2</value>
 </property>
 <property>
 <name>dfs.data.dir</name>
 <value>/home/hadoop/data1,/home/hadoop/data2</value>
 </property>
 -->
 <!--指定hdfs的nameservice为bi，需要和core-site.xml中的保持一致 -->
 <property>
 <name>dfs.nameservices</name>
 <value>bi</value>
 </property>
 <!-- bi下面有两个NameNode，分别是nn1，nn2 -->
 <property>
 <name>dfs.ha.namenodes.bi</name>
 <value>namenode1,namenode2</value>
 </property>
 <!-- namenoden1的RPC通信地址 -->
 <property>
 <name>dfs.namenode.rpc-address.bi.namenode1</name>
 <value>us1:9000</value>
 </property>
 <!-- namenoden1的http通信地址 -->
 <property>
 <name>dfs.namenode.http-address.bi.namenode1</name>
 <value>us1:50070</value>
 </property>
 <!-- namenoden2的RPC通信地址 -->
 <property>
 <name>dfs.namenode.rpc-address.bi.namenode2</name>
 <value>us5:9000</value>
 </property>
 <!-- namenoden2的http通信地址 -->
 <property>
 <name>dfs.namenode.http-address.bi.namenode2</name>
 <value>us5:50070</value>
 </property>
 <!-- 指定NameNode的edits元数据在JournalNode上的存放位置 -->
 <property>
 <name>dfs.namenode.shared.edits.dir</name>
 <value>qjournal://us2:8485;us3:8485;us4:8485/bi</value>
 </property>
 <!-- 指定JournalNode在本地磁盘存放数据的位置 -->
 <property>
 <name>dfs.journalnode.edits.dir</name>
 <value>/home/hadoop/appdata/journaldata</value>
 </property>
 <!-- 开启NameNode失败自动切换 -->
 <property>
 <name>dfs.ha.automatic-failover.enabled</name>
 <value>true</value>
 </property>
 <!-- 配置失败自动切换实现方式 -->
 <property>
 <name>dfs.client.failover.proxy.provider.bi</name>
 <value>org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider</value>
 </property>
 <!-- 配置隔离机制方法，多个机制用换行分割，即每个机制暂用一行-->
 <property>
 <name>dfs.ha.fencing.methods</name>
 <value>
 sshfence
 shell(/bin/true)
 </value>
 </property>
 <!-- 使用sshfence隔离机制时需要ssh免登陆 -->
 <property>
 <name>dfs.ha.fencing.ssh.private-key-files</name>
 <value>/home/hadoop/.ssh/id_rsa</value>
 </property>
 <!-- 配置sshfence隔离机制超时时间 -->
 <property>
 <name>dfs.ha.fencing.ssh.connect-timeout</name>
 <value>30000</value>
 </property>
 </configuration>
 ###############################################################################

 2.2.4修改mapred-site.xml
 <configuration>
 <!--配置mapreduce运行位置-->
 <property>
 <name>mapreduce.framework.name</name>
 <value>yarn</value>
 </property>
 </configuration>

 ###############################################################################

 2.2.5修改yarn-site.xml
 <configuration>

 <!-- Site specific YARN configuration properties -->
 <!--配置yarn的管理机
 <property>
 <name>yarn.resourcemanager.hostname</name>
 <value>us1</value>
 </property>
 <property>
 <name>yarn.nodemanager.aux-services</name>
 <value>mapreduce_shuffle</value>
 </property>

 <property>
 <name>yarn.scheduler.fair.preemption.cluster-utilization-threshold</name>
 <value>1.0</value>
 </property>
 <property>
 <name>yarn.resourcemanager.address</name>
 <value>us1:8032</value>
 </property>
 <property>
 <name>yarn.resourcemanager.scheduler.address</name>
 <value>us1:8030</value>
 </property>
 <property>
 <name>yarn.resourcemanager.resource-tracker.address</name>
 <value>us1:8031</value>
 </property>
 -->
 <!-- 开启RM高可用 -->
 <property>
 <name>yarn.resourcemanager.ha.enabled</name>
 <value>true</value>
 </property>
 <!-- 指定RM的cluster id -->
 <property>
 <name>yarn.resourcemanager.cluster-id</name>
 <value>yrc</value>
 </property>
 <!-- 指定rouceManagerM的名字 -->
 <property>
 <name>yarn.resourcemanager.ha.rm-ids</name>
 <value>rm1,rm2</value>
 </property>
 <!-- 分别指定RM的地址 -->
 <property>
 <name>yarn.resourcemanager.hostname.rm1</name>
 <value>us1</value>
 </property>
 <property>
 <name>yarn.resourcemanager.hostname.rm2</name>
 <value>us5</value>
 </property>
 <!-- 指定zk集群地址 -->
 <property>
 <name>yarn.resourcemanager.zk-address</name>
 <value>us2:2181,us3:2181,us4:2181</value>
 </property>
 <property>
 <name>yarn.nodemanager.aux-services</name>
 <value>mapreduce_shuffle</value>
 </property>
 </configuration>


 2.2.6修改slaves(slaves是指定子节点的位置，因为要在us1,us5上启动HDFS、在us1,us5启动yarn，所以us1,us5上的slaves文件指定的是datanode的位置，us1、us5上的slaves文件指定的是nodemanager的位置)
 us2
 us3
 us4

 2.2.7配置免密码登陆
 #首先要配置us1到us2、us3、us4、us5的免密码登陆
 #在us1上生产一对钥匙
 ssh-keygen -t rsa
 #将公钥拷贝到其他节点，包括自己
 ssh-coyp-id us1
 ssh-coyp-id us2
 ssh-coyp-id us3
 ssh-coyp-id us4
 ssh-coyp-id us5
 #配置us2到us2、us3、us4的免密码登陆
 #在us2上生产一对钥匙
 ssh-keygen -t rsa
 #将公钥拷贝到其他节点
 ssh-coyp-id us2
 ssh-coyp-id us3
 ssh-coyp-id us4
 #注意：两个namenode之间要配置ssh免密码登陆，别忘了配置us5到us1的免登陆
 在us5上生产一对钥匙
 ssh-keygen -t rsa
 ssh-coyp-id -i us1

 2.4将配置好的hadoop拷贝到其他节点

 ###注意：严格按照下面的步骤!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 2.5启动zookeeper集群（分别在us2、us3、us4上启动zk）
 cd /hadoop/zookeeper-3.4.11/bin/
 ./zkServer.sh start
 #查看状态：一个leader，两个follower
 ./zkServer.sh status

 2.6启动journalnode（分别在在us2、us3、us4上执行）
 cd /hadoop/hadoop-2.7.5
 sbin/hadoop-daemon.sh start journalnode
 #运行jps命令检验，us2、us3、us4上多了JournalNode进程

 2.7格式化HDFS
 #在us1上执行命令:
 hdfs namenode -format
 #格式化后会在根据core-site.xml中的hadoop.tmp.dir配置生成个文件，将这个文件夹拷贝到us5

 2.8格式化ZKFC(在us1上执行一次即可)
 hdfs zkfc -formatZK

 2.9启动HDFS(在us1上执行)
 sbin/start-dfs.sh

 2.10启动YARN(#####注意#####：是在hadoop02上执行start-yarn.sh，把namenode和resourcemanager分开是因为性能问题，因为他们都要占用大量资源，所以把他们分开了，他们分开了就要分别在不同的机器上启动)
 sbin/start-yarn.sh


 到此，hadoop-2.7.5配置完毕，可以统计浏览器访问:
 http://us1:50070
 NameNode 'us1:9000' (active)
 http://hadoop01:50070
 NameNode 'us5:9000' (standby)

 验证HDFS HA
 首先向hdfs上传一个文件
 hadoop fs -put /etc/profile /profile
 hadoop fs -ls /
 然后再kill掉active的NameNode
 kill -9 <pid of NN>
 通过浏览器访问：http://192.168.1.202:50070
 NameNode 'hadoop02:9000' (active)
 这个时候hadoop02上的NameNode变成了active
 在执行命令：
 hadoop fs -ls /
 -rw-r--r--   3 root supergroup       1926 2014-02-06 15:36 /profile
 刚才上传的文件依然存在！！！
 手动启动那个挂掉的NameNode
 sbin/hadoop-daemon.sh start namenode
 通过浏览器访问：http://192.168.1.201:50070
 NameNode 'hadoop01:9000' (standby)

 验证YARN：
 运行一下hadoop提供的demo中的WordCount程序：
 hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.4.1.jar wordcount /profile /out

 OK，大功告成！！！




 测试集群工作状态的一些指令 ：
 bin/hdfs dfsadmin -report	 查看hdfs的各节点状态信息

 bin/hdfs haadmin -getServiceState namenode1		 获取一个namenode节点的HA状态

 sbin/hadoop-daemon.sh start namenode  单独启动一个namenode进程

 ./hadoop-daemon.sh start zkfc   单独启动一个zkfc进程


 */
public class HA {
}
