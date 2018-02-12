package com.cl.hslearn.day6;

/**
 * Created by L on 18/2/11.

 hadoop是什么
    hadoop是apache旗下的一套开源软件平台
    hadoop提供的功能，利用服务器集群，根据用户的自定义业务逻辑，对海量数据进行分布式处理
    hadoop的核心组件：
        1.HDFS（分布式文件系统）
            namenode管理datanode，datanode用于存放数据
                会另带起一个secondaryNameNode(次要的nameNode)
        2.YARN(运算资源调度系统)
            resourceManager管理nodeManager，nodeManager用来管理运算单元
        3.MAPREDUCE(分布式运算编程框架)
            此部分可以用spark或者flink等高性能计算框架代替



 安装hadoop
    0.官网下载，解压
    1.配置
        进入etc
            1）配置hadoop-env.sh，配置hadoop环境
                配置JAVA_HOME
            2）core-site.xml hadoop核心配置
                配置默认文件系统
                <property>
                    <name>fs.defaultFS</name>
                    <value>hdfs://us1:9000</value>
                </property>
                配置hadoop临时数据目录
                <property>
                    <name>hadoop.tmp.dir</name>
                    <value>/home/hadoop/hdpdata</value>
                </property>
            3）hdfs-site.xml
                配置数据副本的数量
                <property>
                    <name>dfs.replication</name>
                    <value>2</value>
                </property>
            4）mapred-site.xml
                配置mapreduce运行位置
                <property>
                    <name>mapreduce.framework.name</name>
                    <value>yarn</value>
                </property>
            5）yarn-env.sh
                配置yarn的管理机
                <property>
                    <name>yarn.resourcemanager.hostname</name>
                    <value>us1</value>
                </property>

                <property>
                    <name>yarn.nodemanager.aux-services</name>
                    <value>mapreduce_shuffle</value>
                </property>
        其实上面的信息可以配置到一个文件内，name 可以在官方文件上查找
    2.分发，配置环境变量，启动，第一次启动前要先格式化hdfs文件系统
        格式化hdfs：hadoop namenode -format
        单独启动：
            启动主服务器上的namenode：hadoop-daemon.sh start namenode
                启动后便可以通过http://us1:50070/ 访问hdfs的namenode和datanode的节点情况
            启动datanode：hadoop-daemon.sh start datanode
        集群启动：
            需要先配置slaves：添加要启动datanode的主机名
            启动集群datanode：start-dfs.sh







 */
public class Hadoop_01 {
}
