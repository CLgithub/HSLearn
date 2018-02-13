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

 hadoop shell客户端：
    hadoop操作文件系统
        hadoop fs -put fileName     #上传文件到hadoop的文件系统（hdfs）
                  -get
        hadoop fs -ls /             #查看hdfs根目录路径文件
                  -cat
                  -mkdir /dirName
        文件其实是存放在hdpdata/dfs/data/current/BP-1184836542-192.168.1.31-1518342045963/current/finalized/subdir0/subdir0下
        每台机器上都有，当文件大于块（128m）时，会分为几块存储

    HDFS常用命令:
        ls
        mkdir
        moveFromLocal   从本地移动到hdfs
        moveToLocal     从hdfs移动到本地
        appendToFile    追加一个文件到已经存在的文件末尾
        cat
        tail
        text            以字符形式打印一个文件的内容
        chgrp chmod chown 与linux文件系统中的用法一样
        copyFromLocal或put   从本地文件系统中拷贝文件到sdfs
        copyToLocal或get     从hdfs拷贝到本地
        cp mv rm rmdir df
        du                  统计文件夹的大小信息
        getmerge            合并下载多个文件
        count               统计一个指定目录下的文件节点数量
        setrep              设置hdfs中文件的副本数量（指记录在元数据中，是非确实是这么多数量，要看namenode的数量）



 mapreduce 测试：
    到hadoop-2.7.5/share/hadoop/mapreduce下，执行
        hadoop jar hadoop-mapreduce-examples-2.7.5.jar wordcount /dir1/ /dir1out
    统计/dir1/文件夹下各单词的数量输出到/dir1o/下

 */
public class Hadoop_01 {
}
