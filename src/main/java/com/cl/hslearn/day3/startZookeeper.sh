#!/bin/bash
#自定启动集群：
#     原理：通过ssh去执行命令，但是要带环境变量，写到shell脚本里
#         export 定义的变量，会对自己所在的shell进程及其子进程生效
#         b=1 定义的变量，只对自己所在的shell进程生效
#         在script.sh中定义的变量，在当前登录的shell进程 source     script.sh时，脚本中定义的变量也会进入当前登录的进程

for i in 1 2 3 4 5
do
echo "us"$i":"
ssh us$i "/home/hadoop/apps/zookeeper-3.4.11/bin/zkServer.sh start"
#ssh us$i "source /etc/profile;/home/hadoop/apps/zookeeper-3.4.11/bin/zkServer.sh start"  #如果通过ssh登录时没有加载所需环境变量，需要在命令里生效所需环境变量
done
