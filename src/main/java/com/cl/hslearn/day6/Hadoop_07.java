package com.cl.hslearn.day6;

import com.sun.xml.internal.xsom.impl.scd.Iterators;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 查找两两共同好友：
 A:B,C,D,F,E,O
 B:A,C,E,K
 C:F,A,D,I
 E:B,C,D,M,L
 F:A,B,C,D,E,O,M
 G:A,C,D,E,F
 H:A,C,D,E,O
 I:A,O
 J:B,O
 K:A,C,D
 L:D,E,F
 M:E,F,G
 O:A,H,I,J

 比如：
    A-B:C,E
  以后再写
 */
public class Hadoop_07 {

}

