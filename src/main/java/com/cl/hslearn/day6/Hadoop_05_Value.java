package com.cl.hslearn.day6;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Hadoop_05_Value extends BinaryComparable implements WritableComparable<BinaryComparable> {
    private long upNumber;
    private long downNumber;
    private long sumNumber;

    public Hadoop_05_Value(long upNumber, long downNumber) {
        this.upNumber = upNumber;
        this.downNumber = downNumber;
        this.sumNumber = upNumber+downNumber;
    }

    public Hadoop_05_Value() {
    }

    @Override
    public int getLength() {
        return this.getLength();
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }

    @Override
    public String toString() {
        return upNumber+"\t"+downNumber+"\t"+sumNumber;
//        return "Hadoop05_Value{" +
//                "upNumber=" + upNumber +
//                ", downNumber=" + downNumber +
//                ", sumNumber=" + sumNumber +
//                '}';
    }

    public long getUpNumber() {
        return upNumber;
    }

    public void setUpNumber(long upNumber) {
        this.upNumber = upNumber;
    }

    public long getDownNumber() {
        return downNumber;
    }

    public void setDownNumber(long downNumber) {
        this.downNumber = downNumber;
    }

    public long getSumNumber() {
        return upNumber+downNumber;
    }


    public void set(long upNumber, long downNumber) {
        this.upNumber = upNumber;
        this.downNumber = downNumber;
        this.sumNumber = upNumber+downNumber;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upNumber);
        dataOutput.writeLong(downNumber);
        dataOutput.writeLong(sumNumber);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upNumber=dataInput.readLong();
        downNumber=dataInput.readLong();
        sumNumber=dataInput.readLong();
    }

//    @Override
//    public int compareTo(Hadoop_05_Value o) {
//        return (this.sumNumber-o.getSumNumber())>0?1:-1;
////        Long l = this.sumNumber - o.getSumNumber();
////        return l.intValue();
//    }


    @Override
    public int compareTo(BinaryComparable other) {
        Hadoop_05_Value o= (Hadoop_05_Value) other;
        return (this.sumNumber-o.getSumNumber())<0?1:-1;
    }
}
