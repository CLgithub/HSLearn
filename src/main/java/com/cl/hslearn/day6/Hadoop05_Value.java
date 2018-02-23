package com.cl.hslearn.day6;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Hadoop05_Value implements Writable {
    private long upNumber;
    private long downNumber;
    private long sumNumber;

    public Hadoop05_Value(long upNumber, long downNumber) {
        this.upNumber = upNumber;
        this.downNumber = downNumber;
        this.sumNumber = upNumber+downNumber;
    }

    public Hadoop05_Value() {
    }

    @Override
    public String toString() {
        return "Hadoop05_Value{" +
                "upNumber=" + upNumber +
                ", downNumber=" + downNumber +
                ", sumNumber=" + sumNumber +
                '}';
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
}
