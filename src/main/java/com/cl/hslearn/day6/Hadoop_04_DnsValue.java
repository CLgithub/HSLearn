package com.cl.hslearn.day6;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Hadoop_04_DnsValue implements Writable{
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(sum);
        dataOutput.write((domain+System.lineSeparator()).getBytes());
        dataOutput.write((timeStr+System.lineSeparator()).getBytes());
        dataOutput.write((sip+System.lineSeparator()).getBytes());

    }

    public void readFields(DataInput dataInput) throws IOException {
        sum=dataInput.readInt();
        domain=dataInput.readLine();
        timeStr=dataInput.readLine();
        sip=dataInput.readLine();
    }

    private int sum;
    private String domain;
    private String timeStr;
    private String sip;

    @Override
    public String toString() {
        return "Hadoop_04_DnsValue{" +
                "sum=" + sum +
                ", domain='" + domain + '\'' +
                ", timeStr='" + timeStr + '\'' +
                ", sip='" + sip + '\'' +
                '}';
    }


    public Hadoop_04_DnsValue(int sum, String domain, String timeStr, String sip) {
        this.sum = sum;
        this.domain = domain;
        this.timeStr = timeStr;
        this.sip = sip;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getTimeStr() {
        return timeStr;
    }

    public void setTimeStr(String timeStr) {
        this.timeStr = timeStr;
    }

    public String getSip() {
        return sip;
    }

    public void setSip(String sip) {
        this.sip = sip;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }


    public Hadoop_04_DnsValue() {
    }

}
