package com.cl.hslearn.day6;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Hadoop_04_DnsKey extends BinaryComparable implements WritableComparable<BinaryComparable> {
//    public static void main(String[] args){
//        Hadoopp_04_DnsKey b=new Hadoopp_04_DnsKey("1","","");
//        Hadoopp_04_DnsKey a=new Hadoopp_04_DnsKey("1","","");
//        System.out.println(b.equals(a));
//    }
    private String domain;
    private String sip;
    private String timeStr;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Hadoop_04_DnsKey that = (Hadoop_04_DnsKey) o;

        if (domain.equals(that.domain)&&sip.equals(that.sip)&&timeStr.equals(that.timeStr))
            return true;
        else
            return false;


//        if (domain != null ? !domain.equals(that.domain) : that.domain != null) return false;
//        if (sip != null ? !sip.equals(that.sip) : that.sip != null) return false;
//        return timeStr != null ? timeStr.equals(that.timeStr) : that.timeStr == null;
    }

    @Override
    public int compareTo(BinaryComparable other) {
        Hadoop_04_DnsKey hadoop_04_dnsKey= (Hadoop_04_DnsKey) other;
        String domain = hadoop_04_dnsKey.getDomain();
        String sip = hadoop_04_dnsKey.getSip();
        String timeStr = hadoop_04_dnsKey.getTimeStr();
        if(domain.equals(this.domain)
                &&sip.equals(this.sip)
                &&timeStr.equals(this.timeStr)){
            return 0;
        }else {
            return 1;
        }
    }

    @Override
    public String toString() {
        return "Hadoopp_04_DnsKey{" +
                "domain='" + domain + '\'' +
                ", sip='" + sip + '\'' +
                ", timeStr='" + timeStr + '\'' +
                '}';
    }

    public Hadoop_04_DnsKey() {
    }

    @Override
    public int getLength() {
        return this.getLength();
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }

    public Hadoop_04_DnsKey(String domain, String sip, String timeStr) {
        this.domain = domain;
        this.sip = sip;
        this.timeStr = timeStr;
    }


    /**
     * 定义对象在写入hdfs文件系统时的序列化结构
      * @param dataOutput
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBytes(domain+System.lineSeparator());
        dataOutput.writeBytes(sip+System.lineSeparator());
        dataOutput.writeBytes(timeStr+System.lineSeparator());
    }

    /**
     * 定义从hadfs文件系统读取对象时的发序列化结构
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        domain=dataInput.readLine();
        sip=dataInput.readLine();
        timeStr=dataInput.readLine();
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getSip() {
        return sip;
    }

    public void setSip(String sip) {
        this.sip = sip;
    }

    public String getTimeStr() {
        return timeStr;
    }

    public void setTimeStr(String timeStr) {
        this.timeStr = timeStr;
    }
}
