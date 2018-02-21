package com.cl.hslearn.day6;

import org.apache.hadoop.io.Writable;
import sun.jvm.hotspot.debugger.posix.elf.ELFSectionHeader;

import javax.swing.text.DefaultStyledDocument;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Hadoopp_04_DnsKey implements Writable {
    private String domain;
    private String sip;
    private String timeStr;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Hadoopp_04_DnsKey that = (Hadoopp_04_DnsKey) o;

        if (domain.equals(that.domain)&&sip.equals(that.sip)&&timeStr.equals(that.timeStr))
            return true;
        else
            return false;


//        if (domain != null ? !domain.equals(that.domain) : that.domain != null) return false;
//        if (sip != null ? !sip.equals(that.sip) : that.sip != null) return false;
//        return timeStr != null ? timeStr.equals(that.timeStr) : that.timeStr == null;
    }


    @Override
    public String toString() {
        return "Hadoopp_04_DnsKey{" +
                "domain='" + domain + '\'' +
                ", sip='" + sip + '\'' +
                ", timeStr='" + timeStr + '\'' +
                '}';
    }

    public Hadoopp_04_DnsKey() {
    }

    public Hadoopp_04_DnsKey(String domain, String sip, String timeStr) {
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
        dataOutput.writeBytes(domain);
        dataOutput.writeBytes(sip);
        dataOutput.writeBytes(timeStr);
    }

    /**
     * 定义从hadfs文件系统读取对象时的发序列化结构
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        domain=String.valueOf(dataInput.readByte());
        sip=String.valueOf(dataInput.readByte());
        timeStr=String.valueOf(dataInput.readByte());

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
