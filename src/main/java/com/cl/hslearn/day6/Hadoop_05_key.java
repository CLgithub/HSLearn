package com.cl.hslearn.day6;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.jboss.netty.util.internal.ReusableIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//public class Hadoop_05_key implements Writable,Comparable<Hadoop_05_key>{
public class Hadoop_05_key extends BinaryComparable implements WritableComparable<BinaryComparable>{
//public class Hadoop_05_key extends Text {

    @Override
    public int getLength() {
        return this.getLength();
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }

    /**
     * 用于按key排序，但是这样暂时无法将相同电话☎️号码合并在一起，因为不知道在哪判断两个key是否相同，equals并没有被调用
     * @param o
     * @return
     */
    @Override
    public int compareTo(BinaryComparable o) {
        Hadoop_05_key a=(Hadoop_05_key) o;
        if(a.getPhoneN().equals(this.phoneN)){
            System.out.println(a+"-"+this+":0");
            return 0;
        }else{
//            return -1;
            long l = this.sumNum - a.getSumNum();
            if(l>0){
                System.out.println(a+"-"+this+":-1");
                return -1;
            }else if(l<0){
                System.out.println(a+"-"+this+":1");
                return 1;
            }else {
                System.out.println(a+"-"+this+":2");
                return 1;
            }

        }
    }

    @Override
    public boolean equals(Object o) {
        System.out.println(this+"--"+o);
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        Hadoop_05_key that = (Hadoop_05_key) o;
        if(that.getPhoneN().equals(this.phoneN)) {
            return true;
        }else {
            return false;
        }

//        if (phoneN != null ? !phoneN.equals(that.phoneN) : that.phoneN != null) {
//            return false;
//        }else{
//            return true;
//        }
//        return sumNum != null ? sumNum.equals(that.sumNum) : that.sumNum == null;
    }

    @Override
    public int hashCode() {
        System.out.println(this+"hashCode");
        int result = super.hashCode();
        result = 31 * result + (phoneN != null ? phoneN.hashCode() : 0);
//        result = 31 * result + (sumNum != null ? sumNum.hashCode() : 0);
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBytes(phoneN+System.lineSeparator());
        dataOutput.writeLong(sumNum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        phoneN=dataInput.readLine();
        sumNum=dataInput.readLong();
    }

    private String phoneN;
    private Long sumNum;

    @Override
    public String toString() {
        return "Hadoop_05_key{" +
                "phoneN='" + phoneN + '\'' +
                ", sumNum=" + sumNum +
                '}';
    }

    public String getPhoneN() {
        return phoneN;
    }

    public void setPhoneN(String phoneN) {
        this.phoneN = phoneN;
    }

    public Long getSumNum() {
        return sumNum;
    }

    public void setSumNum(Long sumNum) {
        this.sumNum = sumNum;
    }

    public Hadoop_05_key() {
    }

    public Hadoop_05_key(String phoneN) {
        this.phoneN = phoneN;
    }

    public Hadoop_05_key(String phoneN, Long sumNum) {
        this.phoneN = phoneN;
        this.sumNum = sumNum;
    }

}
