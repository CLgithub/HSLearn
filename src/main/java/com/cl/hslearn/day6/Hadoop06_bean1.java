package com.cl.hslearn.day6;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * hadoop06 map输出key
 */
public class Hadoop06_bean1 implements WritableComparable<Hadoop06_bean1>{
    @Override
    public int compareTo(Hadoop06_bean1 o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(oId);
        dataOutput.writeUTF(dateStr);
        dataOutput.writeUTF(pId);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(categoryId);
        dataOutput.writeInt(price);
        dataOutput.writeInt(flag);

//        dataOutput.writeBytes(new String(oId)+System.lineSeparator());
//        dataOutput.writeBytes(dateStr.getBytes("utf-8")+System.lineSeparator());
//        dataOutput.writeBytes(pId.getBytes("utf-8")+System.lineSeparator());
//        dataOutput.writeInt(amount);
//        dataOutput.writeBytes(new String(name.getBytes("utf-8"))+System.lineSeparator());
//        dataOutput.writeBytes(categoryId.getBytes("utf-8")+System.lineSeparator());
//        dataOutput.writeInt(price);
//        dataOutput.writeInt(flag);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        oId=dataInput.readUTF();
        dateStr=dataInput.readUTF();
        pId=dataInput.readUTF();
        amount=dataInput.readInt();
        name=dataInput.readUTF();
        categoryId=dataInput.readUTF();
        price=dataInput.readInt();
        flag=dataInput.readInt();

//        oId=dataInput.readLine();
//        dateStr=dataInput.readLine();
//        pId=dataInput.readLine();
//        amount=dataInput.readInt();
//        name=dataInput.readLine();
//        categoryId=dataInput.readLine();
//        price=dataInput.readInt();
//        flag=dataInput.readInt();
    }

    private String oId;
    private String dateStr;
    private String pId;
    private int amount;
    private String name;
    private String categoryId;
    private int price;

    private int flag;    //0:封装订单记录，1：封装产品信息记录

    public Hadoop06_bean1() {
    }

    public Hadoop06_bean1(String oId, String dateStr, String pId, int amount, String name, String categoryId, int price, int flag) {
        this.oId = oId;
        this.dateStr = dateStr;
        this.pId = pId;
        this.amount = amount;
        this.name = name;
        this.categoryId = categoryId;
        this.price = price;
        this.flag = flag;
    }

    public void set(String oId, String dateStr, String pId, int amount, String name, String categoryId, int price, int flag) {
        this.oId = oId;
        this.dateStr = dateStr;
        this.pId = pId;
        this.amount = amount;
        this.name = name;
        this.categoryId = categoryId;
        this.price = price;
        this.flag = flag;
    }


    @Override
    public String toString() {
        return "Hadoop06_bean1{" +
                "oId='" + oId + '\'' +
                ", dateStr='" + dateStr + '\'' +
                ", pId='" + pId + '\'' +
                ", amount=" + amount +
                ", name='" + name + '\'' +
                ", categoryId='" + categoryId + '\'' +
                ", price=" + price +
                ", flag=" + flag +
                '}';
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getoId() {
        return oId;
    }

    public void setoId(String oId) {
        this.oId = oId;
    }

    public String getDateStr() {
        return dateStr;
    }

    public void setDateStr(String dateStr) {
        this.dateStr = dateStr;
    }

    public String getpId() {
        return pId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(String categoryId) {
        this.categoryId = categoryId;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }
}
