package com.cl.hslearn.day4;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by L on 18/2/11.
 多线程基础回顾；
    在java中，实现多线程有两种方式
        1.继承Thread线程类
        2.实现Runnable接口
     解决线程安全问题也有两种方式：
        1.synchronized关键字
        2.lock接口
            两者区别：
                1）lock时一个接口，而synchronized时java中的关键字，synchronized时内置的语言实现
                2）synchronizedzai 发生异常时，会自动释放线程占有的锁，而lock在发生异常时，如果没有主动通过unlock()去释放锁，则很可能造成死锁现象
            因此使用lock时需要在finally块中释放锁
               // 3）lock可以让得到锁的线程相应中断，而synchronized却不行，使用synchronized时，等待的线程会一直等待下去
                4）通过lock可以知道有没有成功释放锁，而synchronized无法办到
                5）lock可以提好多个线程进行读写的效率(ReentrantReadWriteLock)

 */
public class Thread_01 {
    public static void main(String[] args){
//        testThread();
        testRunable();

    }

    private static void testThread() {
        Thread1 t1=new Thread1();
        Thread1 t2=new Thread1();
        Thread1 t3=new Thread1();
        t1.start();
        t2.start();
        t3.start();
    }

    private static void testRunable(){
        Thread2 t=new Thread2();
        new Thread(t).start();
        new Thread(t).start();
        new Thread(t).start();
    }
}

class Thread1 extends Thread{
    private static int tickets=100;
    static String syn=new String("");
    @Override
    public void run() {
        while (tickets>0){
            synchronized (syn) {
                if(tickets>0) {
                    shoupiao();
                }
                try {
                    Thread.sleep(25);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    private synchronized void shoupiao() {
        System.out.println(Thread.currentThread().getName() + ":" + tickets);
        tickets--;
    }
}

class Thread2 implements Runnable{
    static int tickets=100;
    static String syn=new String("");

    public void run() {
        while(tickets>0){  //首先保证有飘可卖
            synchronized (syn){ //同步标注
                if(tickets>0){ //判断同步条件下是非有票可卖
                    shoupiao();
                }
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void shoupiao() {
        System.out.println(Thread.currentThread().getName()+":"+tickets);
        tickets--;
    }
}
