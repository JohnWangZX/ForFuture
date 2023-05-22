package 并发编程.ThreadLocal学习;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author: wzx
 * @date: 2023/5/22
 */

public  class ThreadId {
    static final AtomicLong nextId=new AtomicLong(0);
    //定义ThreadLocal变量
    static final ThreadLocal<Long> tl=ThreadLocal.withInitial(()->nextId.getAndIncrement());
    //此方法会为每个线程分配一个唯一的Id
    static long get(){
        return tl.get();
    }
}
