package 并发编程.ThreadLocal学习;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * @author: wzx
 * @date: 2023/5/22
 */

public  class SafeDateFormat {
    //定义ThreadLocal变量
    static final ThreadLocal<DateFormat> tl=ThreadLocal.withInitial(()-> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

    static DateFormat get(){
        return tl.get();
    }
}
    //不同线程执行下面代码
//返回的df是不同的

