package 设计模式.单例模式;

/**
 * @author: wzx
 * @date: 2023/5/18
 */
public class HungrySingleton {
    private static final HungrySingleton hungrySingleton=new HungrySingleton();
    private HungrySingleton(){};
    public static HungrySingleton getInstance(){
        return hungrySingleton;
    }
}
