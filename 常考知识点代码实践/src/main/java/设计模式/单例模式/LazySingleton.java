package 设计模式.单例模式;

/**
 * @author: wzx
 * @date: 2023/5/18
 */
public class LazySingleton {
    private static volatile LazySingleton singleton;
    private LazySingleton(){};
    public static LazySingleton getInstance(){
        if(singleton==null){
            synchronized (LazySingleton.class){
                if(singleton==null)
                    singleton= new LazySingleton();
            }

        }
        return singleton;
    }
}
