package 设计模式.单例模式;

/**
 * @author: wzx
 * @date: 2023/5/18
 */
public class StaticSingleton {
    private  StaticSingleton(){};
    private static class StaticSingletonHolder{
        private static final StaticSingleton staticSingleton=new StaticSingleton();
    }
    public static StaticSingleton getInstance(){
        return StaticSingletonHolder.staticSingleton;

    }
}
