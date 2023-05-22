package 设计模式.适配器模式.类模式;

/**
 * @author: wzx
 * @date: 2023/5/18
 */
public class SHJAdaptor extends SHJ implements WZX {


    public void play() {
        super.watchTV();
    }
}
