import 设计模式.适配器模式.对象模式.OSHJ;
import 设计模式.适配器模式.对象模式.OSHJAdaptor;
import 设计模式.适配器模式.对象模式.OWZX;
import 设计模式.适配器模式.类模式.SHJAdaptor;
import 设计模式.适配器模式.类模式.WZX;

/**
 * @author: wzx
 * @date: 2023/5/18
 */
public class AdaptorTest {
    public static void main(String[] args) {
        WZX wzx= new SHJAdaptor();
        wzx.play();
        wzx.readBooks();

        OWZX owzx=new OSHJAdaptor(new OSHJ());
        owzx.play();
    }
}
