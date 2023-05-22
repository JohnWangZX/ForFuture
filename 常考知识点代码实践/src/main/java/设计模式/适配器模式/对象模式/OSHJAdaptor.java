package 设计模式.适配器模式.对象模式;

/**
 * @author: wzx
 * @date: 2023/5/18
 */
public class OSHJAdaptor  implements OWZX {
    private OSHJ shj;
    public OSHJAdaptor(OSHJ shj){
        this.shj=shj;
    }

    public void eat() {
     shj.eat();
    }

    public void play() {
        shj.watchTV();
    }

    public void readBooks() {
      shj.readBooks();
    }
}
