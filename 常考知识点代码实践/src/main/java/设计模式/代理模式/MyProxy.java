package 设计模式.代理模式;

/**
 * @author: wzx
 * @date: 2023/5/18
 */
public class MyProxy extends TalkImpl {
    private PoliteLanguage politeLanguage;
    public MyProxy(){
        this.politeLanguage=new PoliteLanguage();
    }


    @Override
    public void talk() {
        politeLanguage.sayHello();
        super.talk();
        politeLanguage.sayBye();
    }
}
