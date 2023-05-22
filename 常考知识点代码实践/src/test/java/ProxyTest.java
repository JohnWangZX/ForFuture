import 设计模式.代理模式.DynamicProxy;
import 设计模式.代理模式.MyProxy;
import 设计模式.代理模式.Talk;
import 设计模式.代理模式.TalkImpl;

/**
 * @author: wzx
 * @date: 2023/5/18
 */
public class ProxyTest {
    public static void main(String[] args) {
        TalkImpl talkImpl =new MyProxy();
        talkImpl.talk();
        DynamicProxy dynamicProxy=new DynamicProxy();
        Talk talk =(Talk)dynamicProxy.createProxy(new TalkImpl());
        talk.talk();
    }
}
