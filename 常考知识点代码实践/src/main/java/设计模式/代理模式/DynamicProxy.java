package 设计模式.代理模式;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * @author: wzx
 * @date: 2023/5/18
 */
//PoliteLanguage的动态代理
public class DynamicProxy {
    private  PoliteLanguage politeLanguage;
    public  DynamicProxy(){
        politeLanguage=new PoliteLanguage();
    }
    public Object createProxy(Object proxiedObject){
        Class<?>[] Interfaces=proxiedObject.getClass().getInterfaces();
        DynamicHandler dynamicHandler=new DynamicHandler(proxiedObject);
        return Proxy.newProxyInstance(proxiedObject.getClass().getClassLoader(),Interfaces,dynamicHandler);
    }
    private class DynamicHandler implements InvocationHandler{
        private Object proxiedObject;
        public DynamicHandler(Object proxiedObject){
            this.proxiedObject=proxiedObject;
        }
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            politeLanguage.sayHello();
            Object result=method.invoke(proxiedObject,args);
            politeLanguage.sayBye();
            return result;
        }
    }
}
