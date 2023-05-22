package 设计模式.策略模式;

/**
 * @author: wzx
 * @date: 2023/5/19
 */
public  class Duck {
   private  FlyBehavior flyBehavior;
   public Duck (FlyBehavior flyBehavior){
        this.flyBehavior=flyBehavior;
    }
    public void fly(){
        flyBehavior.fly();
    }

}
