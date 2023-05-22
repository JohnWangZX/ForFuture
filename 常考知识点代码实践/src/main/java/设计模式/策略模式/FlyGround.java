package 设计模式.策略模式;

/**
 * @author: wzx
 * @date: 2023/5/19
 */
public class FlyGround implements FlyBehavior {
    public void fly() {
        System.out.println("fly on the ground");
    }
}
