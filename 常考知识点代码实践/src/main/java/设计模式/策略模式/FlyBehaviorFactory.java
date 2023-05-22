package 设计模式.策略模式;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: wzx
 * @date: 2023/5/19
 */
public class FlyBehaviorFactory {
    private static final Map<String,FlyBehavior> flyBehaviorMap=new HashMap<String,FlyBehavior>();
    static {
        flyBehaviorMap.put("airFly",new FlyAir());
        flyBehaviorMap.put("waterFly",new FlyWater());
        flyBehaviorMap.put("groudFly",new FlyGround());
    }
    public  static FlyBehavior getFlyBehavior(String s){
        return flyBehaviorMap.get(s);
    }
}
