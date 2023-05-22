package 设计模式.策略模式;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: wzx
 * @date: 2023/5/19
 */
public class DuckWeightFactory {
    private static final List<WeightRange> weightRanges=new ArrayList<WeightRange>();
    static {
        weightRanges.add(new WeightRange(0,5,new FlyAir()));
        weightRanges.add(new WeightRange(5,10,new FlyWater()));
        weightRanges.add(new WeightRange(10,15,new FlyGround()));
    }
    public static Duck getDuck(int x){
        Duck duck=null;
        for (WeightRange weightRange : weightRanges) {
            if(weightRange.inRange(x)){
                duck=new Duck(weightRange.getFlyBehavior());
            }
        }
        return duck;
    }



    private static class WeightRange{
        private int startRange;
        private int endRange;
        private FlyBehavior flyBehavior;
        public WeightRange(int startRange,int endRange,FlyBehavior flyBehavior){
            this.startRange=startRange;
            this.endRange=endRange;
            this.flyBehavior=flyBehavior;
        }
        public FlyBehavior getFlyBehavior(){
            return  flyBehavior;
        }
        public boolean inRange(int x){
            return x>=startRange&&x<endRange;
        }

    }
}
