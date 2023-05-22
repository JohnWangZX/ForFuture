package 设计模式.策略模式;

import java.util.Random;

/**
 * @author: wzx
 * @date: 2023/5/19
 */
public class Main {
    public static void main(String[] args) {
        for (int i = 0; i <3 ; i++) {
            DuckWeightFactory.getDuck(new Random().nextInt(15)).fly();
        }

    }
}
