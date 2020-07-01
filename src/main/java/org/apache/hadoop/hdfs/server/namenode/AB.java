
package org.apache.hadoop.hdfs.server.namenode;

import java.util.ArrayList;
import java.util.Arrays;

public class AB {
    public static void main(String[] args) {
        A a = new A();
        System.out.println(a.getX());
        ArrayList<Integer> integers = new ArrayList<>();
        integers.add(3);
        integers.add(5);
        integers.add(4);
        integers.add(2);
        Integer[] integers1 = new Integer[integers.size()];
        integers.toArray(integers1);
        Arrays.sort(integers1, (o1, o2) -> o2 - o1);
        for (Integer integer : integers1) {
            System.out.println(integer);
        }
    }

    private static class A{
        private Integer x = 0;

        public Integer getX() {
            return x;
        }

        public void setX(Integer x) {
            this.x = x;
        }
    }
}
