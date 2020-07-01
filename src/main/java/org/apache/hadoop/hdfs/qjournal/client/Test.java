
package org.apache.hadoop.hdfs.qjournal.client;

public class Test {
    private static void xx(String... params) {
        if (null != params) {
            System.out.println(params.length);
            System.out.println(params[0]);
        }else{
            System.out.println("params is null");
        }
    }


    public static void main(String[] args) {
        xx("a", "b");
        System.out.println("===========");
        xx((String) null);
        System.out.println("===========");
        xx(null);
    }
}
