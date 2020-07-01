
package io.platform.hadoop;

import com.google.common.base.Strings;

public class Mytest {
    public static void main(String[] args) {
        System.out.println("A".getBytes().length);
        System.out.println("a".getBytes().length);
        System.out.println("_".getBytes().length);
        String fileName = "ABcddss_h_xx";
        int x = 1024 - fileName.length();
        System.out.println(x);
        fileName = fileName + Strings.repeat("@", x);
        System.out.println(fileName);
        System.out.println(fileName.substring(0, fileName.indexOf("@")));
    }
}
