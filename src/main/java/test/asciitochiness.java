package test;


import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * Author wenBin
 * Date 2019/5/23 9:33
 * Version 1.0
 */
public class asciitochiness {

    public static void main(String[] args) throws UnsupportedEncodingException {

        String str = "\\xE5\\xAE\\xA4\\xE5\\x86\\x85\\xE9\\xA1\\xB9\\xE7\\x9B\\xAE\\xE7\\xAE\\xA1\\xE7\\x90\\x86";
        String str1 = "%E5%B9%B4%E5%B1%B1%E4%B8%9C%E7%9C%81%E5%9F%BA%E5%9B%A0%E5%88%86%E5%9E%8B%E6%A3%80%E6%B5%8B%E5%AE%A4%E9%97%B4%E8%B4%A8%E8%AF%84%E8%B0%83%E6%9F%A5";
        //注意：在16进制转字符串时我们要先将\x去掉再进行转码
        String string = hexStringToString(str.replaceAll("\\\\x", ""));
        String string1 = hexStringToString(str1.replaceAll("%", ""));
        System.out.println(string1);

    }

    public static String hexStringToString(String s) {
        if (s == null || s.equals("")) {
            return null;
        }
        s = s.replace(" ", "");
        byte[] baKeyword = new byte[s.length() / 2];
        for (int i = 0; i < baKeyword.length; i++) {
            try {
                baKeyword[i] = (byte) (0xff & Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            s = new String(baKeyword, "UTF-8");
            new String();
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        return s;
    }
}
