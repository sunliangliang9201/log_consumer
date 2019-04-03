package com.baofeng.dt.asteroidea.util;

import org.apache.commons.lang.StringUtils;

/**
 * db名字和java名字之间转化
 * DB:下划线分割  java:驼峰
 *
 * @author mingjia
 * @date 2015-11-12
 */
public class NameConvertor {
    private static final char DOWN_CHAR = '_';
    private static final char BLANK_CHAR = ' ';

//    TODO 缓存
//    private static final Cache<String, String> cache = CacheBuilder.newBuilder()
//            .expireAfterAccess(5, TimeUnit.MINUTES)
//            .maximumSize(1000)
//            .build();

    /**
     * MyName ==> my_name
     *
     * @param nameInJava
     * @return
     */
    public static String forDB(String nameInJava) {
        if (StringUtils.isBlank(nameInJava))
            return nameInJava;

        StringBuilder newName = new StringBuilder();
        for (int i = 0; i < nameInJava.length(); i++) {
            char thisChar = nameInJava.charAt(i);
            char preChar = i > 0 ? nameInJava.charAt(i - 1) : BLANK_CHAR;

            if (isUpper(thisChar) && isLower(preChar))
                newName.append(DOWN_CHAR);

            newName.append(toLower(thisChar));
        }
        return newName.toString();
    }

    /**
     * my_name ==> myName
     *
     * @param nameInDB
     * @return
     */
    public static String forJava(String nameInDB) {
        if (StringUtils.isBlank(nameInDB))
            return nameInDB;

        StringBuilder newName = new StringBuilder();
        for (int i = 0; i < nameInDB.length(); i++) {
            char thisChar = nameInDB.charAt(i);
            char preChar = i > 0 ? nameInDB.charAt(i - 1) : BLANK_CHAR;

            if (thisChar == DOWN_CHAR)
                continue;

            if (preChar == DOWN_CHAR) {
                newName.append(toUpper(thisChar));
            } else {
                newName.append(thisChar);
            }
        }
        return newName.toString();
    }

    private static boolean isUpper(char c) {
        return c >= 'A' && c <= 'Z';
    }

    private static boolean isLower(char c) {
        return c >= 'a' && c <= 'z';
    }

    private static char toLower(char c) {
        return Character.toLowerCase(c);
    }

    private static char toUpper(char c) {
        return Character.toUpperCase(c);
    }
}
