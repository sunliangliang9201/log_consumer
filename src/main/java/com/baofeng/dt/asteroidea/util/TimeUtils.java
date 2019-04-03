package com.baofeng.dt.asteroidea.util;

import org.apache.commons.lang.time.FastDateFormat;

import java.util.Date;
import java.util.TimeZone;

/**
 * @author mignjia
 * @version 1.0
 * @Descriptions
 * @date 16/2/17 下午4:03
 */
public class TimeUtils {


    private static ThreadLocal<FastDateFormat> timeThreadLocal;

    static {

        timeThreadLocal = new ThreadLocal<FastDateFormat>() {
            @Override
            protected synchronized FastDateFormat initialValue() {
                FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
                        TimeZone.getTimeZone("Etc/UTC"));
                return fastDateFormat;
            }
        };

    }

    public static String getFastDate(){
        return timeThreadLocal.get().format(new Date()).toString();
    }
}
