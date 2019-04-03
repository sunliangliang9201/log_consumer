package com.baofeng.dt.asteroidea.exception;

/**
 * 数据库层执行异常
 *
 * @author mingjia
 * @date 2015-11-16
 */
public class DBExecutionException extends RuntimeException {
    public DBExecutionException() {
    }

    public DBExecutionException(Throwable cause) {
        super(cause);
    }

    public DBExecutionException(String message) {
        super(message);
    }

    public DBExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
