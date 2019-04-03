package com.baofeng.dt.asteroidea.exception;

/**
 * 数据库初始化异常
 *
 * @author mingjia
 * @date 2015-11-13
 */
public class DBInitException extends RuntimeException {
    public DBInitException() {
    }

    public DBInitException(Throwable cause) {
        super(cause);
    }

    public DBInitException(String message) {
        super(message);
    }

    public DBInitException(String message, Throwable cause) {
        super(message, cause);
    }
}
