package com.baofeng.dt.asteroidea.exception;

/**
 * @author mingjia
 * @date 2015-11-11
 */
public class DBNotFoundException extends RuntimeException {
    public DBNotFoundException(String message) {
        super(message);
    }
}
