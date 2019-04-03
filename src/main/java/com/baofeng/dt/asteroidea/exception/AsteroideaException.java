package com.baofeng.dt.asteroidea.exception;

/**
 * @author mignjia
 * @date 17/3/17
 */
public class AsteroideaException extends RuntimeException {
    public AsteroideaException() {
    }

    public AsteroideaException(Throwable cause) {
        super(cause);
    }

    public AsteroideaException(String message) {
        super(message);
    }

    public AsteroideaException(String message, Throwable cause) {
        super(message, cause);
    }
}
