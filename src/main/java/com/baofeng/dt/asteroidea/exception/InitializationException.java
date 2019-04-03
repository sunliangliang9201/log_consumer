package com.baofeng.dt.asteroidea.exception;

/**
 * @author mignjia
 * @date 17/3/17
 */
public class InitializationException extends AsteroideaException {

    public InitializationException() {
        super();
    }

    public InitializationException(String message) {
        super(message);
    }

    public InitializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public InitializationException(Throwable cause) {
        super(cause);
    }
}
