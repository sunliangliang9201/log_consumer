package com.baofeng.dt.asteroidea.handler;

/**
 * @author mignjia
 * @date 17/4/12
 */
public enum AckMode {

    /**
     * Commit after each record is processed by the listener.
     */
    RECORD,

    /**
     * Commit whatever has already been processed before the next poll.
     */
    BATCH,

    /**
     * Commit pending updates after
     * {@link ContainerProperties#setAckTime(long) ackTime} has elapsed.
     */
    TIME,

    /**
     * Commit pending updates after
     * {@link ContainerProperties#setAckCount(int) ackCount} has been
     * exceeded.
     */
    COUNT,

    /**
     * Commit pending updates after
     * {@link ContainerProperties#setAckCount(int) ackCount} has been
     * exceeded or after {@link ContainerProperties#setAckTime(long)
     * ackTime} has elapsed.
     */
    COUNT_TIME,

    /**
     * User takes responsibility for acks using an
     * {@link AcknowledgingMessageListener}.
     */
    MANUAL,

    /**
     * User takes responsibility for acks using an
     * {@link AcknowledgingMessageListener}. The consumer is woken to
     * immediately process the commit.
     */
    MANUAL_IMMEDIATE,
}
