package com.baofeng.dt.asteroidea.handler;

/**
 * Handle for acknowledging the processing of a
 * {@link org.apache.kafka.clients.consumer.ConsumerRecord}. Recipients can store the
 * reference in asynchronous scenarios, but the internal state should be assumed transient
 * (i.e. it cannot be serialized and deserialized later)
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public interface Acknowledgment {

    /**
     * Invoked when the message for which the acknowledgment has been created has been processed.
     * Calling this method implies that all the previous messages in the partition have been processed already.
     */
    void acknowledge();

}