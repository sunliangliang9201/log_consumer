package com.baofeng.dt.asteroidea.handler;

import com.baofeng.dt.asteroidea.exception.KafkaException;
import com.baofeng.dt.asteroidea.model.TopicMeta;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.*;
import java.util.concurrent.*;

/**
 * @author mignjia
 * @date 17/4/12
 */
public class ManualCommitKafkaHandler {

    private static final Logger logger = LoggerFactory.getLogger(ManualCommitKafkaHandler.class);

    private static final int DEFAULT_SHUTDOWN_TIMEOUT = 10000;
    private static final int DEFAULT_QUEUE_DEPTH = 1000;
    private static final int DEFAULT_PAUSE_AFTER = 10000;
    private volatile long pollTimeout = 100;


    private volatile boolean running = false;
    private boolean syncCommits = true;
    private boolean ackOnError = true;
    private int ackCount = 50;
    private AckMode ackMode = AckMode.BATCH;
    private final ConsumerFactory consumerFactory;
    private final TopicMeta topicMeta;
    private ListenableFuture<?> listenerConsumerFuture;
    private ListenerConsumer listenerConsumer;
    private AsyncListenableTaskExecutor consumerExecutor;
    private AsyncListenableTaskExecutor listenerExecutor;

    private final Object lifecycleMonitor = new Object();
    private AcknowledgingMessageListener acknowledgingMessageListener;
    private ConsumerRebalanceListener consumerRebalanceListener;
    private MessageListener listener;
    private ErrorHandler errorHandler = new LoggingErrorHandler();

    public ManualCommitKafkaHandler(ConsumerFactory consumerFactory, TopicMeta topicMeta){
        this.consumerFactory = consumerFactory;
        this.topicMeta = topicMeta;
    }


    public final void start() {
        synchronized (this.lifecycleMonitor) {
            doStart();
        }
    }

    public void setMessageListener(MessageListener listener){
            this.listener = listener;
    }

    private void doStart(){
        if (isRunning()) {
            return;
        }
        this.consumerRebalanceListener = createConsumerRebalanceListener();
        this.consumerExecutor = new SimpleAsyncTaskExecutor(
                ("ManualCommitKafkaHandler" + "-kafka-consumer-"));
        this.listenerExecutor = new SimpleAsyncTaskExecutor(
                ("ManualCommitKafkaHandler" + "-kafka-listener-"));
        this.listenerConsumer = new ListenerConsumer(this.listener, this.acknowledgingMessageListener);
        setRunning(true);
        this.listenerConsumerFuture = this.consumerExecutor
                .submitListenable(this.listenerConsumer);
        logger.info("ManualCommitKafkaHandler start!");
    }

    public final void stop() {
        final CountDownLatch latch = new CountDownLatch(1);
        stop(new Runnable() {
            @Override
            public void run() {
                latch.countDown();
            }
        });
        try {
            latch.await(DEFAULT_SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
        }
    }


    public void stop(Runnable callback) {
        synchronized (this.lifecycleMonitor) {
            doStop(callback);
        }
    }

    private void doStop(final Runnable callback) {
        if (isRunning()) {
            this.listenerConsumerFuture.addCallback(new ListenableFutureCallback<Object>() {
                @Override
                public void onFailure(Throwable e) {
                   logger.error("Error while stopping the container: ", e);
                    if (callback != null) {
                        callback.run();
                    }
                }

                @Override
                public void onSuccess(Object result) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(ManualCommitKafkaHandler.this + " stopped normally");
                    }
                    if (callback != null) {
                        callback.run();
                    }
                }
            });
            setRunning(false);
            this.listenerConsumer.consumer.wakeup();
        }
    }



    public boolean isRunning() {
        return this.running;
    }

    protected void setRunning(boolean running) {
        this.running = running;
    }


    private final class ListenerConsumer implements SchedulingAwareRunnable {

        private final Logger logger = LoggerFactory.getLogger(ListenerConsumer.class);

        private final Consumer consumer;

        private final Map<String, Map<Integer, Long>> offsets = new HashMap<String, Map<Integer, Long>>();

        private final MessageListener listener;

        private final AcknowledgingMessageListener acknowledgingMessageListener;

        private final boolean autoCommit = false;

        private final boolean isManualAck = false;

        private final boolean isManualImmediateAck = false;
//                this.containerProperties.getAckMode().equals(AckMode.MANUAL_IMMEDIATE);

        private final boolean isAnyManualAck = true;

        private final boolean isRecordAck = false;

        private final boolean isBatchAck = true;

        private final BlockingQueue<ConsumerRecords> recordsToProcess =
                new LinkedBlockingQueue<ConsumerRecords>(DEFAULT_QUEUE_DEPTH);

        private final BlockingQueue<ConsumerRecord> acks = new LinkedBlockingQueue<ConsumerRecord>();

//        private final ApplicationEventPublisher applicationEventPublisher = getApplicationEventPublisher();

        private volatile Map<TopicPartition, Long> definedPartitions;

        private ConsumerRecords unsent;

        private volatile Collection<TopicPartition> assignedPartitions;

        private int count;

        private volatile ListenerInvoker invoker;

        private long last;

        private volatile Future<?> listenerInvokerFuture;

        /**
         * The consumer is currently paused due to a slow listener. The consumer will be
         * resumed when the current batch of records has been processed but will continue
         * to be polled.
         */
        private boolean paused;

        private ListenerConsumer(MessageListener listener, AcknowledgingMessageListener ackListener) {
            final Consumer consumer = ManualCommitKafkaHandler.this.consumerFactory.createConsumer();

            ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // do not stop the invoker if it is not started yet
                    // this will occur on the initial start on a subscription
                    if (ListenerConsumer.this.logger.isTraceEnabled()) {
                        ListenerConsumer.this.logger.trace("Received partition revocation notification, " +
                                "and will stop the invoker.");
                    }
                    if (ListenerConsumer.this.listenerInvokerFuture != null) {
                        stopInvokerAndCommitManualAcks();
                        ListenerConsumer.this.recordsToProcess.clear();
                        ListenerConsumer.this.unsent = null;
                    }
                    else {
                        if (!CollectionUtils.isEmpty(partitions)) {
                            ListenerConsumer.this.logger.error("Invalid state: the invoker was not active, " +
                                    "but the consumer had allocated partitions");
                        }
                    }
                    ManualCommitKafkaHandler.this.consumerRebalanceListener.onPartitionsRevoked(partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    ListenerConsumer.this.assignedPartitions = partitions;
                    // Commit initial positions - this is generally redundant but
                    // it protects us from the case when another consumer starts
                    // and rebalance would cause it to reset at the end
                    // see https://github.com/spring-projects/spring-kafka/issues/110
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
                    for (TopicPartition partition : partitions) {
                        offsets.put(partition, new OffsetAndMetadata(consumer.position(partition)));
                    }
                    if (ListenerConsumer.this.logger.isDebugEnabled()) {
                        ListenerConsumer.this.logger.debug("Committing: " + offsets);
                    }
                    if (ManualCommitKafkaHandler.this.syncCommits) {
                        ListenerConsumer.this.consumer.commitSync(offsets);
                    }

                    // We will not start the invoker thread if we are in autocommit mode,
                    // as we will execute synchronously then
                    // We will not start the invoker thread if the container is stopped
                    // We will not start the invoker thread if there are no partitions to
                    // listen to
                    if ( ManualCommitKafkaHandler.this.isRunning() && !CollectionUtils.isEmpty(partitions)) {
                        startInvoker();
                    }
                    ManualCommitKafkaHandler.this.consumerRebalanceListener.onPartitionsAssigned(partitions);
                }

            };

            //todo  init topics
            consumer.subscribe(Arrays.asList(ManualCommitKafkaHandler.this.topicMeta.getTopics().split(",")), rebalanceListener);
            this.consumer = consumer;
            this.listener = listener;
            this.acknowledgingMessageListener = ackListener;

            logger.info("Start consumer with topic:" + topicMeta.getTopics());
        }

        private void startInvoker() {
            ListenerConsumer.this.invoker = new ListenerInvoker();
            ListenerConsumer.this.listenerInvokerFuture = ManualCommitKafkaHandler.this.listenerExecutor
                    .submit(ListenerConsumer.this.invoker);
        }

        @Override
        public boolean isLongLived() {
            return true;
        }

        @Override
        public void run() {
            this.count = 0;
            this.last = System.currentTimeMillis();
            if (isRunning() && this.definedPartitions != null) {
                initPartitionsIfNeeded();
                // we start the invoker here as there will be no rebalance calls to
                // trigger it, but only if the container is not set to autocommit
                // otherwise we will process records on a separate thread
                if (!this.autoCommit) {
                    startInvoker();
                }
            }
            long lastAlertAt = System.currentTimeMillis();
            while (isRunning()) {
                try {
                    if (this.logger.isTraceEnabled()) {
                        this.logger.trace("Polling (paused=" + this.paused + ")...");
                    }
                    ConsumerRecords records = this.consumer.poll(pollTimeout);
                    if (this.logger.isDebugEnabled()) {
                        this.logger.debug("Received: " + records.count() + " records");
                    }
                    if (records != null && records.count() > 0) {
                        // if the container is set to auto-commit, then execute in the
                        // same thread
                        // otherwise send to the buffering queue
                        if (sendToListener(records)) {
                                this.consumer.pause(this.assignedPartitions
                                        .toArray(new TopicPartition[this.assignedPartitions.size()]));

                                logger.info("Pause consumer with partitions "+this.assignedPartitions.toString());
                                this.paused = true;
                                this.unsent = records;
                        }
                    }

                    this.unsent = checkPause(this.unsent);
                    processCommits();
                }
                catch (WakeupException e) {
                    this.unsent = checkPause(this.unsent);
                }
                catch (Exception e) {
                    if (ManualCommitKafkaHandler.this.errorHandler != null) {
                        ManualCommitKafkaHandler.this.errorHandler.handle(e, null);
                    }
                    else {
                        this.logger.error("Container exception", e);
                    }
                }
            }
            if (this.listenerInvokerFuture != null) {
                stopInvokerAndCommitManualAcks();
            }
            try {
                this.consumer.unsubscribe();
            }
            catch (WakeupException e) {
                // No-op. Continue process
            }
            this.consumer.close();
            if (this.logger.isInfoEnabled()) {
                this.logger.info("Consumer stopped");
            }
        }


        private void stopInvokerAndCommitManualAcks() {
            long now = System.currentTimeMillis();
            this.invoker.stop();
            long remaining = DEFAULT_SHUTDOWN_TIMEOUT + now - System.currentTimeMillis();
            try {
                this.listenerInvokerFuture.get(remaining, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (ExecutionException e) {
                this.logger.error("Error while shutting down the listener invoker:", e);
            }
            catch (TimeoutException e) {
                this.logger.info("Invoker timed out while waiting for shutdown and will be canceled.");
                this.listenerInvokerFuture.cancel(true);
            }
            finally {
                this.listenerInvokerFuture = null;
            }
            processCommits();
            if (this.offsets.size() > 0) {
                // we always commit after stopping the invoker
                commitIfNecessary();
            }
            this.invoker = null;
        }

        private ConsumerRecords checkPause(ConsumerRecords unsent) {
            if (this.paused && this.recordsToProcess.size() < DEFAULT_QUEUE_DEPTH) {
                // Listener has caught up.
                this.consumer.resume(
                        this.assignedPartitions.toArray(new TopicPartition[this.assignedPartitions.size()]));
                logger.info("Resume consumer with partitions "+this.assignedPartitions.toString());
                this.paused = false;
                if (unsent != null) {
                    try {
                        sendToListener(unsent);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new KafkaException("Interrupted while sending to listener", e);
                    }
                }
                return null;
            }
            return unsent;
        }

        private boolean sendToListener(final ConsumerRecords records) throws InterruptedException {
                return !this.recordsToProcess.offer(records, DEFAULT_PAUSE_AFTER, TimeUnit.MILLISECONDS);
        }

        /**
         * Process any acks that have been queued by the listener thread.
         */
        private void handleAcks() {
            ConsumerRecord record = this.acks.poll();
            while (record != null) {
                if (this.logger.isTraceEnabled()) {
                    this.logger.trace("Ack: " + record);
                }
                processAck(record);
                record = this.acks.poll();
            }
        }

        private void processAck(ConsumerRecord record) {
            if (ListenerConsumer.this.isManualImmediateAck) {
                try {
                    ackImmediate(record);
                }
                catch (WakeupException e) {
                    // ignore - not polling
                }
            }
            else {
                addOffset(record);
            }
        }

        private void ackImmediate(ConsumerRecord record) {
            Map<TopicPartition, OffsetAndMetadata> commits = Collections.singletonMap(
                    new TopicPartition(record.topic(), record.partition()),
                    new OffsetAndMetadata(record.offset() + 1));
            if (ListenerConsumer.this.logger.isDebugEnabled()) {
                ListenerConsumer.this.logger.debug("Committing: " + commits);
            }
            if (!ManualCommitKafkaHandler.this.syncCommits) {
                return;
            }
            ListenerConsumer.this.consumer.commitSync(commits);
        }

        private void invokeListener(final ConsumerRecords records) {
            Iterator<ConsumerRecord> iterator = records.iterator();
            while (iterator.hasNext() && ((this.invoker != null && this.invoker.active))) {
                final ConsumerRecord record = iterator.next();
                if (this.logger.isTraceEnabled()) {
                    this.logger.trace("Processing " + record);
                }
                try {
                    if (this.acknowledgingMessageListener != null) {
                        this.acknowledgingMessageListener.onMessage(record,
                                new ConsumerAcknowledgment(record, this.isManualImmediateAck));
                    }
                    else {
                        this.listener.onMessage(record);
                    }
                    this.acks.add(record);
                    if (this.isRecordAck) {
                        this.consumer.wakeup();
                    }
                }
                catch (Exception e) {
                    if (ManualCommitKafkaHandler.this.ackOnError) {
                        this.acks.add(record);
                    }
                    if (ManualCommitKafkaHandler.this.errorHandler != null) {
                        ManualCommitKafkaHandler.this.errorHandler.handle(e, record);
                    }
                    else {
                        this.logger.error("Listener threw an exception and no error handler for " + record, e);
                    }
                }
            }
            if (this.isManualAck || this.isBatchAck) {
                this.consumer.wakeup();
            }
        }

        private void processCommits() {
            handleAcks();
            this.count += this.acks.size();
//            long now;
            AckMode ackMode =  ManualCommitKafkaHandler.this.ackMode;
            if (!this.isManualAck) {
                updatePendingOffsets();
            }
//            boolean countExceeded = this.count >= ManualCommitKafkaHandler.this.ackCount;
            if (this.logger.isDebugEnabled() && ackMode.equals(AckMode.COUNT)) {
                this.logger.debug("Committing in AckMode.COUNT because count " + this.count
                        + " exceeds configured limit of " + ManualCommitKafkaHandler.this.ackCount);
            }
            commitIfNecessary();
            this.count = 0;
        }

        private void initPartitionsIfNeeded() {
			/*
			 * Note: initial position setting is only supported with explicit topic assignment.
			 * When using auto assignment (subscribe), the ConsumerRebalanceListener is not
			 * called until we poll() the consumer.
			 */
            for (Map.Entry<TopicPartition, Long> entry : this.definedPartitions.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                Long offset = entry.getValue();
                if (offset != null) {
                    long newOffset = offset;

                    if (offset < 0) {
                        this.consumer.seekToEnd(topicPartition);
                        newOffset = this.consumer.position(topicPartition) + offset;
                    }

                    this.consumer.seek(topicPartition, newOffset);
                    if (this.logger.isDebugEnabled()) {
                        this.logger.debug("Reset " + topicPartition + " to offset " + newOffset);
                    }
                }
            }
        }

        private void updatePendingOffsets() {
            ConsumerRecord record = this.acks.poll();
            while (record != null) {
                addOffset(record);
                record = this.acks.poll();
            }
        }

        private void addOffset(ConsumerRecord record) {
            if (!this.offsets.containsKey(record.topic())) {
                this.offsets.put(record.topic(), new HashMap<Integer, Long>());
            }
            this.offsets.get(record.topic()).put(record.partition(), record.offset());
        }

        private void commitIfNecessary() {
            Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<TopicPartition, OffsetAndMetadata>();
            for (Map.Entry<String, Map<Integer, Long>> entry : this.offsets.entrySet()) {
                for (Map.Entry<Integer, Long> offset : entry.getValue().entrySet()) {
                    commits.put(new TopicPartition(entry.getKey(), offset.getKey()),
                            new OffsetAndMetadata(offset.getValue() + 1));
                }
            }
            this.offsets.clear();
            if (this.logger.isDebugEnabled()) {
                this.logger.debug("Commit list: " + commits);
            }
            if (!commits.isEmpty()) {
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug("Committing: " + commits);
                }
                try {
                    this.consumer.commitSync(commits);
                }
                catch (WakeupException e) {
                    // ignore - not polling
                    if (this.logger.isDebugEnabled()) {
                        this.logger.debug("Woken up during commit:",e);

                    }
                }
            }
        }

        private final class ListenerInvoker implements SchedulingAwareRunnable {

            private final CountDownLatch exitLatch = new CountDownLatch(1);

            private volatile boolean active = true;

            private volatile Thread executingThread;

            @Override
            public void run() {
                Assert.isTrue(this.active, "This instance is not active anymore");
                try {
                    this.executingThread = Thread.currentThread();
                    while (this.active) {
                        try {
                            ConsumerRecords records = ListenerConsumer.this.recordsToProcess.poll(500,
                                    TimeUnit.MILLISECONDS);
                            if (this.active) {
                                if (records != null) {
                                    invokeListener(records);
                                }
                                else {
                                    if (ListenerConsumer.this.logger.isTraceEnabled()) {
                                        ListenerConsumer.this.logger.trace("No records to process");
                                    }
                                }
                            }
                        }
                        catch (InterruptedException e) {
                            if (!this.active) {
                                Thread.currentThread().interrupt();
                            }
                            else {
                                ListenerConsumer.this.logger.debug("Interrupt ignored");
                            }
                        }
                        if (!ListenerConsumer.this.isManualImmediateAck && this.active) {
                            ListenerConsumer.this.consumer.wakeup();
                        }
                    }
                }
                finally {
                    this.active = false;
                    this.exitLatch.countDown();
                }
            }

            @Override
            public boolean isLongLived() {
                return true;
            }

            private void stop() {
                if (ListenerConsumer.this.logger.isDebugEnabled()) {
                    ListenerConsumer.this.logger.debug("Stopping invoker");
                }
                this.active = false;
                try {
                    if (!this.exitLatch.await(DEFAULT_SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS)
                            && this.executingThread != null) {
                        if (ListenerConsumer.this.logger.isDebugEnabled()) {
                            ListenerConsumer.this.logger.debug("Interrupting invoker");
                        }
                        this.executingThread.interrupt();
                    }
                }
                catch (InterruptedException e) {
                    if (this.executingThread != null) {
                        this.executingThread.interrupt();
                    }
                    Thread.currentThread().interrupt();
                }
                if (ListenerConsumer.this.logger.isDebugEnabled()) {
                    ListenerConsumer.this.logger.debug("Invoker stopped");
                }
            }
        }

        private final class ConsumerAcknowledgment implements Acknowledgment {

            private final ConsumerRecord record;

            private final boolean immediate;

            private ConsumerAcknowledgment(ConsumerRecord record, boolean immediate) {
                this.record = record;
                this.immediate = immediate;
            }

            @Override
            public void acknowledge() {
                try {
                    ListenerConsumer.this.acks.put(this.record);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new KafkaException("Interrupted while queuing ack for " + this.record, e);
                }
                if (this.immediate) {
                    ListenerConsumer.this.consumer.wakeup();
                }
            }

            @Override
            public String toString() {
                return "Acknowledgment for " + this.record;
            }

        }

    }

    protected final ConsumerRebalanceListener createConsumerRebalanceListener() {
        return new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                logger.info("partitions revoked:" + partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                logger.info("partitions assigned:" + partitions);
            }

        };
    }
}
