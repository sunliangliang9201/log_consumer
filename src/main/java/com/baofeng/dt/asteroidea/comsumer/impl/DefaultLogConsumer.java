package com.baofeng.dt.asteroidea.comsumer.impl;

import com.baofeng.dt.asteroidea.comsumer.LogConsumer;
import com.baofeng.dt.asteroidea.handler.DefaultKafkaConsumerFactory;
import com.baofeng.dt.asteroidea.handler.ManualCommitKafkaHandler;
import com.baofeng.dt.asteroidea.handler.MessageListener;
import com.baofeng.dt.asteroidea.handler.TopicMetaManager;
import com.baofeng.dt.asteroidea.model.TopicMeta;
import com.baofeng.dt.asteroidea.storage.HDFSStorage;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @Descriptions The class LogConsumer.java's implementation：具体的log消费者
 * @author mingjia
 */
public class DefaultLogConsumer extends Thread implements LogConsumer {

    /**
     * key:队列名称，value:消费这个主题的线程列表
     */
    private Map<Long, TaskHandler> topicThreadMap;
    private TopicMetaManager tm;
    private List<TopicMeta> topicMetas;
    private Configuration conf;
    private int defaultHdfsIORetry = 3;
    private static final String LINE_BREAK = System.lineSeparator();
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLogConsumer.class);

    public void initialize(Configuration conf, TopicMetaManager tm) {
        this.tm=tm;
        this.conf = conf;
        topicThreadMap=new ConcurrentHashMap<Long, TaskHandler>();

        List<TopicMeta> topicMetas=this.tm.getTopicMetas();
        this.topicMetas = topicMetas;
        LOG.info("initialize default consumer:"+ this.getName());
    }

    public void run() {
        createAllTopicsMessageStreams(this.conf ,this.topicMetas);
    }
    
    private void createAllTopicsMessageStreams(Configuration conf,List<TopicMeta> topicMetas){

        /**
         * 遍历所有的主题
         */
        for (final TopicMeta topicMeta : topicMetas) {
            TaskHandler taskHandler =  new TaskHandler(conf,topicMeta);
            taskHandler.init();
            taskHandler.start();
            this.topicThreadMap.put(topicMeta.getId(), taskHandler);
        }
    }

    @Override
    public void update(List<TopicMeta> topicMetaList) {
        List<TopicMeta> oldList= new ArrayList<TopicMeta>();
        oldList.addAll(topicMetas);
        this.topicMetas = topicMetaList;

        oldList.removeAll(topicMetaList);

        //删除修改删除的任务
        for(TopicMeta t : oldList){
            if(topicThreadMap.containsKey(t.getId())){
                topicThreadMap.get(t.getId()).close();
                topicThreadMap.remove(t.getId());
            }
        }

        //添加新任务
        for(TopicMeta t : topicMetaList){
            if(topicThreadMap.containsKey(t.getId())){
                continue;
            }
            TaskHandler taskHandler =  new TaskHandler(conf,t);
            taskHandler.init();
            taskHandler.start();
            this.topicThreadMap.put(t.getId(), taskHandler);
        }
    }

    private class TaskHandler extends Thread{

        private HDFSStorage hs=null;
        private TopicMeta topicMeta;
        private Configuration conf;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private ConsumerConnector consumer;
        private int hdfsIORetry;
        private ExecutorService executor;


        private TaskHandler(Configuration conf ,TopicMeta topicMeta){
            this.conf = conf;
            this.topicMeta = topicMeta;
            this.hdfsIORetry = conf.getInt("hdfs.write.retry", defaultHdfsIORetry);
            this.hs = new HDFSStorage();
        }

        public void init(){
            hs.init(conf, topicMeta);
            Properties props = new Properties();

            props.put("zookeeper.connect", conf.get("zookeeper.connect"));
            props.put("zookeeper.session.timeout.ms", conf.get("ezookeeper.session.timeout.ms","15000"));
            props.put("auto.commit.interval.ms", conf.get("auto.commit.interval.ms", "2000"));
            props.put("zookeeper.sync.time.ms",conf.get("zookeeper.sync.time.ms", "3000"));

            props.put("session.timeout.ms", conf.get("session.timeout.ms", "30000"));
            props.put("auto.offset.reset", conf.get("auto.offset.reset", "largest"));
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("group.id", topicMeta.getGroupID());
            consumer =kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

        }



        public void run() {
            LOG.info("Start thread:{}, with topic meta:{}", this.getName(), topicMeta.toString());
            if (null == hs) {
                LOG.error("Fail to get storage adpter for topicMeta : "
                        + topicMeta.toString() + ",It will not consume this topic.");
                return;
            }
            int isWriteLineBreak = topicMeta.getIsWriteLineBreaks();
            hs.start();
            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            topicCountMap.put(topicMeta.getTopics(), topicMeta.getNumThreads());
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topicMeta.getTopics());

            executor = Executors.newFixedThreadPool(topicMeta.getNumThreads());
            for (final KafkaStream stream : streams) {
                executor.submit(new Consumer(stream,isWriteLineBreak));
            }


        }
        
        public void close(){
            if (consumer != null) {
                consumer.shutdown();
            }
            if (executor != null) {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                        LOG.error("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                    }
                } catch (InterruptedException e) {
                    LOG.info("Interrupted during shutdown, exiting uncleanly");
                }
            }
            if(null!=hs){
                hs.close();
                hs=null;
            }
            super.interrupt();
            LOG.info("Stop thread:{}, with topic meta:{}",this.getName(),topicMeta.toString());
        }

        private class Consumer implements Runnable{
            private KafkaStream stream;
            private int isWriteLineBreak = 1;

            public Consumer(KafkaStream stream,int isWriteLineBreak){
                this.stream = stream;
                this.isWriteLineBreak = isWriteLineBreak;
            }


            @Override
            public void run() {
                ConsumerIterator<byte[], byte[]> it = this.stream.iterator();
                while (it.hasNext()){
                    if (null != hs) {
                        for (int i = 1; i <= hdfsIORetry; i++) {
                            try {
                                if (isWriteLineBreak == 1) {
                                    hs.doStore(new String(it.next().message()) + LINE_BREAK);
                                } else {
                                    hs.doStore(new String(it.next().message()));
                                }
                                break;
                            } catch (IOException e) {
                                if (i == hdfsIORetry) {
                                    LOG.error("Fail to write recoed to hdfs after {} tries!Drop record！", hdfsIORetry);
                                    break;
                                }
                                LOG.warn("Fail to store recode!", e);
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e1) {
                                    LOG.error("Fail to sleep store thread!", e);
                                }

                            }
                        }
                    }
                }
            }
        }

    }


    private void stopTopicThreads(){
        for (TaskHandler t :topicThreadMap.values()){
            t.close();
        }
        topicThreadMap.clear();
    }


    public void close(){
        stopTopicThreads();
        LOG.info("Stop Default consumer !");
        this.interrupt();

    }



}
