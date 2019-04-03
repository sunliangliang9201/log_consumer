package com.baofeng.dt.asteroidea.comsumer;


import com.baofeng.dt.asteroidea.handler.MetaWatcher;
import com.baofeng.dt.asteroidea.handler.TopicMetaManager;
import org.apache.hadoop.conf.Configuration;


public interface LogConsumer extends MetaWatcher {

    public void initialize(Configuration conf,  TopicMetaManager tm);
    
    public void start();
    
    public void close();
}
