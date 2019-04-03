package com.baofeng.dt.asteroidea.handler;

import com.baofeng.dt.asteroidea.comparators.ComparatorTopicMeta;
import com.baofeng.dt.asteroidea.db.dao.MySQLDao;
import com.baofeng.dt.asteroidea.exception.InitializationException;
import com.baofeng.dt.asteroidea.model.TopicMeta;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author mignjia
 * @date 17/3/16
 */
public class TopicMetaManager {

    private static final Logger LOG = LoggerFactory.getLogger(TopicMetaManager.class);

    private static final int DEFAULT_META_RELOAD_INTERVAL = 60;
    private int reloadInterval;
    private Thread reloadThread;
    private List<MetaWatcher> watchers = new ArrayList<MetaWatcher>();
    private List<TopicMeta> topicMetas;
    private volatile boolean isRunning = true;


    public void initialize(Configuration conf){
        List<TopicMeta> initTopicMetas = getMetaFromDB();

        if(null == initTopicMetas){
            throw new InitializationException("Error to load meata data from db!");
        }

        topicMetas = filterMeta(initTopicMetas);

        if(topicMetas.isEmpty()){
            LOG.warn("No meta data found during TopicMetaManager init.");
        }

        reloadInterval = conf.getInt("consumer.meta.reload.interval", DEFAULT_META_RELOAD_INTERVAL);
        reloadThread = new Thread(){
            public void run() {
                while (isRunning) {
                    try {
                        Thread.sleep(reloadInterval * 1000);
                    } catch (InterruptedException e) {
                        LOG.error("Fail to sleep!", e);
                    }
                    reloadMetaData();
                }
            }
        };
    }

    public  List<TopicMeta> getTopicMetas(){
        return topicMetas;
    }

    public void start(){
        reloadThread.setName("TopicManager-reload-thread-%d");
        reloadThread.setDaemon(true);
        reloadThread.start();
        LOG.info("Start topic manager reload thread.");
    }

    private void reloadMetaData(){
        List<TopicMeta> newTopicMetas = getMetaFromDB();
        if(null == newTopicMetas){
            LOG.error("Error to load meta data from db,skip reload!");
            return;
        }

        newTopicMetas = filterMeta(newTopicMetas);

        if(checkModified(topicMetas, newTopicMetas)){
            LOG.info("Topic meta has been modified, reload consumer threads!");
            synchronized (topicMetas){
                topicMetas = newTopicMetas;
            }
            notifyWatchers(topicMetas);
        }


    }

    public boolean checkModified(List<TopicMeta> oldMetas, List<TopicMeta> newMetas){
        if(oldMetas.size() != newMetas.size())
            return true;
        Collections.sort(oldMetas, new ComparatorTopicMeta());
        Collections.sort(newMetas, new ComparatorTopicMeta());
        for(int i=0;i<oldMetas.size();i++){
            if(!oldMetas.get(i).equals(newMetas.get(i)))
                return true;
        }
        return false;

    }


    private void notifyWatchers(List<TopicMeta> topicMetaList){
        for (MetaWatcher watcher : watchers)
        {
            try {
                watcher.update(topicMetaList);
            }catch (Exception e){
                LOG.error("Fail to update watcher:",e);
            }

        }

    }


    public void addWatcher(MetaWatcher watcher){
        watchers.add(watcher);
    }

    private List<TopicMeta> getMetaFromDB(){
        return MySQLDao.dao.getMeta();
    }


    private List<TopicMeta> filterMeta(List<TopicMeta> topicMetas){
        List<TopicMeta> metaMap = new ArrayList<TopicMeta>();

        //todo filter error meta data.

        return  topicMetas;
    }

    public void close(){
        isRunning = false;
        if(null != reloadThread) {
            reloadThread.interrupt();
        }
    }


}
