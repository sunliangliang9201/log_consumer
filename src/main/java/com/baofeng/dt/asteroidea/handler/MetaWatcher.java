package com.baofeng.dt.asteroidea.handler;

import com.baofeng.dt.asteroidea.model.TopicMeta;

import java.util.List;

/**
 * @author mignjia
 * @date 17/3/17
 */
public interface MetaWatcher {

    public void update(List<TopicMeta> topicMetaList);
}
