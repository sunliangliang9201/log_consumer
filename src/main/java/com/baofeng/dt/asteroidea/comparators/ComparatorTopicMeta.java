package com.baofeng.dt.asteroidea.comparators;

import com.baofeng.dt.asteroidea.model.TopicMeta;

import java.util.Comparator;

/**
 * @author mignjia
 * @date 17/3/20
 */
public class ComparatorTopicMeta implements Comparator<TopicMeta> {
    @Override
    public int compare(TopicMeta o1, TopicMeta o2) {
        long orderNo1 = o1.getId();
        long orderNo2 = o2.getId();
        if (orderNo1 == orderNo2){
            return 0;
        }else {
            return orderNo1 > orderNo2 ? 1 : -1;
        }
    }
}
