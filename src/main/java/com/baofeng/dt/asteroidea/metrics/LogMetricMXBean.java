package com.baofeng.dt.asteroidea.metrics;

public interface LogMetricMXBean {
    
    public long getMessageCount();

    public long getQPS();

    public String getFlowSize();

    public String getTopicMeta();
}
