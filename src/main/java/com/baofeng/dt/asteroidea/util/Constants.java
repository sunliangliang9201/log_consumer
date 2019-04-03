package com.baofeng.dt.asteroidea.util;

/**
 * @author mignjia
 * @version 1.0
 * @Descriptions
 * @date 16/1/11 下午2:33
 */
public class Constants {
    /** zookeeper 连接重试时间间隔,默认时间是2s */
    public static final int DEFAULT_ZOOKEEPER_CONNECTION_RETRY_INTERVAL = 2000;

    /** zookeeper 连接重试时间间隔,默认时间是2s */
    public static final int DEFAULT_ZOOKEEPER_CONNECTION_TIME_OUT = 15000;

    /** topic备份文件路径 */
    public static final String TOPIC_FILE_BAK_PAHT = "topicfile.bakup.path";

    /** topic备份文件名称 */
    public static final String TOPIC_FILE_BAK_NAME = "topics-bak.xml";


    /** kafka-manager zk地址列表 */
    public static final String KM_ZOOKEEPER_QUORUM = "kafka-manager.zookeeper.address";

    /** zk超时时间 */
    public static final String ZOOKEEPER_TIMEOUT = "zookeeper.session.timeout.ms";

    /** ZK超时时间，默认是15秒,单位：秒 */
    public static final int    ZOOKEEPER_TIMEOUT_DEFAULT = 15;


}
