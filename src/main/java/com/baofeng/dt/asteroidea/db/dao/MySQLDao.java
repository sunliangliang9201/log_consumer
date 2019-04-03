package com.baofeng.dt.asteroidea.db.dao;

import com.baofeng.dt.asteroidea.db.DB;
import com.baofeng.dt.asteroidea.model.TopicMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * @author mignjia
 * @date 17/3/17
 */
public class MySQLDao {

    private static final Logger LOG = LoggerFactory.getLogger(MySQLDao.class);

    public static final MySQLDao dao = new MySQLDao();

    private String driver;
    private String url;
    private String username;
    private String passWord;

    private static final String TOPIC_META_SQL = "SELECT id,topics,groupID,numThreads,fileType,codeC,filePath,filePathFormat,filePathMode,fileSuffix,isWriteLineBreaks" +
            " FROM log_consumer_config WHERE isEnabled=1";


    public void initialize(String driver, String url, String username, String passWord){
        this.driver = driver;
        this.url = url;
        this.username = username;
        this.passWord = passWord;

        DB.initDBConnection(driver, url, username, passWord);
    }

    private void checkConn(){
        if(!DB.get().checkConnection()){
            DB.initDBConnection(driver, url, username, passWord);
        }
    }

    public List<TopicMeta> getMeta(){
        checkConn();
        List<TopicMeta> topicMetas;

        try {
            topicMetas = DB.get().find(TopicMeta.class, TOPIC_META_SQL);
        } catch (SQLException e) {
            LOG.error("Error occured while attempting to query data", e);
            return null;
        }
        return topicMetas;
    }
}
