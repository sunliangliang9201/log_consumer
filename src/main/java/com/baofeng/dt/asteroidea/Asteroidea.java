package com.baofeng.dt.asteroidea;

import com.baofeng.dt.asteroidea.comsumer.LogConsumer;
import com.baofeng.dt.asteroidea.db.dao.MySQLDao;
import com.baofeng.dt.asteroidea.handler.TopicMetaManager;
import com.baofeng.dt.asteroidea.http.HttpServer;
import com.baofeng.dt.asteroidea.http.JMXJsonServlet;
import com.baofeng.dt.asteroidea.util.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.sql.SQLException;

public class Asteroidea {

    private LogConsumer logConsumer=null;
//    private StorageAdapterManager sam=null;
    private String configFileName;
    private HttpServer httpServer=null;
    private TopicMetaManager tm =null;


    public static final Logger LOG =  LoggerFactory.getLogger(Asteroidea.class);

    private Asteroidea(String configFileName){
        this.configFileName = configFileName;
    }

    public static Asteroidea createBootstrap(String configFileName) throws IOException, ReflectiveOperationException, SQLException{
        Asteroidea boot=new Asteroidea(configFileName);
        boot.initialize();
        return boot;
    }
    /**
     * 初始化相关类
     * @throws IOException 
     * @throws SQLException 
     * @throws ReflectiveOperationException 
     */
    public void initialize() throws IOException, ReflectiveOperationException, SQLException {
        Configuration conf = new Configuration();
        conf.addResource(configFileName);


        MySQLDao.dao.initialize(conf.get("consumer.jdbc.drive"),
                conf.get("consumer.jdbc.url"),
                conf.get("consumer.jdbc.username"),
                conf.get("consumer.jdbc.password"));

        tm = new TopicMetaManager();
        tm.initialize(conf);

        logConsumer = (LogConsumer)ReflectionUtils.newInstance(conf.get("log.consumer.impl"));
        logConsumer.initialize(conf, tm);

        tm.addWatcher(logConsumer);

        httpServer=new HttpServer(conf.get("server.host", "0.0.0.0"),conf.getInt("http.port", 18080),"webapps");
        httpServer.addServlet("jmx","/jmx", JMXJsonServlet.class);

    }

    public void start() throws Exception {
    	LOG.info("Main thread : " + Thread.currentThread().getName());
        /**
         * 添加钩子程序，关闭进程时，处理善后工作，例如刷清数据流。
         */
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
              LOG.info("SHUT_DOWN_MSG : Shutting down log-consumer process! Thread name : " + this.getName());
              close();
            }
        });
        tm.start();
        logConsumer.start();
        httpServer.start();

    }

    public void close() {
        if(tm != null){
            tm.close();
        }
        if (logConsumer != null) {
            logConsumer.close();
        }

        if (httpServer!=null) {
            try {
                httpServer.stop();
            } catch (Exception e) {
               LOG.error("Fail to stop web server!",e);
            }
        }

    }
    /**
     * @param args
     * @throws SQLException 
     * @throws ReflectiveOperationException 
     */
    public static void main(String[] args) throws ReflectiveOperationException, SQLException {
        Asteroidea boot=null;
        if(args.length != 1){
            System.out.println("It must have 1 parameters for log-consumer config file!");
            System.exit(0);
        }
        try {
            boot = createBootstrap(args[0]);
        } catch (IOException e) {
            LOG.error("Fail to initialize,please check config file.",e);
            System.exit(-1);
        }
        try {
            boot.start();
        } catch (Exception e) {
            LOG.error("Fail to start log consumer!",e);
            System.exit(-1);
        }
    }
}
