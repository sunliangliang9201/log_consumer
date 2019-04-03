package com.baofeng.dt.asteroidea.db;



import com.baofeng.dt.asteroidea.exception.DBInitException;
import com.baofeng.dt.asteroidea.exception.DBNotFoundException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 数据库操作模板类
 * <p/>
 * 基本使用:
 * DB.get().save(bean);
 * <p/>
 * 也可以自定义DB:
 * DB.initDBConnection("mydb", driver, url, username, passWord);
 * DB.get("mydb").save(bean);
 * <p/>
 * TODO DB异常处理
 *
 * @author zhuhui
 * @date 2015-8-17
 */
public class DB {
    private static final Logger LOG = LoggerFactory.getLogger(DB.class);
    private static final String DB_DEFAULT_KEY = "default_db";
    private static final Map<String, DBUtilsTemplate> DB_MAP = new HashMap<String, DBUtilsTemplate>();
    private static final Map<String, DBConfig> DB_PARAM_MAP = new HashMap<String, DBConfig>();

    public static void initDBConnection(String key, String driver, String url, String username, String passWord) {
        DBUtilsTemplate db = new DBUtilsTemplate(driver, url, username, passWord);
        if (db.checkConnection()) {
            DB_MAP.put(key, db);
            DB_PARAM_MAP.put(key, new DBConfig(driver, url, username, passWord));
            LOG.info("success to init db: {}", url);
        } else {
            throw new DBInitException("fail to init db:" + url);
        }
    }

    public static void initDBConnection(String driver, String url, String username, String passWord) {
        initDBConnection(DB_DEFAULT_KEY, driver, url, username, passWord);
    }

    public static DBUtilsTemplate get(String key) {
        DBUtilsTemplate db = DB_MAP.get(key);
        if (db == null) {
            throw new DBNotFoundException(key + " is not found");
        }
        if(!db.checkConnection()){
            initDBConnection(key, DB_PARAM_MAP.get(key).getDriver(), DB_PARAM_MAP.get(key).getUrl(),
                    DB_PARAM_MAP.get(key).getUsername(), DB_PARAM_MAP.get(key).getPassWord());
        }

        return db;
    }

    public static boolean containsDB(String key){
        if(StringUtils.isBlank(key)){
            throw new DBNotFoundException("key is must needed");
        }
        return DB_MAP.containsKey(key);
    }

    public static DBUtilsTemplate get() {
        return get(DB_DEFAULT_KEY);
    }

    public static void closeAll() {
        for (String key : DB_MAP.keySet()) {
            try {
                DB_MAP.get(key).close();
            } catch (Exception e) {
                LOG.error("fail to close db[" + key + "] connection.");
            }
        }
    }
}
