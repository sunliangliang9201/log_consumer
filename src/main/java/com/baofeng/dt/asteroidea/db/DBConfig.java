package com.baofeng.dt.asteroidea.db;

/**
 * @author mignjia
 * @date 16/11/7
 */
public class DBConfig {

    private String driver;

    private String url;

    private String username;

    private String passWord;

    public DBConfig(String driver, String url, String username, String passWord) {
        this.driver = driver;
        this.url = url;
        this.username = username;
        this.passWord = passWord;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassWord() {
        return passWord;
    }

    public void setPassWord(String passWord) {
        this.passWord = passWord;
    }
}
