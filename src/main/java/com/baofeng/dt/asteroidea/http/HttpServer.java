package com.baofeng.dt.asteroidea.http;

import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.QueuedThreadPool;

import javax.servlet.http.HttpServlet;
import java.util.Map;
import java.util.Set;

/**
 * 
 * Description: HTTP Server<br>
 *
 * Copyright: Copyright (c) 2013 <br>
 * Company: www.renren.com
 * 
 * @author xianquan.zhang{xianquan.zhang@renren.inc.com} 2013-2-16  
 * @version 0.1
 */
public class HttpServer {

  private Server webServer=null;
  private WebAppContext webAppContext =null;
  private final String applicationHomeRoot = System.getProperty("user.dir");
  
  public HttpServer(String host, int port,String application) {
    webServer = new Server();
    Connector listener = createChannelConnector();
    listener.setHost(host);
    listener.setPort(port);

    webServer.addConnector(listener);
    webServer.setThreadPool(new QueuedThreadPool());

    ContextHandlerCollection contexts = new ContextHandlerCollection();
    webServer.setHandler(contexts);

    webAppContext = new WebAppContext();
    webAppContext.setDisplayName("WebAppsContext");
    webAppContext.setContextPath("/"); 
    webAppContext.setWar(applicationHomeRoot+"/"+application);
    webServer.addHandler(webAppContext);
    
  }

  public void start() throws Exception {
    webServer.start();
  }

  public void stop() throws Exception{
    webServer.stop();
  }
  
  private Connector createChannelConnector() {
    SelectChannelConnector scc = new SelectChannelConnector();
    scc.setLowResourceMaxIdleTime(10000);
    scc.setAcceptQueueSize(128);
    scc.setResolveNames(false);
    scc.setUseDirectBuffers(false);

    return scc;
  }
  /**
   * 添加一个参数
   * @param name
   * @param pathSpec
   * @param clazz
   * @param parameterName
   * @param parameterValue
   */
  public void addServlet(String name, String pathSpec, 
      Class<? extends HttpServlet> clazz,String parameterName,String parameterValue){
    ServletHolder holder = new ServletHolder(clazz);
    if ( null != name) {
      holder.setName(name);
    }
    if(null != parameterName && null != parameterValue){
      holder.setInitParameter(parameterName, parameterValue);
    }
    
    this.webAppContext.addServlet(holder, pathSpec);
  }
  /**
   * 添加多参数
   * @param name
   * @param pathSpec
   * @param clazz
   * @param params
   */
  public void addServlet(String name, String pathSpec, 
      Class<? extends HttpServlet> clazz,Map<String,String> params){
    ServletHolder holder = new ServletHolder(clazz);
    if ( null != name) {
      holder.setName(name);
    }
    if(null!=params){
      Set<Map.Entry<String,String>> set = params.entrySet();
      for(Map.Entry<String,String> me:set){
        holder.setInitParameter(me.getKey(),me.getValue());
      }
    }
    
    this.webAppContext.addServlet(holder, pathSpec);
  }
  /**
   * 无参数
   * @param name
   * @param pathSpec
   * @param clazz
   */
  public void addServlet(String name, String pathSpec, 
      Class<? extends HttpServlet> clazz){
    this.addServlet(name, pathSpec, clazz, null, null);
  }
  
  public void setAttribute(String name,Object value){
    this.webAppContext.setAttribute(name, value);
  }
}
