package com.baofeng.dt.asteroidea.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.lang.management.ManagementFactory;

public class MBeanUtils {
    private static final Logger LOG =  LoggerFactory.getLogger(MBeanUtils.class);
    
    /**
     * 
     * @param serviceName 服务名称
     * @param nameName mbean名称
     * @param theMbean mbean对象
     * @return
     */
    public static ObjectName register(String serviceName, String nameName,
        Object theMbean) {
        final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = getMBeanName(serviceName, nameName);
        try {
            mbs.registerMBean(theMbean, name);
            return name;
        } catch (InstanceAlreadyExistsException ie) {
            LOG.warn("InstanceAlreadyExistsException",ie);
        } catch (Exception e) {
            LOG.warn("Error registering " + name, e);
        }
        return null;
    }

    public static void unregister(String serviceName, String nameName){
        final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = getMBeanName(serviceName, nameName);
        try {
            mbs.unregisterMBean(name);
        } catch (InstanceNotFoundException e) {
            LOG.warn("InstanceNotFoundException", e);
        } catch (MBeanRegistrationException e) {
            LOG.warn("MBeanRegistrationException", e);
        }
    }
    
    private static ObjectName getMBeanName(String serviceName, String nameName) {
        ObjectName name = null;
        String nameStr = "LogConsumer:service="+ serviceName +",name="+ nameName;
        try {
          name = new ObjectName(nameStr);
        } catch (MalformedObjectNameException e) {
          LOG.warn("Error creating MBean object name: "+ nameStr, e);
        }
        return name;
      }
}
