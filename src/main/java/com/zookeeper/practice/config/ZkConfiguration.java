package com.zookeeper.practice.config;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ZkConfiguration {

    @Autowired
    private ZkProperties zkProperties;

    /**
     * 创建zookeeper客户端，指定初始化方法start，初始化bean时执行start方法启动客户端
     * @return
     */
    @Bean(initMethod = "start")
    public CuratorFramework curatorFramework() {
        return CuratorFrameworkFactory.builder()
                .connectString(zkProperties.getConnectString())
                .connectionTimeoutMs(zkProperties.getConnectionTimeoutMs())
                .sessionTimeoutMs(zkProperties.getSessionTimeoutMs())
                .retryPolicy(new RetryNTimes(zkProperties.getRetryCount(), zkProperties.getElapsedTimeMs()))
                .namespace("walker")
                .build();
    }

}
