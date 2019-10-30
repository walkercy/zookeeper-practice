package com.zookeeper.practice;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.zookeeper.practice.leader.CuratorLeaderDemo;

@SpringBootTest
public class LeaderLatchTest {

    @Autowired
    private CuratorLeaderDemo leaderDemo;

    @Test
    public void leaderTest() throws InterruptedException {
        // 三个线程启动，模拟集群选举
        new Thread(() -> {
            leaderDemo.leader("node1");
        }, "client-node1").start();

        new Thread(() -> {
            leaderDemo.leader("node2");
        }, "client-node2").start();

        new Thread(() -> {
            leaderDemo.leader("node3");
        }, "client-node3").start();

        TimeUnit.SECONDS.sleep(60);
    }

}
