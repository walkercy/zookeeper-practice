package com.zookeeper.practice;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.zookeeper.practice.demo.ZkDistributedQueue;

@SpringBootTest
public class DistributedQueueTest {

    @Autowired
    private CuratorFramework curatorFramework;

    @Test
    void testQueue() throws InterruptedException {
        String path = "test";
        ZkDistributedQueue<String> queue = new ZkDistributedQueue<>(curatorFramework, 10);
        new Thread(() -> {
            queue.take();
            queue.put(path, "walkerzk");
            System.err.println(queue.take());
        }).start();
        TimeUnit.SECONDS.sleep(3);
        new Thread(() -> {
            queue.put(path, "lalla");
            queue.put(path, "lalla2");
        }).start();
        while (true) {

        }
    }

}
