package com.zookeeper.practice;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.zookeeper.practice.demo.ZkClient;
import com.zookeeper.practice.demo.ZkDistributedLock;


@SpringBootTest
class PracticeApplicationTests {

    @Autowired
    private ZkClient zkClient;

    @Autowired
    private ZkDistributedLock zkDistributedLock;

    @Test
    void contextLoads() throws InterruptedException {
        new Thread(() -> {
            zkDistributedLock.acquire("/fifoPath/test");
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                zkDistributedLock.release();
            }
        }).start();

        TimeUnit.SECONDS.sleep(1);

        new Thread(() -> {
            zkDistributedLock.acquire("/fifoPath/test");
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                zkDistributedLock.release();
            }
        }).start();

        while (true) {}
    }

}
