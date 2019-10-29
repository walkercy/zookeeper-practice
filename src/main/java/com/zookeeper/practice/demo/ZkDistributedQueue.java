package com.zookeeper.practice.demo;

import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.zookeeper.CreateMode;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

/**
 * zk实现分布式队列
 */
@Slf4j
public class ZkDistributedQueue<T> {

    private CuratorFramework client;

    /**
     * 队列元素个数
     */
    private int size;

    /**
     * 队列容量
     */
    private int capacity;

    private ReentrantLock mainLock;

    private Condition notEmpty;

    private Condition notFull;

    private ObjectMapper mapper;

    private String rootPath = "/queue";

    public ZkDistributedQueue(CuratorFramework client, int capacity) {
        this.client = client;
        this.capacity = capacity;
        this.mainLock = new ReentrantLock();
        this.notEmpty = mainLock.newCondition();
        this.notFull = mainLock.newCondition();
        this.mapper = new ObjectMapper();

        try {
            // 创建根节点
            if (client.checkExists().forPath(rootPath) == null) {
                client.create().withMode(CreateMode.PERSISTENT).forPath(rootPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void put(String path, T element) {
        if (client.getState() == CuratorFrameworkState.STARTED) {
            mainLock.lock();
            try {
                // 队列满
                if (size == capacity) {
                    log.info("队列已满，阻塞线程");
                    notFull.await();
                }
                byte[] data = mapper.writeValueAsBytes(element);
                path = rootPath + "/" + path;
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                        .forPath(path, data);
                size++;
                notEmpty.signal();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                mainLock.unlock();
            }
        }
    }

    public T take() {
        mainLock.lock();
        try {
            if (size == 0) {
                log.info("队列为空，阻塞线程");
                notEmpty.await();
            }
            List<String> children = client.getChildren().forPath(rootPath);
            // 获取第一个节点
            String pathToDelete = rootPath + "/" + children.get(0);
            byte[] data = client.getData().forPath(pathToDelete);
            T result = (T) mapper.readValue(data, Object.class);
            client.delete().guaranteed().deletingChildrenIfNeeded().forPath(pathToDelete);
            size--;
            notFull.signal();
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            mainLock.unlock();
        }
    }

    public int size() {
        return size;
    }

}
