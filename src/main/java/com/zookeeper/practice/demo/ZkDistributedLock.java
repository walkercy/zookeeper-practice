package com.zookeeper.practice.demo;

import java.util.Collections;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * 利用zk临时顺序节点，以FIFO的方式
 * 实现分布式锁，类似于jdk中的AQS
 */
@Slf4j
@Component
public class ZkDistributedLock {

    private static final String FIFO_LOCK_PATH = "/fifoPath";

    private final byte[] lock = new byte[0];

    @Autowired
    private CuratorFramework curatorFramework;

    /**
     * 初始化方法创建持久化父节点
     * @throws Exception
     */
    @PostConstruct
    private void init() throws Exception {
        if (curatorFramework.checkExists().forPath(FIFO_LOCK_PATH) == null) {
            curatorFramework.create()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath(FIFO_LOCK_PATH);
        }
    }

    public void acquire(String path) {
        try {
            // 创建临时顺序节点
            String realPath = curatorFramework.create()
                    .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath(path);
            log.info("success to create zNode {}", realPath);
            boolean hasLock = false;
            while (curatorFramework.getState().equals(CuratorFrameworkState.STARTED) && !hasLock) {
                if (isMinNumZnode(realPath)) {
                    // 拿到锁，跳出循环
                    hasLock = true;
                } else {
                    // 不是最小的znode，阻塞
                    synchronized (lock) {
                        // 监听最小序号znode的删除事件
                        addDeletedWatcher();
                        log.info("path {} not the minimal znode, block it", realPath);
                        lock.wait();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void release() {
        try {
            List<String> childrenPath = curatorFramework.getChildren().forPath(FIFO_LOCK_PATH);
            String pathToWatch = FIFO_LOCK_PATH + "/" + childrenPath.get(0);
            curatorFramework.delete().guaranteed().forPath(pathToWatch);
            curatorFramework.newWatcherRemoveCuratorFramework().removeWatchers();
            log.info("znode {} release the lock", pathToWatch);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void addDeletedWatcher() throws Exception {
        List<String> childrenPath = curatorFramework.getChildren().forPath(FIFO_LOCK_PATH);
        String pathToWatch = FIFO_LOCK_PATH + "/" + childrenPath.get(0);
        log.info("add delete watch to path {}", pathToWatch);
        PathChildrenCache cache = new PathChildrenCache(curatorFramework, FIFO_LOCK_PATH, false);
        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        cache.getListenable().addListener(((client, event) -> {
            if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)
                    && pathToWatch.equals(event.getData().getPath())) {
                synchronized (lock) {
                    // 通知所有获取锁的线程
                    log.info("通知所有获取锁阻塞的线程");
                    lock.notifyAll();
                }
            }
        }));
    }

    private boolean isMinNumZnode(String path) throws Exception {
        List<String> childrenPath = curatorFramework.getChildren().forPath(FIFO_LOCK_PATH);
        Collections.sort(childrenPath);
        return path.contains(childrenPath.get(0));
    }

}
