package com.zookeeper.practice.demo;

import java.util.concurrent.CountDownLatch;

import javax.swing.plaf.IconUIResource;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * 利用zk同一路径下不能创建同名节点的特性
 * 结合jdk的线程屏障技术，实现分布式锁
 */
@Slf4j
@Component
public class ZkClient implements InitializingBean {

    private static final String ROOT_LOCK_PATH = "/rootLock";

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    @Autowired
    private CuratorFramework curatorFramework;

    public void acquireDistributedLock(String path) {
        path = ROOT_LOCK_PATH + "/" + path;
        while (true) {
            try {
                // 同一路径下，不能有相同名称的zNode
                curatorFramework.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.EPHEMERAL)
                        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                        .forPath(path);
                log.info("success to acquire lock for path : {}", path);
                break;
            } catch (Exception e) {
                log.info("failed to acquire lock for path : {}", path);
                // 获取锁失败，阻塞，等待其他线程通知
                try {
                    if (countDownLatch.getCount() <= 0) {
                        countDownLatch = new CountDownLatch(1);
                    }
                    countDownLatch.await();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    public boolean releaseDistributedLock(String path) {
        path = ROOT_LOCK_PATH + "/" + path;
        try {
            if (curatorFramework.checkExists().forPath(path) != null) {
                curatorFramework.delete().forPath(path);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private void addWatcher(String path) throws Exception {
        String keyPath = ROOT_LOCK_PATH.equals(path) ? path : ROOT_LOCK_PATH + "/" + path;
        PathChildrenCache childrenCache = new PathChildrenCache(curatorFramework, keyPath, false);
        childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        childrenCache.getListenable().addListener(((client, event) -> {
            // 子节点移除事件
            if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                String oldPath = event.getData().getPath();
                log.info("znode {} 已经被删除", oldPath);
                if (oldPath.contains(path)) {
                    countDownLatch.countDown();
                }
            }
        }));
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (curatorFramework.checkExists().forPath(ROOT_LOCK_PATH) == null) {
            curatorFramework.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath(ROOT_LOCK_PATH);
        }
        addWatcher(ROOT_LOCK_PATH);
    }

}
