package com.zookeeper.practice.leader;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * curator框架进行leader选举的demo
 */
@Slf4j
@Component
public class CuratorLeaderDemo {

    @Autowired
    private CuratorFramework client;

    public void leader(String id) {
        try {
            LeaderLatch leaderLatch = new LeaderLatch(client, "/leader", id);
            leaderLatch.addListener(new LeaderLatchListener() {
                @Override
                public void isLeader() {
                    log.info("isLeader");
                }

                @Override
                public void notLeader() {
                    log.info("notLeader");
                }
            });
            leaderLatch.start();
            for (int i = 0; i < 60; i++) {
                TimeUnit.SECONDS.sleep(5);
                log.info("node is leader {}", leaderLatch.hasLeadership());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
