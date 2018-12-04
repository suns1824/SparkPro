package com.happy.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import scala.tools.nsc.backend.jvm.BTypes;

import java.io.IOException;

/**
 * 服务的用户
 */
public class ConfigWatcher implements Watcher {

    private ActiveKeyValueStore store;

    public ConfigWatcher(String hosts) throws IOException, InterruptedException{
        store = new ActiveKeyValueStore();
        store.connect(hosts);
    }

    public void displayConfig() throws InterruptedException, KeeperException {
        String value = store.read(ConfigUpdater.PATH, this);
        System.out.printf("Read %s as %s\n", ConfigUpdater.PATH, value);
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeDataChanged) {
            try {
                displayConfig();
            } catch (InterruptedException e) {
                //print
                Thread.currentThread().interrupt();
            } catch (KeeperException e) {
                //print
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ConfigWatcher watcher = new ConfigWatcher(args[0]);
        watcher.displayConfig();
        //stay alive until process is killed or thread is interrupted
        Thread.sleep(Long.MAX_VALUE);
    }
}
