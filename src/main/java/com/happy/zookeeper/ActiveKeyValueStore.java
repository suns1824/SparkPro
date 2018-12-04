package com.happy.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

public class ActiveKeyValueStore extends ConnectionWatcher {

    private static final Charset CHARSET = Charset.forName("UTF-8");

    private static final int MAX_RETRIES = 5;
    private static final int RETRY_PERIOD_SECONDS = 10;

    public void write(String path, String value) throws InterruptedException, KeeperException{
//        Stat stat  = zk.exists(path, false);
//        if (stat == null) {
//            zk.create(path, value.getBytes(CHARSET), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        } else {
//            zk.setData(path, value.getBytes(CHARSET), -1);
//        }
        //幂等操作-支持循环执行重试
        int retries = 0;
        while (true) {
            try {
                Stat stat = zk.exists(path, false);
                if (stat == null) {
                    zk.create(path, value.getBytes(CHARSET), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } else {
                    zk.setData(path, value.getBytes(CHARSET), stat.getVersion());
                }
                return;
            } catch (KeeperException.SessionExpiredException e) {
                //会话过期zk对象直接进入到closed状态，它无法进行重新连接
                throw e;
            } catch (InterruptedException e) {
                if (retries ++ == MAX_RETRIES) {
                    throw e;
                }
                //sleep then retry
                TimeUnit.SECONDS.sleep(RETRY_PERIOD_SECONDS);
            }
        }
    }

    public String read(String path, Watcher watcher) throws InterruptedException, KeeperException{
        byte[] data = zk.getData(path, watcher, null);
        return new String(data, CHARSET);
    }
}
