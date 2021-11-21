package com.tbank.bdp.sink.common;

import java.util.concurrent.ThreadFactory;

/**
 * @author hongshen
 * @date 2020/12/24
 */
public class ChThreadFactory implements ThreadFactory {

    final ThreadGroup group;
    final String namePrefix;
    final int index;

    public ChThreadFactory(String namePrefix, int index) {
        this.namePrefix = namePrefix;
        this.index = index;
        SecurityManager s = System.getSecurityManager();
        this.group = s != null ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
    }

    public Thread newThread(Runnable r) {
        Thread t = new Thread(this.group, r, this.namePrefix + "[T#" + index + "]", 0L);
        t.setDaemon(true);
        return t;
    }
}
