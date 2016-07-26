package com.meituan.ads.ado.common.concurrency;

/**
 * Created by liuding on 5/11/16.
 */
public interface QueueCoordinator<T> {

    public boolean enQueue(T key);

    public void deQueue(T key);

}
