package com.meituan.ads.ado.common.concurrency.impl;

import com.meituan.ads.ado.common.concurrency.QueueCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.meituan.ads.ado.common.concurrency.impl.QueueCoordinatorImpl.ActionEnum.ACCEPT;
import static com.meituan.ads.ado.common.concurrency.impl.QueueCoordinatorImpl.ActionEnum.BLOCK;
import static com.meituan.ads.ado.common.concurrency.impl.QueueCoordinatorImpl.ActionEnum.DECLINE;


/**
 * Created by liuding on 5/11/16.
 */
public class QueueCoordinatorImpl<T> implements QueueCoordinator<T> {
    private static final long NAP_TIME_IN_MILL = 10;
    private static final long STALE_TIMEOUT_IN_MILL = 20000; //20S
    private static final long DEFAULT_TIMEOUT = 3000; //3S
    private static final Long INIT_QUEUE_ID = Long.valueOf(-1);

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static Stastic stastic = new Stastic();
    private Long lastLoggingTime = System.currentTimeMillis();
    private ConcurrentHashMap<T, TrafficAutomate> key2Status = new ConcurrentHashMap();

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    private long timeout=DEFAULT_TIMEOUT;


    private static class Stastic
    {
        AtomicLong enQueueCount = new AtomicLong();
        AtomicLong staleCount = new AtomicLong();
        AtomicLong declineCount = new AtomicLong();
        AtomicLong acceptCount = new AtomicLong();
        AtomicLong blockCount = new AtomicLong();
        AtomicLong evictCount = new AtomicLong();

        @Override
        public String toString() {
            return "Stastic{" +
                    "enQueueCount=" + enQueueCount.get() +
                    ", staleCount=" + staleCount.get() +
                    ", declineCount=" + declineCount.get() +
                    ", acceptCount=" + acceptCount.get() +
                    ", blockCount=" + blockCount.get() +
                    ", evictCount=" + evictCount.get() +
                    '}';
        }
    }


    static enum ActionEnum
    {
        ACCEPT,
        BLOCK,
        DECLINE;
    }

    private static class TrafficAutomate
    {
        long head = INIT_QUEUE_ID;
        Date headCreationTime;
        long tail = INIT_QUEUE_ID;
        Date tailCreationTime;

        synchronized ActionEnum enQueue(long id)
        {
            stastic.enQueueCount.incrementAndGet();

            cleanStaleStatus(id);
            if(tail != INIT_QUEUE_ID)
            {
                stastic.declineCount.incrementAndGet();
                return DECLINE;
            }

            if(head == INIT_QUEUE_ID)
            {
                stastic.acceptCount.incrementAndGet();
                head = id;
                headCreationTime = new Date();
                return ACCEPT;
            }
            else
            {
                stastic.blockCount.incrementAndGet();
                tail = id;
                tailCreationTime = new Date();
                return BLOCK;
            }
        }

        synchronized  void evictHeadByForce(long id)
        {
            if(id == tail)
            {
                stastic.evictCount.incrementAndGet();
                head = tail;
                headCreationTime = tailCreationTime;

                tail = INIT_QUEUE_ID;
                tailCreationTime = null;
            }
            //otherwise current id should have been deQueue by other thread
        }

        synchronized boolean deQueue(long id)
        {
            if(id != head)
            {
                return false;
            }

            head = tail;
            headCreationTime = tailCreationTime;

            tail = INIT_QUEUE_ID;
            tailCreationTime = null;
            return true;
        }

        private synchronized void cleanStaleStatus(long id)
        {
            if(head == id)
            {
                //
                stastic.staleCount.incrementAndGet();
                deQueue(id);
            }

            long currentTime = System.currentTimeMillis();
            for(int i = 0; i < 2; i++)
            {
                if(head != INIT_QUEUE_ID && (currentTime-headCreationTime.getTime()) > STALE_TIMEOUT_IN_MILL)
                {
                    stastic.staleCount.incrementAndGet();
                    deQueue(head);
                }
            }
        }

        synchronized boolean isHead(long id)
        {
            return (head == id);
        }
    }


    private void _nonExceptionSleep(long millSeconds)
    {
        try {
            Thread.sleep(millSeconds);
        } catch (InterruptedException e) {
            logger.error("exception when sleep", e);
        }
    }

    private void _loggingStastic()
    {
        long curTime = System.currentTimeMillis();
        if((curTime-lastLoggingTime) > 10*1000)
        {
            lastLoggingTime = curTime;
            logger.info(stastic.toString());
        }
    }

    @Override
    public boolean enQueue(T key) {
        _loggingStastic();

        TrafficAutomate trafficAutomate = key2Status.get(key);
        if(trafficAutomate == null)
        {
            trafficAutomate = new TrafficAutomate();
            TrafficAutomate oldAutomate = key2Status.putIfAbsent(key, trafficAutomate);
            if(oldAutomate != null)
            {
                trafficAutomate = oldAutomate;
            }
        }
        long threadId = Thread.currentThread().getId();

        ActionEnum action = trafficAutomate.enQueue(threadId);

        if(action == DECLINE)
        {
            return false;
        }
        else if (action == ACCEPT)
        {
            return true;
        }
        //Blocking status means some other thread are working on this key, so please wait till timeout
        long start = System.currentTimeMillis();
        long span = 0;
        do {
            _nonExceptionSleep(NAP_TIME_IN_MILL);

            if(trafficAutomate.isHead(threadId))
            {
                return true;
            }

            span = System.currentTimeMillis() - start;
        }while(span <= timeout);

        //remove head so that it won't block the queue for too long
        trafficAutomate.evictHeadByForce(threadId);

        return true;
    }

    @Override
    public void deQueue(T key) {
        TrafficAutomate trafficAutomate = key2Status.get(key);

        if(trafficAutomate == null)
        {
            logger.error("key {} doesn't exist ", key);
            return;
        }

        long threadId = Thread.currentThread().getId();

        if(!trafficAutomate.deQueue(threadId))
        {
            logger.error("key {} was evicted by other thread ", key);
        }
    }
}
