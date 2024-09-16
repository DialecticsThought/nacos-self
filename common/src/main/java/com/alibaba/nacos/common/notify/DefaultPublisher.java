/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.common.notify;

import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.alibaba.nacos.common.notify.NotifyCenter.ringBufferSize;

/**
 * The default event publisher implementation.
 *
 * <p>Internally, use {@link ArrayBlockingQueue <Event/>} as a message staging queue.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 * @author zongtanghu
 */
public class DefaultPublisher extends Thread implements EventPublisher {

    protected static final Logger LOGGER = LoggerFactory.getLogger(NotifyCenter.class);
    /**
     * 标识事件发布者是否初始化的状态位
     */
    private volatile boolean initialized = false;

    /**
     * 标识事件发布者是否关闭的状态位
     */
    private volatile boolean shutdown = false;


    /**
     * 事件类型，其非集合，说明一个线程只能绑定一种事件类型
     */
    private Class<? extends Event> eventType;
    /**
     * 监听这种事件的订阅者集合
     */
    protected final ConcurrentHashSet<Subscriber> subscribers = new ConcurrentHashSet<>();
    /**
     * 阻塞队列大小
     */
    private int queueMaxSize = -1;
    /**
     * 阻塞队列
     */
    private BlockingQueue<Event> queue;
    /**
     * 最大的事件序号，事件每次产生都会有个事件序号
     */
    protected volatile Long lastEventSequence = -1L;

    private static final AtomicReferenceFieldUpdater<DefaultPublisher, Long> UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(DefaultPublisher.class, Long.class, "lastEventSequence");

    @Override
    public void init(Class<? extends Event> type, int bufferSize) {

        // 设置后台守护线程，当服务关闭的时候会自动关闭
        setDaemon(true);
        // 设置线程的名称，当我们打印JVM线程的时候可以快速查找到
        setName("nacos.publisher-" + type.getName());
        // 指定事件类型
        this.eventType = type;
        // 指定阻塞队列的大小
        this.queueMaxSize = bufferSize;
        if (this.queueMaxSize == -1) {
            this.queueMaxSize = ringBufferSize;
        }
        // 初始化阻塞队列
        this.queue = new ArrayBlockingQueue<>(this.queueMaxSize);
        // 启动线程
        start();
    }

    public ConcurrentHashSet<Subscriber> getSubscribers() {
        return subscribers;
    }

    @Override
    public synchronized void start() {
        // initialized变量使用volatile修饰，保证可见性，其它线程能及时发现initialized已经被改为true

        if (!initialized) {// 保证只启动一次，调用父类，告诉JVM去进行线程启动
            // start just called once
            // TODO 进入
            super.start();
            // 线程启动后，将initialized置为true，保证只会启动一次
            initialized = true;
        }
    }

    @Override
    public long currentEventSize() {
        return queue.size();
    }

    /**
     * TODO 重点
     */
    @Override
    public void run() {
        openEventHandler();
    }

    void openEventHandler() {
        try {

            // This variable is defined to resolve the problem which message overstock in the queue.
            int waitTimes = 60;
            // To ensure that messages are not lost, enable EventHandler when
            // waiting for the first Subscriber to register
            while (!shutdown && !hasSubscriber() && waitTimes > 0) {
                ThreadUtils.sleep(1000L);
                waitTimes--;
            }
            // shutdown初始值为false，执行一个死循环，一直运行在后台，直到在shutdown()方法中将shutdown置为true，才会停止
            // 当然，如果阻塞队列中没有任务的话，线程会被阻塞，阻塞队列的线程是不占用CPU资源的，也就是占了一部分空间和资源
            while (!shutdown) {

                // 阻塞队列中如果没有事件，这里进行阻塞
                final Event event = queue.take();
                // 获取到事件进行处理. 当阻塞队列中有数据的时候，就会立马触发获取到队列元素，开始处理
                // TODO 进入
                receiveEvent(event);
                UPDATER.compareAndSet(this, lastEventSequence, Math.max(lastEventSequence, event.sequence()));
            }
        } catch (Throwable ex) {
            LOGGER.error("Event listener exception : ", ex);
        }
    }

    private boolean hasSubscriber() {
        return CollectionUtils.isNotEmpty(subscribers);
    }

    @Override
    public void addSubscriber(Subscriber subscriber) {
        subscribers.add(subscriber);
    }

    @Override
    public void removeSubscriber(Subscriber subscriber) {
        subscribers.remove(subscriber);
    }

    @Override
    public boolean publish(Event event) {
        // 校验publisher是否初始化
        checkIsStart();
        // 将事件存入阻塞队列中，这样订阅者就可以从队列中取出任务执行
        boolean success = this.queue.offer(event);
        // 如果存入队列失败，则直接通知订阅者处理事件
        if (!success) {
            LOGGER.warn("Unable to plug in due to interruption, synchronize sending time, event : {}", event);
            receiveEvent(event);
            return true;
        }
        return true;
    }

    void checkIsStart() {
        if (!initialized) {
            throw new IllegalStateException("Publisher does not start");
        }
    }

    @Override
    public void shutdown() {
        this.shutdown = true;
        this.queue.clear();
    }

    public boolean isInitialized() {
        return initialized;
    }

    /**
     * Receive and notifySubscriber to process the event.
     * 接收事件,并通知订阅者处理事件
     * @param event {@link Event}.
     */
    void receiveEvent(Event event) {
        final long currentEventSequence = event.sequence();
        // 没有订阅者，则直接返回
        if (!hasSubscriber()) {
            LOGGER.warn("[NotifyCenter] the {} is lost, because there is no subscriber.", event);
            return;
        }

        // Notification single event listener
        for (Subscriber subscriber : subscribers) {
            // 判断事件是否匹配，跳过不匹配的那些订阅者，不进行处理
            if (!subscriber.scopeMatches(event)) {
                continue;
            }

            // Whether to ignore expiration events
            // 是否忽略过期的事件
            if (subscriber.ignoreExpireEvent() && lastEventSequence > currentEventSequence) {
                LOGGER.debug("[NotifyCenter] the {} is unacceptable to this subscriber, because had expire",
                        event.getClass());
                continue;
            }

            // Because unifying smartSubscriber and subscriber, so here need to think of compatibility.
            // Remove original judge part of codes.
            // TODO 进入
            notifySubscriber(subscriber, event);
        }
    }

    @Override
    public void notifySubscriber(final Subscriber subscriber, final Event event) {

        LOGGER.debug("[NotifyCenter] the {} will received by {}", event, subscriber);
        // 包装成一个任务去处理事件
        final Runnable job = () -> subscriber.onEvent(event);
        // 支持配置线程池，很灵活，让每个订阅者决定是同步调用还是异步调用
        final Executor executor = subscriber.executor();

        if (executor != null) {
            // 如果订阅者指定了线程池，将Runnable放入线程池，等待异步执行
            executor.execute(job);
        } else {
            try {
                // 直接调用方法，即同步调用
                job.run();
            } catch (Throwable e) {
                LOGGER.error("Event callback exception: ", e);
            }
        }
    }
}
