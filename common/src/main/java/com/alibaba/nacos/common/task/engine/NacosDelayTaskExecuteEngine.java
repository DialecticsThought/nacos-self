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

package com.alibaba.nacos.common.task.engine;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.executor.ExecutorFactory;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.task.AbstractDelayTask;
import com.alibaba.nacos.common.task.NacosTaskProcessor;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Nacos delay task execute engine.
 *
 * @author xiweng.yy
 */
public class NacosDelayTaskExecuteEngine extends AbstractNacosTaskExecuteEngine<AbstractDelayTask> {
    /**
     * 定时任务线程池，在构造方法中初始化
     */
    private final ScheduledExecutorService processingExecutor;
    /**
     * 任务队列
     * key：对应的服务
     */
    protected final ConcurrentHashMap<Object, AbstractDelayTask> tasks;

    protected final ReentrantLock lock = new ReentrantLock();

    public NacosDelayTaskExecuteEngine(String name) {
        this(name, null);
    }

    public NacosDelayTaskExecuteEngine(String name, Logger logger) {
        this(name, 32, logger, 100L);
    }

    public NacosDelayTaskExecuteEngine(String name, int initCapacity, Logger logger, long processInterval) {
        super(logger);
        // 初始化任务队列
        tasks = new ConcurrentHashMap<>(initCapacity);
        // 创建定时任务的线程池  （单线程的线程池）
        processingExecutor = ExecutorFactory.newSingleScheduledExecutorService(new NameThreadFactory(name));
        // 在指定的初始延迟时间(100毫秒)后开始执行任务，并按固定的时间间隔周期性(100毫秒)地执行任务。
        // 默认延时100毫秒执行ProcessRunnable，然后每隔100毫秒周期性执行ProcessRunnable
        // TODO 查看 ProcessRunnable
        processingExecutor
                .scheduleWithFixedDelay(new ProcessRunnable(), processInterval, processInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public int size() {
        lock.lock();
        try {
            return tasks.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        lock.lock();
        try {
            return tasks.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public AbstractDelayTask removeTask(Object key) {
        lock.lock();
        try {
            // 根据key得到任务
            AbstractDelayTask task = tasks.get(key);
            if (null != task && task.shouldProcess()) {
                return tasks.remove(key);
            } else {
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Collection<Object> getAllTaskKeys() {
        Collection<Object> keys = new HashSet<>();
        lock.lock();
        try {
            // 把任务这个map的所有key拿到
            keys.addAll(tasks.keySet());
        } finally {
            lock.unlock();
        }
        return keys;
    }

    @Override
    public void shutdown() throws NacosException {
        tasks.clear();
        processingExecutor.shutdown();
    }

    @Override
    public void addTask(Object key, AbstractDelayTask newTask) {
        // 加锁防并发处理,key就是对应的服务
        lock.lock();
        try {
            // ConcurrentHashMap<Object, AbstractDelayTask> tasks = new ConcurrentHashMap<>(initCapacity);
            // 通过key判断是否已存在map中
            AbstractDelayTask existTask = tasks.get(key);
            if (null != existTask) {
                // 服务存在的话，则需要合并任务，其实就是合并多个任务，一起执行
                newTask.merge(existTask);
            }
            // 将任务放入到map中，等待处理
            tasks.put(key, newTask);
        } finally {
            lock.unlock();
        }
    }

    /**
     * process tasks in execute engine.
     */
    protected void processTasks() {
        // 得到所有任务的key
        // TODO 进入
        Collection<Object> keys = getAllTaskKeys();
        // 遍历所有任务的key
        for (Object taskKey : keys) {
            // TODO 进入
            // 该方法的核心:   AbstractDelayTask task = tasks.get(key);
            // 1.得到任务
            // 2.从队列中移除这个任务
            AbstractDelayTask task = removeTask(taskKey);
            if (null == task) {
                continue;
            }
            // taskKey示例值: Service{namespace='public', group='DEFAULT_GROUP', name='discovery-provider', ephemeral=true, revision=0}
            // 找到处理类
            // TODO 查看Processor 是子类PushDelayTaskExecuteEngine的内部类 PushDelayTaskProcessor
            // 如果是dump的任务 那么 就是DumpProcessor
            NacosTaskProcessor processor = getProcessor(taskKey);
            try {
                // ReAdd task if process failed
                if (!processor.process(task)) {
                    // 处理失败的话，重新入队（即重试）
                    retryFailedTask(taskKey, task);
                }
            } catch (Throwable e) {
                getEngineLog().error("Nacos task execute error ", e);
                retryFailedTask(taskKey, task);
            }
        }
    }

    private void retryFailedTask(Object key, AbstractDelayTask task) {
        task.setLastProcessTime(System.currentTimeMillis());
        addTask(key, task);
    }

    /**
     * 任务处理类
     */
    private class ProcessRunnable implements Runnable {

        @Override
        public void run() {
            try {
                // TODO 进入
                processTasks();
            } catch (Throwable e) {
                getEngineLog().error(e.toString(), e);
            }
        }
    }
}
