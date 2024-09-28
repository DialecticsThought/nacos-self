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

package com.alibaba.nacos.common.executor;

import com.alibaba.nacos.common.utils.StringUtils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Name thread factory.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
public class NameThreadFactory implements ThreadFactory {
    // 用于为每个新创建的线程分配一个唯一的编号。AtomicInteger 是线程安全的整数操作类，确保在多线程环境下递增操作不会引发竞争条件
    //它从 0 开始计数，每次创建一个新线程时，id 递增
    private final AtomicInteger id = new AtomicInteger(0);

    private String name;

    //构造方法接受一个 name 参数，这个参数将作为线程名称的前缀
    public NameThreadFactory(String name) {
        //  public static final String DOT = ".";
        // 会检查传入的 name 是否以 . 结尾（即 DOT）。如果没有，自动为它加上 .，以便每个线程的名称都是规范的，如 "pool-1-thread-0" 这种格式
        if (!name.endsWith(StringUtils.DOT)) {
            name += StringUtils.DOT;
        }
        this.name = name;
    }

    /**
     *
     * @param r a runnable to be executed by new thread instance 表示该线程要执行的任务
     * @return
     */
    @Override
    public Thread newThread(Runnable r) {
        // name 是线程的前缀，id.getAndIncrement() 通过 AtomicInteger 生成一个唯一编号并递增，用于区别每个线程的名称
        // 例如，name = "pool-1-thread-"，如果 id 是 0，则生成的线程名称为 "pool-1-thread-0"
        String threadName = name + id.getAndIncrement();
        // 新线程的任务是 r，线程名称是 threadName
        Thread thread = new Thread(r, threadName);
        // 设置线程为守护线程 (Daemon Thread)。
        thread.setDaemon(true);
        return thread;
    }
}
