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

package com.alibaba.nacos.config.server.service.dump.processor;

import com.alibaba.nacos.common.task.NacosTask;
import com.alibaba.nacos.common.task.NacosTaskProcessor;
import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nacos.config.server.model.ConfigInfoWrapper;
import com.alibaba.nacos.config.server.service.AggrWhitelist;
import com.alibaba.nacos.config.server.service.ClientIpWhiteList;
import com.alibaba.nacos.config.server.service.ConfigCacheService;
import com.alibaba.nacos.config.server.service.SwitchService;
import com.alibaba.nacos.config.server.service.dump.task.DumpAllTask;
import com.alibaba.nacos.config.server.service.repository.ConfigInfoPersistService;
import com.alibaba.nacos.config.server.utils.GroupKey2;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.alibaba.nacos.config.server.utils.PropertyUtil;
import com.alibaba.nacos.persistence.model.Page;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.config.server.constant.Constants.ENCODE_UTF8;
import static com.alibaba.nacos.config.server.utils.LogUtil.DEFAULT_LOG;

/**
 * Dump all processor.
 *
 * @author Nacos
 * @date 2020/7/5 12:19 PM
 */
public class DumpAllProcessor implements NacosTaskProcessor {

    public DumpAllProcessor(ConfigInfoPersistService configInfoPersistService) {
        this.configInfoPersistService = configInfoPersistService;
    }

    @Override
    public boolean process(NacosTask task) {
        // 检查传入的 task 是否是 DumpAllTask 类型。如果不是，记录错误日志并返回 false，表示处理失败
        if (!(task instanceof DumpAllTask)) {
            DEFAULT_LOG.error("[all-dump-error] ,invalid task type,DumpAllProcessor should process DumpAllTask type.");
            return false;
        }
        // 如果是 DumpAllTask 类型，进行强制类型转换
        DumpAllTask dumpAllTask = (DumpAllTask) task;
        // 调用 configInfoPersistService.findConfigMaxId() 获取当前数据库中配置信息的最大 ID。
        // currentMaxId 是数据库中最新的配置信息 ID
        long currentMaxId = configInfoPersistService.findConfigMaxId();
        // lastMaxId 用于跟踪每次查询处理后的最大 ID
        long lastMaxId = 0;
        ThreadPoolExecutor executorService = null;
        // 根据 dumpAllTask.isStartUp() 判断任务是否是启动时执行的任务
        if (dumpAllTask.isStartUp()) {
            // 如果是启动时任务，则使用多线程（CPU 核心数）来处理转储任务，并设置了一个固定大小的队列来处理分页任务
            executorService = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
                    Runtime.getRuntime().availableProcessors(), 60L, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(PropertyUtil.getAllDumpPageSize() * 2),
                    r -> new Thread(r, "dump all executor"), new ThreadPoolExecutor.CallerRunsPolicy());
        } else {
            // 如果不是启动时任务，使用单线程来处理任务
            executorService = new ThreadPoolExecutor(1, 1, 60L, TimeUnit.SECONDS, new SynchronousQueue<>(),
                    r -> new Thread(r, "dump all executor"), new ThreadPoolExecutor.CallerRunsPolicy());
        }

        DEFAULT_LOG.info("start dump all config-info...");
        // 逐步从 lastMaxId 开始，查询数据库中所有的配置信息，直到达到 currentMaxId
        while (lastMaxId < currentMaxId) {

            long start = System.currentTimeMillis();
            // configInfoPersistService.findAllConfigInfoFragment(lastMaxId, pageSize, isStartUp)
            // 按 lastMaxId 查询下一个数据片段。pageSize 是每次查询的记录数
            Page<ConfigInfoWrapper> page = configInfoPersistService.findAllConfigInfoFragment(lastMaxId,
                    PropertyUtil.getAllDumpPageSize(), dumpAllTask.isStartUp());
            long dbTimeStamp = System.currentTimeMillis();
            // 如果没有查询到新的配置信息（即 page 或 pageItems 为空），则跳出循环
            if (page == null || page.getPageItems() == null || page.getPageItems().isEmpty()) {
                break;
            }

            for (ConfigInfoWrapper cf : page.getPageItems()) {
                // 遍历查询到的配置信息，将 lastMaxId 更新为当前最大 ID，确保下一次分页从正确的 ID 继续
                lastMaxId = Math.max(cf.getId(), lastMaxId);
                //if not start up, page query will not return content, check md5 and lastModified first ,if changed ,get single content info to dump.
                if (!dumpAllTask.isStartUp()) {// 对于非启动时任务，不会直接获取配置信息的内容。
                    final String groupKey = GroupKey2.getKey(cf.getDataId(), cf.getGroup(), cf.getTenant());
                    // 首先检查 LastModified（上次修改时间）是否比本地缓存的新
                    boolean newLastModified = cf.getLastModified() > ConfigCacheService.getLastModifiedTs(groupKey);
                    //check md5 & update local disk cache.
                    // 同时，检查 MD5 值是否不同。
                    String localContentMd5 = ConfigCacheService.getContentMd5(groupKey);
                    boolean md5Update = !localContentMd5.equals(cf.getMd5());
                    if (newLastModified || md5Update) {// 修改时间的变化 || MD5 值的变化
                        LogUtil.DUMP_LOG.info("[dump-all] find change config {}, {}, md5={}", groupKey,
                                cf.getLastModified(), cf.getMd5());
                        // 如果有变化，则查询详细配置信息
                        cf = configInfoPersistService.findConfigInfo(cf.getDataId(), cf.getGroup(), cf.getTenant());
                    } else {
                        continue;
                    }
                }

                if (cf == null) { // 如果配置数据为空，则跳过
                    continue;
                }
                // 聚合白名单
                if (cf.getDataId().equals(AggrWhitelist.AGGRIDS_METADATA)) {
                    // TODO 进入
                    AggrWhitelist.load(cf.getContent());
                }
                // 客户端白名单
                if (cf.getDataId().equals(ClientIpWhiteList.CLIENT_IP_WHITELIST_METADATA)) {
                    // TODO 进入
                    ClientIpWhiteList.load(cf.getContent());
                }
                // 切换服务
                if (cf.getDataId().equals(SwitchService.SWITCH_META_DATA_ID)) {
                    // TODO 进入
                    SwitchService.load(cf.getContent());
                }

                final String content = cf.getContent();
                final String dataId = cf.getDataId();
                final String group = cf.getGroup();
                final String tenant = cf.getTenant();
                final long lastModified = cf.getLastModified();
                final String type = cf.getType();
                final String encryptedDataKey = cf.getEncryptedDataKey();

                // 使用线程池中的线程执行转储任务。通过 ConfigCacheService.dumpWithMd5 方法将配置内容及其 MD5 值转储到本地磁盘
                executorService.execute(() -> {
                    final String md5Utf8 = MD5Utils.md5Hex(content, ENCODE_UTF8);
                    // TODO 进入
                    boolean result = ConfigCacheService.dumpWithMd5(dataId, group, tenant, content, md5Utf8,
                            lastModified, type, encryptedDataKey);
                    if (result) {
                        LogUtil.DUMP_LOG.info("[dump-all-ok] {}, {}, length={},md5UTF8={}",
                                GroupKey2.getKey(dataId, group), lastModified, content.length(), md5Utf8);
                    } else {
                        LogUtil.DUMP_LOG.info("[dump-all-error] {}", GroupKey2.getKey(dataId, group));
                    }

                });

            }

            long diskStamp = System.currentTimeMillis();
            DEFAULT_LOG.info("[all-dump] submit all task for {} / {}, dbTime={},diskTime={}", lastMaxId, currentMaxId,
                    (dbTimeStamp - start), (diskStamp - dbTimeStamp));
        }

        //wait all task are finished and then shutdown executor.
        try {
            // 等待线程池中所有任务完成，使用 executorService.getQueue().size() 和 executorService.getActiveCount() 检查未完成的任务数。
            // 每隔 1 秒检查一次，直到所有任务完成后，关闭线程池
            int unfinishedTaskCount = 0;
            while ((unfinishedTaskCount = executorService.getQueue().size() + executorService.getActiveCount()) > 0) {
                DEFAULT_LOG.info("[all-dump] wait {} dump tasks to be finished", unfinishedTaskCount);
                Thread.sleep(1000L);
            }
            executorService.shutdown();

        } catch (Exception e) {
            DEFAULT_LOG.error("[all-dump] wait  dump tasks to be finished error", e);
        }
        // 记录成功完成全量转储操作的日志，并返回 true 表示成功
        DEFAULT_LOG.info("success to  dump all config-info。");
        return true;
    }

    final ConfigInfoPersistService configInfoPersistService;
}
