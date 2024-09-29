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
import com.alibaba.nacos.config.server.model.ConfigInfoBetaWrapper;
import com.alibaba.nacos.config.server.service.ConfigCacheService;
import com.alibaba.nacos.config.server.service.repository.ConfigInfoBetaPersistService;
import com.alibaba.nacos.config.server.utils.GroupKey2;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.alibaba.nacos.persistence.model.Page;

import static com.alibaba.nacos.config.server.utils.LogUtil.DEFAULT_LOG;

/**
 * Dump all beta processor.
 *
 * @author Nacos
 * @author Wei.Wang
 * @date 2020/7/5 12:18 PM
 *
 * TODO
 * 用于将所有 Beta 配置信息转储到本地缓存。
 * 这个类实现了 NacosTaskProcessor 接口，通过分页的方式处理大量的 Beta 配置信息，并逐条转储
 */
public class DumpAllBetaProcessor implements NacosTaskProcessor {

    public DumpAllBetaProcessor(ConfigInfoBetaPersistService configInfoBetaPersistService) {
        this.configInfoBetaPersistService = configInfoBetaPersistService;
    }

    @Override
    public boolean process(NacosTask task) {
        int rowCount = configInfoBetaPersistService.configInfoBetaCount();
        int pageCount = (int) Math.ceil(rowCount * 1.0 / PAGE_SIZE);

        int actualRowCount = 0;
        for (int pageNo = 1; pageNo <= pageCount; pageNo++) {
            // 从数据库中获取当前页的 Beta 配置信息。pageNo 是当前页码，PAGE_SIZE 是每页的条目数
            Page<ConfigInfoBetaWrapper> page = configInfoBetaPersistService.findAllConfigInfoBetaForDumpAll(pageNo,
                    PAGE_SIZE);
            if (page != null) {
                // 获取当前页中的所有 ConfigInfoBetaWrapper 对象。每个 ConfigInfoBetaWrapper 代表一条 Beta 配置信息，包括 dataId、group、tenant 等字段
                for (ConfigInfoBetaWrapper cf : page.getPageItems()) {
                    // 调用 ConfigCacheService.dumpBeta 方法，将当前的 Beta 配置信息转储到本地缓存。
                    // 传递的参数包括 dataId、group、tenant、配置内容 content、最后修改时间 lastModified、Beta IP 列表 betaIps 和加密密钥 encryptedDataKey
                    boolean result = ConfigCacheService.dumpBeta(cf.getDataId(), cf.getGroup(), cf.getTenant(),
                            cf.getContent(), cf.getLastModified(), cf.getBetaIps(), cf.getEncryptedDataKey());
                    LogUtil.DUMP_LOG.info("[dump-all-beta-ok] result={}, {}, {}, length={}, md5={}", result,
                            GroupKey2.getKey(cf.getDataId(), cf.getGroup()), cf.getLastModified(),
                            cf.getContent().length(), cf.getMd5());
                }
                // 累加当前页处理的 Beta 配置信息条数，更新已处理的总条目数
                actualRowCount += page.getPageItems().size();
                DEFAULT_LOG.info("[all-dump-beta] {} / {}", actualRowCount, rowCount);
            }
        }
        return true;
    }

    static final int PAGE_SIZE = 1000;

    final ConfigInfoBetaPersistService configInfoBetaPersistService;
}
