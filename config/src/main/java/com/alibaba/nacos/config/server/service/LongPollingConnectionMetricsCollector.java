/*
 *
 * Copyright 1999-2021 Alibaba Group Holding Ltd.
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
 *
 */

package com.alibaba.nacos.config.server.service;

import com.alibaba.nacos.plugin.control.connection.ConnectionMetricsCollector;
import com.alibaba.nacos.sys.utils.ApplicationUtils;

import java.util.stream.Collectors;

/**
 * long polling connection metrics.
 *
 * @author shiyiyue
 */
public class LongPollingConnectionMetricsCollector implements ConnectionMetricsCollector {

    @Override
    public String getName() {
        return "long_polling";
    }

    @Override
    public int getTotalCount() {
        return ApplicationUtils.getBean(LongPollingService.class).allSubs.size();
    }

    @Override
    public int getCountForIp(String ip) {
        // 获取长轮询客户端列表: 通过 ApplicationUtils.getBean(LongPollingService.class) 获取 LongPollingService 实例中的 allSubs 列表。
        return ApplicationUtils.getBean(LongPollingService.class).
                // 将 allSubs 转换为流，以便对其进行处理
                allSubs.stream()
                // 使用过滤器遍历所有订阅长轮询的客户端，找到与传入的 ip 地址匹配的客户端
                .filter(a ->
                        a.ip.equalsIgnoreCase(ip)).
                // 将匹配的客户端集合转换为一个列表
                collect(Collectors.toList()).
                // 返回该列表的大小，即来自该 IP 地址的长轮询连接数
                size();
    }
}
