/*
 * Copyright 1999-2023 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.plugin.control.impl;

import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.plugin.control.Loggers;
import com.alibaba.nacos.plugin.control.connection.ConnectionControlManager;
import com.alibaba.nacos.plugin.control.connection.ConnectionMetricsCollector;
import com.alibaba.nacos.plugin.control.connection.request.ConnectionCheckRequest;
import com.alibaba.nacos.plugin.control.connection.response.ConnectionCheckCode;
import com.alibaba.nacos.plugin.control.connection.response.ConnectionCheckResponse;
import com.alibaba.nacos.plugin.control.connection.rule.ConnectionControlRule;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Nacos default control plugin implementation.
 *
 * @author shiyiyue
 */
public class NacosConnectionControlManager extends ConnectionControlManager {

    @Override
    public String getName() {
        return "nacos";
    }

    public NacosConnectionControlManager() {
        super();
    }

    @Override
    public void applyConnectionLimitRule(ConnectionControlRule connectionControlRule) {
        super.connectionControlRule = connectionControlRule;
        Loggers.CONTROL.info("Connection control rule updated to ->" + (this.connectionControlRule == null ? null
                : JacksonUtils.toJson(this.connectionControlRule)));
        Loggers.CONTROL.warn("Connection control updated, But connection control manager is no limit implementation.");
    }

    @Override
    public ConnectionCheckResponse check(ConnectionCheckRequest connectionCheckRequest) {
        // 创建一个 ConnectionCheckResponse 对象，该对象将保存检查结果。
        ConnectionCheckResponse connectionCheckResponse = new ConnectionCheckResponse();
        // 初始状态为连接检查通过（假设连接数不会超过限制）
        connectionCheckResponse.setSuccess(true);
        // 表示连接检查成功，连接通过
        connectionCheckResponse.setCode(ConnectionCheckCode.PASS_BY_TOTAL);
        // 获取连接数的限制值，即服务器允许的最大连接数。
        // connectionControlRule 是连接控制的规则对象，getCountLimit() 方法返回当前允许的最大连接数。
        int totalCountLimit = connectionControlRule.getCountLimit();
        // Get total connection from metrics
        /**
         * 收集所有连接类型的连接数。
         * metricsCollectorList 是一个 ConnectionMetricsCollector 列表，
         * 其中每个元素都负责收集特定类型的连接统计数据（如长轮询、短连接等）
         * getName() 返回连接类型名称。
         * getTotalCount() 返回当前类型的连接总数。
         * 最终生成一个 Map，键为连接类型的名称（例如 "long_polling"），值为连接的总数
         */
        Map<String, Integer> metricsTotalCount = metricsCollectorList.stream().collect(
                Collectors.toMap(ConnectionMetricsCollector::getName, ConnectionMetricsCollector::getTotalCount));
        // 计算所有连接类型的连接总数。通过 metricsTotalCount.values() 提取所有连接类型的连接数量，并对这些数量进行求和，得到所有连接的总数
        int totalCount = metricsTotalCount.values().stream().mapToInt(Integer::intValue).sum();
        // 如果当前连接总数 totalCount 达到或超过限制 totalCountLimit，则
        if (totalCount >= totalCountLimit) {
            // 设置 connectionCheckResponse 的状态为失败 (setSuccess(false))。
            connectionCheckResponse.setSuccess(false);
            // 设置返回的错误代码为 DENY_BY_TOTAL_OVER，表示由于连接数超出限制，拒绝新连接
            connectionCheckResponse.setCode(ConnectionCheckCode.DENY_BY_TOTAL_OVER);
        }
        return connectionCheckResponse;
    }

}
