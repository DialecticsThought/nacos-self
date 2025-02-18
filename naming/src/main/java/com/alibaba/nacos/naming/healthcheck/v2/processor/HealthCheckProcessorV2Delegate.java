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

package com.alibaba.nacos.naming.healthcheck.v2.processor;

import com.alibaba.nacos.naming.core.v2.metadata.ClusterMetadata;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.healthcheck.extend.HealthCheckExtendProvider;
import com.alibaba.nacos.naming.healthcheck.extend.HealthCheckProcessorExtendV2;
import com.alibaba.nacos.naming.healthcheck.v2.HealthCheckTaskV2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Delegate of health check v2.x.
 * 健康检查 v2.x 的委托类
 * 这个类作为健康检查的代理，管理不同类型的健康检查处理器
 * @author nacos
 */
@Component("healthCheckDelegateV2")
public class HealthCheckProcessorV2Delegate implements HealthCheckProcessorV2 {
    // 一个Map，用来存储按类型分类的健康检查处理器
    private final Map<String, HealthCheckProcessorV2> healthCheckProcessorMap = new HashMap<>();
    /**
     * 构造方法，初始化提供者和扩展的健康检查处理器
     * @param provider 健康检查扩展提供者
     * @param healthCheckProcessorExtend 扩展的健康检查处理器
     */
    public HealthCheckProcessorV2Delegate(HealthCheckExtendProvider provider,
            HealthCheckProcessorExtendV2 healthCheckProcessorExtend) {
        // 初始化提供者（可能是配置资源或其他初始化工作）
        provider.setHealthCheckProcessorExtend(healthCheckProcessorExtend);
        // 初始化提供者（可能是配置资源或其他初始化工作）
        provider.init();
    }

    /**
     * Spring的自动注入方法，通过该方法将健康检查处理器集合注入进来
     * 将每个处理器按其类型存入Map中
     * @param processors 健康检查处理器的集合
     */
    @Autowired
    public void addProcessor(Collection<HealthCheckProcessorV2> processors) {
        // 过滤掉处理器类型为空的处理器，将其类型和处理器本身映射存入Map
        healthCheckProcessorMap.putAll(processors.stream().filter(processor -> processor.getType() != null)
                .collect(Collectors.toMap(HealthCheckProcessorV2::getType, processor -> processor)));
    }

    @Override
    public void process(HealthCheckTaskV2 task, Service service, ClusterMetadata metadata) {
        // 获取健康检查类型
        String type = metadata.getHealthyCheckType();
        // 根据健康检查类型获取对应的处理器
        HealthCheckProcessorV2 processor = healthCheckProcessorMap.get(type);
        // 如果没有找到匹配的处理器，使用默认的None类型处理器
        if (processor == null) {
            processor = healthCheckProcessorMap.get(NoneHealthCheckProcessor.TYPE);
        }
        // 执行处理器的健康检查任务
        processor.process(task, service, metadata);
    }

    @Override
    public String getType() {
        return null;
    }
}
