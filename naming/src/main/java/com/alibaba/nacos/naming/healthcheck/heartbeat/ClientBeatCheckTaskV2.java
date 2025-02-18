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

package com.alibaba.nacos.naming.healthcheck.heartbeat;

import com.alibaba.nacos.common.task.AbstractExecuteTask;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.core.v2.client.impl.IpPortBasedClient;
import com.alibaba.nacos.naming.core.v2.pojo.HealthCheckInstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.healthcheck.NacosHealthCheckTask;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.sys.utils.ApplicationUtils;

import java.util.Collection;

/**
 * Client beat check task of service for version 2.x.
 *
 * @author nkorange
 */
public class ClientBeatCheckTaskV2 extends AbstractExecuteTask implements BeatCheckTask, NacosHealthCheckTask {

    private final IpPortBasedClient client;

    private final String taskId;

    private final InstanceBeatCheckTaskInterceptorChain interceptorChain;

    public ClientBeatCheckTaskV2(IpPortBasedClient client) {
        this.client = client;
        this.taskId = client.getResponsibleId();
        this.interceptorChain = InstanceBeatCheckTaskInterceptorChain.getInstance();
    }

    public GlobalConfig getGlobalConfig() {
        return ApplicationUtils.getBean(GlobalConfig.class);
    }

    @Override
    public String taskKey() {
        return KeyBuilder.buildServiceMetaKey(client.getClientId(), String.valueOf(client.isEphemeral()));
    }

    @Override
    public String getTaskId() {
        return taskId;
    }

    @Override
    public void doHealthCheck() {
        try {
            // 拿到当前客户端上注册的全部服务
            Collection<Service> services = client.getAllPublishedService();
            // 遍历服务
            for (Service each : services) {
                HealthCheckInstancePublishInfo instance = (HealthCheckInstancePublishInfo) client
                        .getInstancePublishInfo(each);
                // 将全部服务用拦截器链一个个执行, 从这里往下看
                // TODO 进入
                // ServiceEnableBeatCheckInterceptor 检查服务是否开启了心跳检查功能，开启了就通过，没开启就拦截 ，不往下走了
                // InstanceEnableBeatCheckInterceptor  处理逻辑同上，再再做一次实例级别的过滤
                // InstanceBeatCheckResponsibleInterceptor 过滤掉不由自己注册的临时节点（因为并不是一个服务端要负责集群中全部临时节点的心跳处理的，而是只负责通过自己注册的
                // 当实例通过了上面三层拦截，留下来的都是要处理的了，在 InstanceBeatCheckTask 类中进行处理
                // TODO 进入
                interceptorChain.doInterceptor(new InstanceBeatCheckTask(client, each, instance));
            }
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("Exception while processing client beat time out.", e);
        }
    }

    @Override
    public void run() {
        doHealthCheck();
    }

    @Override
    public void passIntercept() {
        doHealthCheck();
    }

    @Override
    public void afterIntercept() {
    }
}
