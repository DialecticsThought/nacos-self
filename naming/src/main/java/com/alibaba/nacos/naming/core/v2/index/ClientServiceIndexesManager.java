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

package com.alibaba.nacos.naming.core.v2.index;

import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.SmartSubscriber;
import com.alibaba.nacos.common.trace.DeregisterInstanceReason;
import com.alibaba.nacos.common.trace.event.naming.DeregisterInstanceTraceEvent;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent;
import com.alibaba.nacos.naming.core.v2.event.publisher.NamingEventPublisherFactory;
import com.alibaba.nacos.naming.core.v2.event.service.ServiceEvent;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Client and service index manager.
 *
 * @author xiweng.yy
 * TODO
 * 他实现了 SmartSubscriber 接口 是订阅者
 * 既然是订阅者，查看onEvent方法 因为有事件的话publisher会执行subscriber的onEvent
 */
@Component
public class ClientServiceIndexesManager extends SmartSubscriber {
    /**
     * 服务与发布这个服务的客户端的关联关系
     * key是service，value就是clientId的集合
     */
    private final ConcurrentMap<Service, Set<String>> publisherIndexes = new ConcurrentHashMap<>();

    private final ConcurrentMap<Service, Set<String>> subscriberIndexes = new ConcurrentHashMap<>();

    public ClientServiceIndexesManager() {
        // TODO 向NotifyCenter中注册自己
        // NamingEventPublisherFactory.getInstance()也是用到了单例，返回了NamingEventPublisherFactory
        // TODO 进入
        NotifyCenter.registerSubscriber(this, NamingEventPublisherFactory.getInstance());
    }

    public Collection<String> getAllClientsRegisteredService(Service service) {
        return publisherIndexes.containsKey(service) ? publisherIndexes.get(service) : new ConcurrentHashSet<>();
    }

    public Collection<String> getAllClientsSubscribeService(Service service) {
        return subscriberIndexes.containsKey(service) ? subscriberIndexes.get(service) : new ConcurrentHashSet<>();
    }

    public Collection<Service> getSubscribedService() {
        return subscriberIndexes.keySet();
    }

    /**
     * Clear the service index without instances.
     *
     * @param service The service of the Nacos.
     */
    public void removePublisherIndexesByEmptyService(Service service) {
        if (publisherIndexes.containsKey(service) && publisherIndexes.get(service).isEmpty()) {
            publisherIndexes.remove(service);
        }
    }

    @Override
    public List<Class<? extends Event>> subscribeTypes() {
        List<Class<? extends Event>> result = new LinkedList<>();
        result.add(ClientOperationEvent.ClientRegisterServiceEvent.class);
        result.add(ClientOperationEvent.ClientDeregisterServiceEvent.class);
        result.add(ClientOperationEvent.ClientSubscribeServiceEvent.class);
        result.add(ClientOperationEvent.ClientUnsubscribeServiceEvent.class);
        result.add(ClientOperationEvent.ClientReleaseEvent.class);
        return result;
    }

    @Override
    public void onEvent(Event event) {
        if (event instanceof ClientOperationEvent.ClientReleaseEvent) {
            // 处理客户端断开连接事件
            handleClientDisconnect((ClientOperationEvent.ClientReleaseEvent) event);
        } else if (event instanceof ClientOperationEvent) {
            // 处理排除ClientReleaseEvent后的其它客户端操作事件
            // TODO 进入
            handleClientOperation((ClientOperationEvent) event);
        }
    }

    /**
     * 1、从订阅者列表中移除所有服务对这个客户端的引用
     * 2、从发布者列表中移除所有服务对这个客户端的引用
     * @param event
     */
    private void handleClientDisconnect(ClientOperationEvent.ClientReleaseEvent event) {
        Client client = event.getClient();
        for (Service each : client.getAllSubscribeService()) {
            // 从订阅者列表中移除所有服务对这个客户端的引用
            // private final ConcurrentMap<Service, Set<String>> subscriberIndexes = new ConcurrentHashMap<>();
            // key: Service      value: 客户端ID集合
            removeSubscriberIndexes(each, client.getClientId());
        }
        DeregisterInstanceReason reason = event.isNative()
                ? DeregisterInstanceReason.NATIVE_DISCONNECTED : DeregisterInstanceReason.SYNCED_DISCONNECTED;
        long currentTimeMillis = System.currentTimeMillis();
        for (Service each : client.getAllPublishedService()) {
            // 从发布者列表中移除所有服务对这个客户端的引用
            removePublisherIndexes(each, client.getClientId());
            InstancePublishInfo instance = client.getInstancePublishInfo(each);
            NotifyCenter.publishEvent(new DeregisterInstanceTraceEvent(currentTimeMillis,
                    "", false, reason, each.getNamespace(), each.getGroup(), each.getName(),
                    instance.getIp(), instance.getPort()));
        }
    }

    private void handleClientOperation(ClientOperationEvent event) {
        // 获取服务
        Service service = event.getService();
        // 获取客户端ID
        String clientId = event.getClientId();
        if (event instanceof ClientOperationEvent.ClientRegisterServiceEvent) {
            // 处理ClientRegisterServiceEvent事件（服务注册)
            // TODO 进入 以服务注册为例
            addPublisherIndexes(service, clientId);
        } else if (event instanceof ClientOperationEvent.ClientDeregisterServiceEvent) {
            // 处理ClientDeregisterServiceEvent事件（服务下线）
            removePublisherIndexes(service, clientId);
        } else if (event instanceof ClientOperationEvent.ClientSubscribeServiceEvent) {
            // 处理ClientSubscribeServiceEvent事件（订阅）
            // TODO com.alibaba.nacos.naming.core.v2.service.impl.EphemeralClientOperationServiceImpl.subscribeService出发盖房
            addSubscriberIndexes(service, clientId);
        } else if (event instanceof ClientOperationEvent.ClientUnsubscribeServiceEvent) {
            // 处理ClientUnsubscribeServiceEvent事件（取消订阅）
            removeSubscriberIndexes(service, clientId);
        }
    }

    private void addPublisherIndexes(Service service, String clientId) {
        // 将客户端添加到注册服务中
        // ConcurrentMap<Service, Set<String>> publisherIndexes
        publisherIndexes.computeIfAbsent(service, key -> new ConcurrentHashSet<>()).add(clientId);
        // 发布服务变更事件，这样其它客户端才能及时知道service这个服务可用了
        NotifyCenter.publishEvent(new ServiceEvent.ServiceChangedEvent(service, true));
    }

    private void removePublisherIndexes(Service service, String clientId) {
        publisherIndexes.computeIfPresent(service, (s, ids) -> {
            ids.remove(clientId);
            // 发布服务变更事件，这样其它客户端才能及时知道service这个服务不可用了
            NotifyCenter.publishEvent(new ServiceEvent.ServiceChangedEvent(service, true));
            return ids.isEmpty() ? null : ids;
        });
    }

    private void addSubscriberIndexes(Service service, String clientId) {
        Set<String> clientIds = subscriberIndexes.computeIfAbsent(service, key -> new ConcurrentHashSet<>());
        // Fix #5404, Only first time add need notify event.
        if (clientIds.add(clientId)) {
            NotifyCenter.publishEvent(new ServiceEvent.ServiceSubscribedEvent(service, clientId));
        }
    }

    private void removeSubscriberIndexes(Service service, String clientId) {
        Set<String> clientIds = subscriberIndexes.get(service);
        if (clientIds == null) {
            return;
        }
        clientIds.remove(clientId);
        if (clientIds.isEmpty()) {
            subscriberIndexes.remove(service);
        }
    }
}
