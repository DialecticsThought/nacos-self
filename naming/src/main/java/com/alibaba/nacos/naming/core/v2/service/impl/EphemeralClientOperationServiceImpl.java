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

package com.alibaba.nacos.naming.core.v2.service.impl;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.exception.runtime.NacosRuntimeException;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.naming.core.v2.ServiceManager;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManager;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManagerDelegate;
import com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent;
import com.alibaba.nacos.naming.core.v2.event.metadata.MetadataEvent;
import com.alibaba.nacos.naming.core.v2.pojo.BatchInstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.core.v2.service.ClientOperationService;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.pojo.Subscriber;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Operation service for ephemeral clients and services.
 *
 * @author xiweng.yy
 */
@Component("ephemeralClientOperationService")
public class EphemeralClientOperationServiceImpl implements ClientOperationService {

    private final ClientManager clientManager;

    public EphemeralClientOperationServiceImpl(ClientManagerDelegate clientManager) {
        this.clientManager = clientManager;
    }

    @Override
    public void registerInstance(Service service, Instance instance, String clientId) throws NacosException {
        // 实例活动状态检查  即实例需要满足：心跳超时时间 > 心跳间隔、IP删除超时时间 > 心跳间隔
        NamingUtils.checkInstanceIsLegal(instance);
        // ServiceManager.getInstance()使用饿汉式单例返回一个ServiceManager对象
        // getSingleton从缓存singletonRepository中获取一个单例Service, 已存在的时候直接从缓存获取（注意Service的equals和hasCode方法）
        //TODO 进入 getInstance() 和 getSingleton
        Service singleton = ServiceManager.getInstance().getSingleton(service);
        // 判断获取到的service是否是临时实例，如果不是，则报错，
        // 因为当前的service（EphemeralClientOperationServiceImpl）就是处理临时实例的
        if (!singleton.isEphemeral()) {
            throw new NacosRuntimeException(NacosException.INVALID_PARAM,
                    String.format("Current service %s is persistent service, can't register ephemeral instance.",
                            singleton.getGroupedServiceName()));
        }
        // 根据客户端ID从客户端管理器中找到客户端信息,这个关系在连接建立的时候存储
        // clientId=172.110.0.138:1001#true
        Client client = clientManager.getClient(clientId);
        // 判断Client是否合法：
        // 1、client是否为空，为空代表客户端已经断开连接，非法
        // 2、client是否为临时的，如果非临时的连接，非法，直接返回
        checkClientIsLegal(client, clientId);
        // 获取实例发布信息，包含一些属性，如实例IP、实例ID、
        //TODO 可以进入看一下
        InstancePublishInfo instanceInfo = getPublishInfo(instance);
        /**
         * 负责存储当前客户端服务注册表，也就是 service和instance的关系。
         * 1. 客户端将自己注册到了服务器端的ClientManager中；
         * 2. Client客户端内部有一个Map: ConcurrentHashMap<Service, InstancePublishInfo> publishers. 即发布者列表
         * 3. 将实例信息放入发布者列表中（ key: Service 、 value: 实例发布信息）
         */
        // com.alibaba.nacos.naming.core.v2.client.AbstractClient.publishers
        // protected final ConcurrentHashMap<Service, InstancePublishInfo> publishers = new ConcurrentHashMap<>(16, 0.75f, 1);
        // TODO 进入
        client.addServiceInstance(singleton, instanceInfo);
        // 设置客户端最后更新时间为当前时间
        client.setLastUpdatedTime();
        client.recalculateRevision();
        // 发布客户端注册事件通知订阅者, Nacos使用了发布-订阅模式来处理. 简单理解就是，事件发布器发布相应的事件，然后对事件感兴趣的订阅者就会进行相应的处理，解耦
        NotifyCenter.publishEvent(new ClientOperationEvent.ClientRegisterServiceEvent(singleton, clientId));
        // 发布实例元数据事件通知订阅者
        NotifyCenter
                .publishEvent(new MetadataEvent.InstanceMetadataEvent(singleton, instanceInfo.getMetadataId(), false));
    }

    @Override
    public void batchRegisterInstance(Service service, List<Instance> instances, String clientId) {
        Service singleton = ServiceManager.getInstance().getSingleton(service);
        if (!singleton.isEphemeral()) {
            throw new NacosRuntimeException(NacosException.INVALID_PARAM,
                    String.format("Current service %s is persistent service, can't batch register ephemeral instance.",
                            singleton.getGroupedServiceName()));
        }
        Client client = clientManager.getClient(clientId);
        checkClientIsLegal(client, clientId);
        BatchInstancePublishInfo batchInstancePublishInfo = new BatchInstancePublishInfo();
        List<InstancePublishInfo> resultList = new ArrayList<>();
        for (Instance instance : instances) {
            InstancePublishInfo instanceInfo = getPublishInfo(instance);
            resultList.add(instanceInfo);
        }
        batchInstancePublishInfo.setInstancePublishInfos(resultList);
        client.addServiceInstance(singleton, batchInstancePublishInfo);
        client.setLastUpdatedTime();
        client.recalculateRevision();
        NotifyCenter.publishEvent(new ClientOperationEvent.ClientRegisterServiceEvent(singleton, clientId));
        NotifyCenter.publishEvent(
                new MetadataEvent.InstanceMetadataEvent(singleton, batchInstancePublishInfo.getMetadataId(), false));
    }

    @Override
    public void deregisterInstance(Service service, Instance instance, String clientId) {
        if (!ServiceManager.getInstance().containSingleton(service)) {
            Loggers.SRV_LOG.warn("remove instance from non-exist service: {}", service);
            return;
        }
        Service singleton = ServiceManager.getInstance().getSingleton(service);
        Client client = clientManager.getClient(clientId);
        checkClientIsLegal(client, clientId);
        InstancePublishInfo removedInstance = client.removeServiceInstance(singleton);
        client.setLastUpdatedTime();
        client.recalculateRevision();
        if (null != removedInstance) {
            NotifyCenter.publishEvent(new ClientOperationEvent.ClientDeregisterServiceEvent(singleton, clientId));
            NotifyCenter.publishEvent(
                    new MetadataEvent.InstanceMetadataEvent(singleton, removedInstance.getMetadataId(), true));
        }
    }

    @Override
    public void subscribeService(Service service, Subscriber subscriber, String clientId) {
        Service singleton = ServiceManager.getInstance().getSingletonIfExist(service).orElse(service);
        Client client = clientManager.getClient(clientId);
        checkClientIsLegal(client, clientId);
        client.addServiceSubscriber(singleton, subscriber);
        client.setLastUpdatedTime();
        NotifyCenter.publishEvent(new ClientOperationEvent.ClientSubscribeServiceEvent(singleton, clientId));
    }

    @Override
    public void unsubscribeService(Service service, Subscriber subscriber, String clientId) {
        Service singleton = ServiceManager.getInstance().getSingletonIfExist(service).orElse(service);
        Client client = clientManager.getClient(clientId);
        checkClientIsLegal(client, clientId);
        client.removeServiceSubscriber(singleton);
        client.setLastUpdatedTime();
        NotifyCenter.publishEvent(new ClientOperationEvent.ClientUnsubscribeServiceEvent(singleton, clientId));
    }

    private void checkClientIsLegal(Client client, String clientId) {
        if (client == null) {
            Loggers.SRV_LOG.warn("Client connection {} already disconnect", clientId);
            throw new NacosRuntimeException(NacosException.CLIENT_DISCONNECT,
                    String.format("Client [%s] connection already disconnect, can't register ephemeral instance.",
                            clientId));
        }
        if (!client.isEphemeral()) {
            Loggers.SRV_LOG.warn("Client connection {} type is not ephemeral", clientId);
            throw new NacosRuntimeException(NacosException.INVALID_PARAM,
                    String.format("Current client [%s] is persistent client, can't register ephemeral instance.",
                            clientId));
        }
    }
}
