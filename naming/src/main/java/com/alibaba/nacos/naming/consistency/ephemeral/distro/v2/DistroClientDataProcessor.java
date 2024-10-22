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

package com.alibaba.nacos.naming.consistency.ephemeral.distro.v2;

import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.SmartSubscriber;
import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.core.distributed.distro.DistroProtocol;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataProcessor;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataStorage;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.entity.DistroKey;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.naming.constants.ClientConstants;
import com.alibaba.nacos.naming.core.v2.ServiceManager;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.client.ClientSyncData;
import com.alibaba.nacos.naming.core.v2.client.ClientSyncDatumSnapshot;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManager;
import com.alibaba.nacos.naming.core.v2.event.client.ClientEvent;
import com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent;
import com.alibaba.nacos.naming.core.v2.event.metadata.MetadataEvent;
import com.alibaba.nacos.naming.core.v2.event.publisher.NamingEventPublisherFactory;
import com.alibaba.nacos.naming.core.v2.pojo.BatchInstanceData;
import com.alibaba.nacos.naming.core.v2.pojo.BatchInstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Distro processor for v2.
 *
 * @author xiweng.yy
 */
public class DistroClientDataProcessor extends SmartSubscriber implements DistroDataStorage, DistroDataProcessor {

    public static final String TYPE = "Nacos:Naming:v2:ClientData";

    private final ClientManager clientManager;

    private final DistroProtocol distroProtocol;

    private volatile boolean isFinishInitial;

    public DistroClientDataProcessor(ClientManager clientManager, DistroProtocol distroProtocol) {
        this.clientManager = clientManager;
        this.distroProtocol = distroProtocol;
        NotifyCenter.registerSubscriber(this, NamingEventPublisherFactory.getInstance());
    }

    @Override
    public void finishInitial() {
        isFinishInitial = true;
    }

    @Override
    public boolean isFinishInitial() {
        return isFinishInitial;
    }

    @Override
    public List<Class<? extends Event>> subscribeTypes() {
        List<Class<? extends Event>> result = new LinkedList<>();
        result.add(ClientEvent.ClientChangedEvent.class);
        result.add(ClientEvent.ClientDisconnectEvent.class);
        result.add(ClientEvent.ClientVerifyFailedEvent.class);
        return result;
    }

    @Override
    public void onEvent(Event event) {
        // 如果是单体的nacos的话不需要同步给其他nacos节点
        if (EnvUtil.getStandaloneMode()) {
            return;
        }
        if (event instanceof ClientEvent.ClientVerifyFailedEvent) {
            syncToVerifyFailedServer((ClientEvent.ClientVerifyFailedEvent) event);
        } else {
            // TODO 进入
            syncToAllServer((ClientEvent) event);
        }
    }

    private void syncToVerifyFailedServer(ClientEvent.ClientVerifyFailedEvent event) {
        Client client = clientManager.getClient(event.getClientId());
        if (isInvalidClient(client)) {
            return;
        }
        DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
        // Verify failed data should be sync directly.
        // TODO 进入
        distroProtocol.syncToTarget(distroKey, DataOperation.ADD, event.getTargetServer(), 0L);
    }

    private void syncToAllServer(ClientEvent event) {
        // 获取事件对应的客户端对象
        Client client = event.getClient();
        // 检查客户端是否无效，如果无效则直接返回，不做任何处理
        if (isInvalidClient(client)) {
            return;
        }
        // 判断是否是客户端断开连接的事件
        if (event instanceof ClientEvent.ClientDisconnectEvent) {
            // 如果是客户端断开事件，生成一个用于同步的 DistroKey，DistroKey 包含客户端的 ID 和类型
            // TODO  TYPE = Nacos:Naming:v2:ClientData
            DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
            // 使用 Distro 协议将该客户端的删除操作同步到所有服务器
            distroProtocol.sync(distroKey, DataOperation.DELETE);
        } else if (event instanceof ClientEvent.ClientChangedEvent) {// 判断是否是客户端状态变化的事件
            // 如果是客户端状态变化事件，生成一个用于同步的 DistroKey，DistroKey 包含客户端的 ID 和类型
            DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
            // 使用 Distro 协议将该客户端的变化操作同步到所有服务器
            distroProtocol.sync(distroKey, DataOperation.CHANGE);
        }
    }

    private boolean isInvalidClient(Client client) {
        // Only ephemeral data sync by Distro, persist client should sync by raft.
        return null == client || !client.isEphemeral() || !clientManager.isResponsibleClient(client);
    }

    @Override
    public String processType() {
        return TYPE;
    }

    @Override
    public boolean processData(DistroData distroData) {
        switch (distroData.getType()) {
            case ADD:
            case CHANGE:
                ClientSyncData clientSyncData = ApplicationUtils.getBean(Serializer.class)
                        .deserialize(distroData.getContent(), ClientSyncData.class);
                handlerClientSyncData(clientSyncData);
                return true;
            case DELETE:
                String deleteClientId = distroData.getDistroKey().getResourceKey();
                Loggers.DISTRO.info("[Client-Delete] Received distro client sync data {}", deleteClientId);
                clientManager.clientDisconnected(deleteClientId);
                return true;
            default:
                return false;
        }
    }

    /**
     * 该方法用于处理客户端同步数据 (ClientSyncData)
     * @param clientSyncData 表示从客户端接收到的同步数据，其中包含客户端的 ID、属性以及服务实例的同步信息
     */
    private void handlerClientSyncData(ClientSyncData clientSyncData) {
        Loggers.DISTRO
                .info("[Client-Add] Received distro client sync data {}, revision={}", clientSyncData.getClientId(),
                        clientSyncData.getAttributes().getClientAttribute(ClientConstants.REVISION, 0L));
        // 同步客户端连接信息：调用 clientManager.syncClientConnected 方法，使用客户端的 ID (clientSyncData.getClientId()) 和属性来同步该客户端的连接信息。
        // 这个方法会确保客户端已连接并且客户端的相关状态已更新
        clientManager.syncClientConnected(clientSyncData.getClientId(), clientSyncData.getAttributes());
        // 获取客户端实例：通过 clientManager.getClient 方法，根据客户端 ID 获取对应的 Client 对象。这个对象包含客户端的相关信息和发布的服务实例
        Client client = clientManager.getClient(clientSyncData.getClientId());
        // 调用 upgradeClient 方法，使用 client 和 clientSyncData 来升级客户端的服务实例信息。
        // 这个方法会检查客户端是否发布了新的服务实例，并同步相关的服务信息。如果有服务取消注册，该方法也会处理相应的移除逻辑。
        upgradeClient(client, clientSyncData);
    }

    /**
     * 用于同步客户端的服务实例信息。
     * 首先处理批量实例数据的同步，然后逐个检查每个服务实例是否需要更新或删除，最后更新客户端的修订版本。
     * 通过发布相应的注册和取消注册事件，确保系统的其他组件能够及时感知服务的变更，维护系统的一致性
     * @param client 当前需要升级的客户端。
     * @param clientSyncData 包含客户端的同步数据，包括服务实例信息。
     */
    private void upgradeClient(Client client, ClientSyncData clientSyncData) {
        // 初始化 syncedService 集合：创建一个空的 syncedService 集合，用于存储在此次同步过程中已处理的 Service 对象，以便后续做差异化处理
        Set<Service> syncedService = new HashSet<>();
        // process batch instance sync logic
        // 调用批量实例同步逻辑：调用 processBatchInstanceDistroData 方法，处理批量服务实例的同步逻辑，主要是通过 clientSyncData 更新客户端的批量实例发布信息
        processBatchInstanceDistroData(syncedService, client, clientSyncData);
        // 获取同步数据中的服务信息：从 clientSyncData 中提取命名空间、组名、服务名和实例发布信息。这些列表用于处理客户端的具体服务实例同步
        List<String> namespaces = clientSyncData.getNamespaces();
        List<String> groupNames = clientSyncData.getGroupNames();
        List<String> serviceNames = clientSyncData.getServiceNames();
        List<InstancePublishInfo> instances = clientSyncData.getInstancePublishInfos();
        //遍历每个服务实例：根据命名空间、组名、服务名创建 Service 对象，
        // 并通过 ServiceManager 获取对应的单例服务对象，然后将该服务添加到 syncedService 集合中，表示该服务已经同步
        for (int i = 0; i < namespaces.size(); i++) {
            Service service = Service.newService(namespaces.get(i), groupNames.get(i), serviceNames.get(i));
            Service singleton = ServiceManager.getInstance().getSingleton(service);
            syncedService.add(singleton);
            // 更新客户端服务实例信息：检查当前服务实例的发布信息与客户端现有的实例发布信息是否一致。如果不一致，则更新客户端的服务实例信息
            InstancePublishInfo instancePublishInfo = instances.get(i);
            if (!instancePublishInfo.equals(client.getInstancePublishInfo(singleton))) {
                client.addServiceInstance(singleton, instancePublishInfo);
                // 当新的实例发布信息被更新后，触发 ClientRegisterServiceEvent 事件，通知系统该客户端已注册该服务。
                NotifyCenter.publishEvent(
                        new ClientOperationEvent.ClientRegisterServiceEvent(singleton, client.getClientId()));
                // 同时，发布 InstanceMetadataEvent 事件，通知该实例的元数据信息也已更新。
                NotifyCenter.publishEvent(
                        new MetadataEvent.InstanceMetadataEvent(singleton, instancePublishInfo.getMetadataId(), false));
            }
        }
        // 删除未同步的服务实例：遍历客户端当前发布的所有服务实例，检查哪些服务实例没有出现在 syncedService 集合中。
        // 如果某个服务没有同步过，表示该服务不再发布，需要从客户端中移除
        for (Service each : client.getAllPublishedService()) {
            if (!syncedService.contains(each)) {
                client.removeServiceInstance(each);
                // 当一个服务实例被移除后，触发 ClientDeregisterServiceEvent 事件，通知系统该客户端取消了该服务的注册
                NotifyCenter.publishEvent(
                        new ClientOperationEvent.ClientDeregisterServiceEvent(each, client.getClientId()));
            }
        }
        // clientSyncData 中提取修订版本号（REVISION），并将其设置到客户端中，用于标记客户端的最新版本
        client.setRevision(clientSyncData.getAttributes().<Integer>getClientAttribute(ClientConstants.REVISION, 0));
    }

    /**
     * 批量处理客户端的服务实例同步操作。
     * 它从客户端获取批量实例的发布信息，
     * 并与客户端现有的实例进行比较，必要时更新服务实例，并通过事件机制通知其他系统或模块进行相应的处理。
     * 这种设计确保了分布式系统中多个服务实例的同步和一致性
     * @param syncedService  用于保存已经同步的 Service 对象
     * @param client  代表发出同步请求的客户端
     * @param clientSyncData  包含客户端同步的数据对象
     */
    private static void processBatchInstanceDistroData(Set<Service> syncedService, Client client,
            ClientSyncData clientSyncData) {
        // 提取批量实例数据：从 clientSyncData 中获取 BatchInstanceData 对象，该对象包含了客户端需要同步的批量实例信息
        BatchInstanceData batchInstanceData = clientSyncData.getBatchInstanceData();
        // 检查数据有效性：如果 batchInstanceData 为空，或者 namespaces 列表为空，则记录日志并返回，不进行进一步处理。这是为了确保数据的有效性
        if (batchInstanceData == null || CollectionUtils.isEmpty(batchInstanceData.getNamespaces())) {
            Loggers.DISTRO.info("[processBatchInstanceDistroData] BatchInstanceData is null , clientId is :{}",
                    client.getClientId());
            return;
        }
        // 提取批量实例的相关信息：从 batchInstanceData 中分别获取命名空间、组名、服务名以及批量实例的发布信息。
        // 这些列表分别对应客户端要同步的各个服务的不同维度数据
        List<String> namespaces = batchInstanceData.getNamespaces();
        List<String> groupNames = batchInstanceData.getGroupNames();
        List<String> serviceNames = batchInstanceData.getServiceNames();
        List<BatchInstancePublishInfo> batchInstancePublishInfos = batchInstanceData.getBatchInstancePublishInfos();
        // 遍历每个命名空间：开始遍历每个命名空间，确保批量处理多个服务实例
        for (int i = 0; i < namespaces.size(); i++) {
            // 创建 Service 对象：通过命名空间、组名和服务名来创建一个新的 Service 对象，表示当前处理的服务
            Service service = Service.newService(namespaces.get(i), groupNames.get(i), serviceNames.get(i));
            // 创建 Service 对象：通过命名空间、组名和服务名来创建一个新的 Service 对象，表示当前处理的服务
            Service singleton = ServiceManager.getInstance().getSingleton(service);
            // 获取单例服务：通过 ServiceManager 获取当前 Service 的单例实例，确保对同一服务不会重复处理
            syncedService.add(singleton);
            // 添加到已同步的服务集合中：将该 Service 添加到已同步的 syncedService 集合中，避免重复处理
            BatchInstancePublishInfo batchInstancePublishInfo = batchInstancePublishInfos.get(i);
            // 获取实例发布信息：从 batchInstancePublishInfos 列表中获取当前服务的发布信息。同时，从客户端获取该服务的已发布实例信息
            InstancePublishInfo publishInfo = client.getInstancePublishInfo(singleton);
            // 比较发布信息：如果批量实例发布信息不为空，且与客户端已有的发布信息不同，继续处理同步操作
            if (batchInstancePublishInfo != null && !batchInstancePublishInfo.equals(publishInfo)) {
                // 更新客户端服务实例：将新的 batchInstancePublishInfo 添加到客户端的服务实例中，表示服务实例已经更新
                client.addServiceInstance(singleton, batchInstancePublishInfo);
                // 发布事件通知：通过 NotifyCenter 发布 ClientRegisterServiceEvent 事件，通知其他系统或模块该客户端的服务实例已被注册或更新
                NotifyCenter.publishEvent(
                        new ClientOperationEvent.ClientRegisterServiceEvent(singleton, client.getClientId()));
            }
        }
    }

    @Override
    public boolean processVerifyData(DistroData distroData, String sourceAddress) {
        DistroClientVerifyInfo verifyData = ApplicationUtils.getBean(Serializer.class)
                .deserialize(distroData.getContent(), DistroClientVerifyInfo.class);
        if (clientManager.verifyClient(verifyData)) {
            return true;
        }
        Loggers.DISTRO.info("client {} is invalid, get new client from {}", verifyData.getClientId(), sourceAddress);
        return false;
    }

    @Override
    public boolean processSnapshot(DistroData distroData) {
        ClientSyncDatumSnapshot snapshot = ApplicationUtils.getBean(Serializer.class)
                .deserialize(distroData.getContent(), ClientSyncDatumSnapshot.class);
        for (ClientSyncData each : snapshot.getClientSyncDataList()) {
            handlerClientSyncData(each);
        }
        return true;
    }

    @Override
    public DistroData getDistroData(DistroKey distroKey) {
        Client client = clientManager.getClient(distroKey.getResourceKey());
        if (null == client) {
            return null;
        }
        byte[] data = ApplicationUtils.getBean(Serializer.class).serialize(client.generateSyncData());
        return new DistroData(distroKey, data);
    }

    @Override
    public DistroData getDatumSnapshot() {
        List<ClientSyncData> datum = new LinkedList<>();
        for (String each : clientManager.allClientId()) {
            Client client = clientManager.getClient(each);
            if (null == client || !client.isEphemeral()) {
                continue;
            }
            datum.add(client.generateSyncData());
        }
        ClientSyncDatumSnapshot snapshot = new ClientSyncDatumSnapshot();
        snapshot.setClientSyncDataList(datum);
        byte[] data = ApplicationUtils.getBean(Serializer.class).serialize(snapshot);
        return new DistroData(new DistroKey(DataOperation.SNAPSHOT.name(), TYPE), data);
    }

    @Override
    public List<DistroData> getVerifyData() {
        List<DistroData> result = null;
        for (String each : clientManager.allClientId()) {
            Client client = clientManager.getClient(each);
            if (null == client || !client.isEphemeral()) {
                continue;
            }
            if (clientManager.isResponsibleClient(client)) {
                DistroClientVerifyInfo verifyData = new DistroClientVerifyInfo(client.getClientId(),
                        client.getRevision());
                DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
                DistroData data = new DistroData(distroKey,
                        ApplicationUtils.getBean(Serializer.class).serialize(verifyData));
                data.setType(DataOperation.VERIFY);
                if (result == null) {
                    result = new LinkedList<>();
                }
                result.add(data);
            }
        }
        return result;
    }
}
