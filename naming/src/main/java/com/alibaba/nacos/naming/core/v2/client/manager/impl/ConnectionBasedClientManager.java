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

package com.alibaba.nacos.naming.core.v2.client.manager.impl;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.remote.RemoteConstants;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.core.remote.ClientConnectionEventListener;
import com.alibaba.nacos.core.remote.Connection;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.v2.DistroClientVerifyInfo;
import com.alibaba.nacos.naming.constants.ClientConstants;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.client.ClientAttributes;
import com.alibaba.nacos.naming.core.v2.client.factory.ClientFactory;
import com.alibaba.nacos.naming.core.v2.client.factory.ClientFactoryHolder;
import com.alibaba.nacos.naming.core.v2.client.impl.ConnectionBasedClient;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManager;
import com.alibaba.nacos.naming.core.v2.event.client.ClientEvent;
import com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.Loggers;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * The manager of {@code ConnectionBasedClient}.
 *
 * @author xiweng.yy
 */
@Component("connectionBasedClientManager")
public class ConnectionBasedClientManager extends ClientConnectionEventListener implements ClientManager {

    private final ConcurrentMap<String, ConnectionBasedClient> clients = new ConcurrentHashMap<>();

    public ConnectionBasedClientManager() {
        // 启动了一个定时任务，无延迟，每隔5s执行一次
        // 具体就是执行ExpiredClientCleaner.run()方法
        //TODO 查看 ExpiredClientCleaner 是一个Runnable
        GlobalExecutor
                .scheduleExpiredClientCleaner(new ExpiredClientCleaner(this), 0, Constants.DEFAULT_HEART_BEAT_INTERVAL,
                        TimeUnit.MILLISECONDS);
    }

    @Override
    public void clientConnected(Connection connect) {
        if (!RemoteConstants.LABEL_MODULE_NAMING.equals(connect.getMetaInfo().getLabel(RemoteConstants.LABEL_MODULE))) {
            return;
        }
        ClientAttributes attributes = new ClientAttributes();
        attributes.addClientAttribute(ClientConstants.CONNECTION_TYPE, connect.getMetaInfo().getConnectType());
        attributes.addClientAttribute(ClientConstants.CONNECTION_METADATA, connect.getMetaInfo());
        clientConnected(connect.getMetaInfo().getConnectionId(), attributes);
    }

    @Override
    public boolean clientConnected(String clientId, ClientAttributes attributes) {
        String type = attributes.getClientAttribute(ClientConstants.CONNECTION_TYPE);
        ClientFactory clientFactory = ClientFactoryHolder.getInstance().findClientFactory(type);
        return clientConnected(clientFactory.newClient(clientId, attributes));
    }

    @Override
    public boolean clientConnected(final Client client) {
        clients.computeIfAbsent(client.getClientId(), s -> {
            Loggers.SRV_LOG.info("Client connection {} connect", client.getClientId());
            return (ConnectionBasedClient) client;
        });
        return true;
    }

    @Override
    public boolean syncClientConnected(String clientId, ClientAttributes attributes) {
        String type = attributes.getClientAttribute(ClientConstants.CONNECTION_TYPE);
        ClientFactory clientFactory = ClientFactoryHolder.getInstance().findClientFactory(type);
        return clientConnected(clientFactory.newSyncedClient(clientId, attributes));
    }

    @Override
    public void clientDisConnected(Connection connect) {
        // TODO 进入
        clientDisconnected(connect.getMetaInfo().getConnectionId());
    }

    @Override
    public boolean clientDisconnected(String clientId) {
        Loggers.SRV_LOG.info("Client connection {} disconnect, remove instances and subscribers", clientId);
        ConnectionBasedClient client = clients.remove(clientId);
        if (null == client) {
            return true;
        }
        client.release();
        boolean isResponsible = isResponsibleClient(client);
        /**
         *  发布客户端释放连接事件
         * 具体处理是在：{@link com.alibaba.nacos.naming.core.v2.index.ClientServiceIndexesManager.onEvent}  TODO 查看
         * 主要做了下面几个事情：
         * 1、从订阅者列表中移除所有服务对这个客户端的引用
         * 2、从发布者列表中移除所有服务对这个客户端的引用
         */
        NotifyCenter.publishEvent(new ClientOperationEvent.ClientReleaseEvent(client, isResponsible));

        /**
         * 发布客户端断开连接事件
         * 具体处理是在：{@link com.alibaba.nacos.naming.core.v2.metadata.NamingMetadataManager.onEvent} TODO 查看
         * 主要做了下面几个事情：
         * 1、将服务实例元数据添加到过期集合中
         */
        NotifyCenter.publishEvent(new ClientEvent.ClientDisconnectEvent(client, isResponsible));
        return true;
    }

    @Override
    public Client getClient(String clientId) {
        return clients.get(clientId);
    }

    @Override
    public boolean contains(String clientId) {
        return clients.containsKey(clientId);
    }

    @Override
    public Collection<String> allClientId() {
        return clients.keySet();
    }

    @Override
    public boolean isResponsibleClient(Client client) {
        return (client instanceof ConnectionBasedClient) && ((ConnectionBasedClient) client).isNative();
    }

    @Override
    public boolean verifyClient(DistroClientVerifyInfo verifyData) {
        ConnectionBasedClient client = clients.get(verifyData.getClientId());
        if (null != client) {
            // remote node of old version will always verify with zero revision
            if (0 == verifyData.getRevision() || client.getRevision() == verifyData.getRevision()) {
                client.setLastRenewTime();
                return true;
            } else {
                Loggers.DISTRO.info("[DISTRO-VERIFY-FAILED] ConnectionBasedClient[{}] revision local={}, remote={}",
                        client.getClientId(), client.getRevision(), verifyData.getRevision());
            }
        }
        return false;
    }

    private static class ExpiredClientCleaner implements Runnable {

        private final ConnectionBasedClientManager clientManager;

        public ExpiredClientCleaner(ConnectionBasedClientManager clientManager) {
            this.clientManager = clientManager;
        }

        @Override
        public void run() {
            long currentTime = System.currentTimeMillis();
            for (String each : clientManager.allClientId()) {
                // 判断客户端是否超时
                ConnectionBasedClient client = (ConnectionBasedClient) clientManager.getClient(each);
                if (null != client && client.isExpire(currentTime)) {
                    // 超时连接处理
                    clientManager.clientDisconnected(each);
                }
            }
        }
    }
}
