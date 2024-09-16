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

package com.alibaba.nacos.core.remote;

import com.alibaba.nacos.api.remote.RequestCallBack;
import com.alibaba.nacos.api.remote.request.ClientDetectionRequest;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.common.remote.exception.ConnectionAlreadyClosedException;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.plugin.control.Loggers;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * nacos runtime connection ejector.
 *
 * @author shiyiyue
 */
public class NacosRuntimeConnectionEjector extends RuntimeConnectionEjector {

    public NacosRuntimeConnectionEjector() {

    }

    /**
     * eject connections on runtime.
     */
    public void doEject() {
        // remove out dated connection
        // TODO 进入
        ejectOutdatedConnection();
        // remove overload connection
        ejectOverLimitConnection();
    }

    /**
     * eject the outdated connection.
     * <pre>
     * TODO
     *   1、拿到当前所有的连接；
     *   2、循环判断每个连接，判断下最近一次活跃时间距离当前时间，是不是超过20s，
     *          如果超过20s，将连接ID加入到一个过期连接集合中放着；
     *   3、循环过期连接集合中的每个连接，Nacos服务端主动发起一个探活，如果探活成功，将连接ID加入到探活成功的集合中；
     *   4、比较过期连接集合、探活成功集合，两者的差集，就是真正探活失败，需要剔除的那些连接，将会执行注销连接操作；
     * </pre>
     */
    private void ejectOutdatedConnection() {
        try {

            Loggers.CONNECTION.info("Connection check task start");

            Map<String, Connection> connections = connectionManager.connections;
            int totalCount = connections.size();
            int currentSdkClientCount = connectionManager.currentSdkClientCount();

            Loggers.CONNECTION.info("Long connection metrics detail ,Total count ={}, sdkCount={},clusterCount={}",
                    totalCount, currentSdkClientCount, (totalCount - currentSdkClientCount));
            // 超时的连接集合
            Set<String> outDatedConnections = new HashSet<>();
            long now = System.currentTimeMillis();
            //outdated connections collect.
            for (Map.Entry<String, Connection> entry : connections.entrySet()) {
                Connection client = entry.getValue();
                // client.getMetaInfo().getLastActiveTime(): 客户端最近一次活跃时间
                // 客户端最近一次活跃时间距离当前时间超过20s的客户端，服务端会发起请求探活，如果失败或者超过指定时间内未响应则剔除服务
                if (now - client.getMetaInfo().getLastActiveTime() >= KEEP_ALIVE_TIME) {
                    outDatedConnections.add(client.getMetaInfo().getConnectionId());
                } else if (client.getMetaInfo().pushQueueBlockTimesLastOver(300 * 1000)) {
                    outDatedConnections.add(client.getMetaInfo().getConnectionId());
                }
            }

            // check out date connection
            Loggers.CONNECTION.info("Out dated connection ,size={}", outDatedConnections.size());
            if (CollectionUtils.isNotEmpty(outDatedConnections)) {
                // 记录成功探活的客户端连接的集合
                Set<String> successConnections = new HashSet<>();
                final CountDownLatch latch = new CountDownLatch(outDatedConnections.size());
                for (String outDateConnectionId : outDatedConnections) {
                    try {
                        Connection connection = connectionManager.getConnection(outDateConnectionId);
                        if (connection != null) {
                        // 创建一个客户端检测请求
                            ClientDetectionRequest clientDetectionRequest = new ClientDetectionRequest();
                            connection.asyncRequest(clientDetectionRequest, new RequestCallBack() {
                                @Override
                                public Executor getExecutor() {
                                    return null;
                                }

                                @Override
                                public long getTimeout() {
                                    return 5000L;
                                }

                                @Override
                                public void onResponse(Response response) {
                                    latch.countDown();
                                    if (response != null && response.isSuccess()) {
                                        // 探活成功，更新最近活跃时间，然后加入到探活成功的集合中
                                        connection.freshActiveTime();
                                        successConnections.add(outDateConnectionId);
                                    }
                                }

                                @Override
                                public void onException(Throwable e) {
                                    latch.countDown();
                                }
                            });

                            Loggers.CONNECTION.info("[{}]send connection active request ", outDateConnectionId);
                        } else {
                            latch.countDown();
                        }

                    } catch (ConnectionAlreadyClosedException e) {
                        latch.countDown();
                    } catch (Exception e) {
                        Loggers.CONNECTION.error("[{}]Error occurs when check client active detection ,error={}",
                                outDateConnectionId, e);
                        latch.countDown();
                    }
                }

                latch.await(5000L, TimeUnit.MILLISECONDS);
                Loggers.CONNECTION.info("Out dated connection check successCount={}", successConnections.size());

                for (String outDateConnectionId : outDatedConnections) {
                    // 不在探活成功的集合，说明探活失败，执行注销连接操作
                    if (!successConnections.contains(outDateConnectionId)) {
                        Loggers.CONNECTION.info("[{}]Unregister Out dated connection....", outDateConnectionId);
                        // 注销过期连接
                        // TODO 进入
                        connectionManager.unregister(outDateConnectionId);
                    }
                }
            }

            Loggers.CONNECTION.info("Connection check task end");

        } catch (Throwable e) {
            Loggers.CONNECTION.error("Error occurs during connection check... ", e);
        }
    }

    /**
     * eject the over limit connection.
     */
    private void ejectOverLimitConnection() {
        // if not count set, then give up
        if (getLoadClient() > 0) {
            try {
                Loggers.CONNECTION.info("Connection overLimit check task start, loadCount={}, redirectAddress={}",
                        getLoadClient(), getRedirectAddress());
                // check count
                int currentConnectionCount = connectionManager.getCurrentConnectionCount();
                int ejectingCount = currentConnectionCount - getLoadClient();
                // if overload
                if (ejectingCount > 0) {
                    // we may modify the connection map when connection reset
                    // avoid concurrent modified exception, create new set for ids snapshot
                    Set<String> ids = new HashSet<>(connectionManager.connections.keySet());
                    for (String id : ids) {
                        if (ejectingCount > 0) {
                            // check sdk
                            Connection connection = connectionManager.getConnection(id);
                            if (connection != null && connection.getMetaInfo().isSdkSource()) {
                                if (connectionManager.loadSingle(id, redirectAddress)) {
                                    ejectingCount--;
                                }
                            }
                        } else {
                            // reach the count
                            break;
                        }
                    }
                }
                Loggers.CONNECTION.info("Connection overLimit task end, current loadCount={}, has ejected loadCont={}",
                        connectionManager.getCurrentConnectionCount(), getLoadClient() - ejectingCount);
            } catch (Throwable e) {
                Loggers.CONNECTION.error("Error occurs during connection overLimit... ", e);
            }
            // reset
            setRedirectAddress(null);
            setLoadClient(-1);
        }
    }

    @Override
    public String getName() {
        return "nacos";
    }
}
