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

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.naming.core.v2.ServiceManager;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManager;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManagerDelegate;
import com.alibaba.nacos.naming.core.v2.metadata.InstanceMetadata;
import com.alibaba.nacos.naming.core.v2.metadata.NamingMetadataManager;
import com.alibaba.nacos.naming.core.v2.pojo.BatchInstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.utils.InstanceUtil;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Service storage.
 *
 * @author xiweng.yy
 */
@Component
public class ServiceStorage {

    private final ClientServiceIndexesManager serviceIndexesManager;

    private final ClientManager clientManager;

    private final SwitchDomain switchDomain;

    private final NamingMetadataManager metadataManager;

    private final ConcurrentMap<Service, ServiceInfo> serviceDataIndexes;

    private final ConcurrentMap<Service, Set<String>> serviceClusterIndex;

    public ServiceStorage(ClientServiceIndexesManager serviceIndexesManager, ClientManagerDelegate clientManager,
            SwitchDomain switchDomain, NamingMetadataManager metadataManager) {
        this.serviceIndexesManager = serviceIndexesManager;
        this.clientManager = clientManager;
        this.switchDomain = switchDomain;
        this.metadataManager = metadataManager;
        this.serviceDataIndexes = new ConcurrentHashMap<>();
        this.serviceClusterIndex = new ConcurrentHashMap<>();
    }

    public Set<String> getClusters(Service service) {
        return serviceClusterIndex.getOrDefault(service, new HashSet<>());
    }

    public ServiceInfo getData(Service service) {
        // TODO 查看 getPushData
        return serviceDataIndexes.containsKey(service) ? serviceDataIndexes.get(service) : getPushData(service);
    }

    public ServiceInfo getPushData(Service service) {
        ServiceInfo result = emptyServiceInfo(service);
        if (!ServiceManager.getInstance().containSingleton(service)) {
            return result;
        }
        // 获取在注册表中的服务【前面服务注册时候讲到过】
        Service singleton = ServiceManager.getInstance().getSingleton(service);
        // 这个 hosts 其实就是实例列表 ☆ ☆ ☆ ☆ ☆ ☆ ☆
        // TODO 进入
        result.setHosts(getAllInstancesFromIndex(singleton));
        serviceDataIndexes.put(singleton, result);
        return result;
    }

    public void removeData(Service service) {
        serviceDataIndexes.remove(service);
        serviceClusterIndex.remove(service);
    }

    private ServiceInfo emptyServiceInfo(Service service) {
        ServiceInfo result = new ServiceInfo();
        result.setName(service.getName());
        result.setGroupName(service.getGroup());
        result.setLastRefTime(System.currentTimeMillis());
        result.setCacheMillis(switchDomain.getDefaultPushCacheMillis());
        return result;
    }

    private List<Instance> getAllInstancesFromIndex(Service service) {
        // 用来存放 五福实例对象的
        Set<Instance> result = new HashSet<>();
        // 用来存放对应集群的
        Set<String> clusters = new HashSet<>();
        // 这里获取服务下面所有的 clientId 【用 each 接收了】
        for (String each : serviceIndexesManager.getAllClientsRegisteredService(service)) {
            // 从每个 clientId 中获取  IpPortBasedClient 信息 具体代码下面贴上了
            Optional<InstancePublishInfo> instancePublishInfo = getInstanceInfo(each, service);
            if (instancePublishInfo.isPresent()) {
                InstancePublishInfo publishInfo = instancePublishInfo.get();
                //If it is a BatchInstancePublishInfo type, it will be processed manually and added to the instance list
                if (publishInfo instanceof BatchInstancePublishInfo) {
                    BatchInstancePublishInfo batchInstancePublishInfo = (BatchInstancePublishInfo) publishInfo;
                    List<Instance> batchInstance = parseBatchInstance(service, batchInstancePublishInfo, clusters);
                    result.addAll(batchInstance);
                } else {
                    // 转换成 服务实例对象，装载 metadata 信息
                    Instance instance = parseInstance(service, instancePublishInfo.get());
                    result.add(instance);
                    clusters.add(instance.getClusterName());
                }
            }
        }
        // cache clusters of this service
        serviceClusterIndex.put(service, clusters);
        return new LinkedList<>(result);
    }

    /**
     * Parse batch instance.
     * @param service service
     * @param batchInstancePublishInfo batchInstancePublishInfo
     * @return batch instance list
     */
    private List<Instance> parseBatchInstance(Service service, BatchInstancePublishInfo batchInstancePublishInfo, Set<String> clusters) {
        List<Instance> resultInstanceList = new ArrayList<>();
        List<InstancePublishInfo> instancePublishInfos = batchInstancePublishInfo.getInstancePublishInfos();
        for (InstancePublishInfo instancePublishInfo : instancePublishInfos) {
            Instance instance = parseInstance(service, instancePublishInfo);
            resultInstanceList.add(instance);
            clusters.add(instance.getClusterName());
        }
        return resultInstanceList;
    }

    private Optional<InstancePublishInfo> getInstanceInfo(String clientId, Service service) {
        // 从map中根据id得到相应客户端
        Client client = clientManager.getClient(clientId);
        if (null == client) {
            return Optional.empty();
        }
        // 返回InstancePublishInfo
        return Optional.ofNullable(client.getInstancePublishInfo(service));
    }

    private Instance parseInstance(Service service, InstancePublishInfo instanceInfo) {
        // TODO 查看
        Instance result = InstanceUtil.parseToApiInstance(service, instanceInfo);
        Optional<InstanceMetadata> metadata = metadataManager
                .getInstanceMetadata(service, instanceInfo.getMetadataId());
        metadata.ifPresent(instanceMetadata -> InstanceUtil.updateInstanceMetadata(result, instanceMetadata));
        return result;
    }
}
