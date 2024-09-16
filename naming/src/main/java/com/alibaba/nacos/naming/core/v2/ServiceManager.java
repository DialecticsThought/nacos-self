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

package com.alibaba.nacos.naming.core.v2;

import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.naming.core.v2.event.metadata.MetadataEvent;
import com.alibaba.nacos.naming.core.v2.pojo.Service;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Nacos service manager for v2.
 *
 * @author xiweng.yy
 */
public class ServiceManager {

// ServiceManager.getInstance()使用饿汉式单例返回一个ServiceManager对象
    private static final ServiceManager INSTANCE = new ServiceManager();
    /**
     * 键（Key）：Service，表示服务对象本身。这是服务的唯一标识，通常包括服务的名称、命名空间等信息。
     * 值（Value）：也是 Service，表示已经在系统中初始化和管理的服务对象。
     * 这个 Map 的主要目的是保证服务对象是单例的。
     * 每个 Service 对象作为键值对的键，也同时作为值。这样做可以保证在系统中访问服务时，始终得到的是唯一的、同一个 Service 实例
     * ，确保每个 Service 对象在整个系统中只会创建一次。
     */
    private final ConcurrentHashMap<Service, Service> singletonRepository;
    /**
     * 键（Key）：String，表示命名空间的名称。命名空间是 Nacos 中用于隔离和管理不同服务的一种逻辑分组方式。
     * 值（Value）：Set<Service>，表示属于该命名空间的所有服务对象。每个 Set 里包含该命名空间下的多个 Service 实例。
     */
    private final ConcurrentHashMap<String, Set<Service>> namespaceSingletonMaps;

    private ServiceManager() {
        singletonRepository = new ConcurrentHashMap<>(1 << 10);
        namespaceSingletonMaps = new ConcurrentHashMap<>(1 << 2);
    }

    public static ServiceManager getInstance() {
        return INSTANCE;
    }

    public Set<Service> getSingletons(String namespace) {
        return namespaceSingletonMaps.getOrDefault(namespace, new HashSet<>(1));
    }

    /**
     * Get singleton service. Put to manager if no singleton.
     *
     * @param service new service
     * @return if service is exist, return exist service, otherwise return new service
     */
    public Service getSingleton(Service service) {
        // 如果service在singletonRepository中找不到，则存入到singletonRepository中；否则返回已存在的Service
        // 怎么判断service是否已经存在，service重写了equal和hasCode方法，namespace+group+serviceName在服务端是一个单例Service
        Service result = singletonRepository.computeIfAbsent(service, key -> {
            NotifyCenter.publishEvent(new MetadataEvent.ServiceMetadataEvent(service, false));
            return service;
        });
        // namespaceSingletonMaps其实是按照NamespaceId来存放所有的Service
        // ConcurrentHashMap<String, Set<Service>> namespaceSingletonMaps;
        namespaceSingletonMaps.computeIfAbsent(result.getNamespace(), namespace -> new ConcurrentHashSet<>()).add(result);
        return result;
    }

    /**
     * Get singleton service if Exist.
     *
     * @param namespace namespace of service
     * @param group     group of service
     * @param name      name of service
     * @return singleton service if exist, otherwise null optional
     */
    public Optional<Service> getSingletonIfExist(String namespace, String group, String name) {
        return getSingletonIfExist(Service.newService(namespace, group, name));
    }

    /**
     * Get singleton service if Exist.
     *
     * @param service service template
     * @return singleton service if exist, otherwise null optional
     */
    public Optional<Service> getSingletonIfExist(Service service) {
        return Optional.ofNullable(singletonRepository.get(service));
    }

    public Set<String> getAllNamespaces() {
        return namespaceSingletonMaps.keySet();
    }

    /**
     * Remove singleton service.
     *
     * @param service service need to remove
     * @return removed service
     */
    public Service removeSingleton(Service service) {
        if (namespaceSingletonMaps.containsKey(service.getNamespace())) {
            namespaceSingletonMaps.get(service.getNamespace()).remove(service);
        }
        return singletonRepository.remove(service);
    }

    public boolean containSingleton(Service service) {
        return singletonRepository.containsKey(service);
    }

    public int size() {
        return singletonRepository.size();
    }
}
