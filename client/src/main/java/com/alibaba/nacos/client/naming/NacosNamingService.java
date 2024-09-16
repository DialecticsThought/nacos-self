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

package com.alibaba.nacos.client.naming;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.api.selector.AbstractSelector;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.naming.cache.ServiceInfoHolder;
import com.alibaba.nacos.client.naming.core.Balancer;
import com.alibaba.nacos.client.naming.event.InstancesChangeEvent;
import com.alibaba.nacos.client.naming.event.InstancesChangeNotifier;
import com.alibaba.nacos.client.naming.remote.NamingClientProxy;
import com.alibaba.nacos.client.naming.remote.NamingClientProxyDelegate;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.InitUtils;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.client.utils.PreInitUtils;
import com.alibaba.nacos.client.utils.ValidatorUtils;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Nacos Naming Service.
 *
 * @author nkorange
 */
@SuppressWarnings("PMD.ServiceOrDaoClassShouldEndWithImplRule")
public class NacosNamingService implements NamingService {

    private static final String DEFAULT_NAMING_LOG_FILE_PATH = "naming.log";

    private static final String UP = "UP";

    private static final String DOWN = "DOWN";

    /**
     * Each Naming service should have different namespace.
     */
    private String namespace;

    @Deprecated
    private String logName;

    private ServiceInfoHolder serviceInfoHolder;

    private InstancesChangeNotifier changeNotifier;

    private NamingClientProxy clientProxy;

    private String notifierEventScope;

    public NacosNamingService(String serverList) throws NacosException {
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, serverList);
        init(properties);
    }

    public NacosNamingService(Properties properties) throws NacosException {
        init(properties);
    }

    private void init(Properties properties) throws NacosException {
        PreInitUtils.asyncPreLoadCostComponent();
        final NacosClientProperties nacosClientProperties = NacosClientProperties.PROTOTYPE.derive(properties);
        ValidatorUtils.checkInitParam(nacosClientProperties);
        this.namespace = InitUtils.initNamespaceForNaming(nacosClientProperties);
        InitUtils.initSerialization();
        InitUtils.initWebRootContext(nacosClientProperties);
        initLogName(nacosClientProperties);

        this.notifierEventScope = UUID.randomUUID().toString();
        this.changeNotifier = new InstancesChangeNotifier(this.notifierEventScope);
        NotifyCenter.registerToPublisher(InstancesChangeEvent.class, 16384);
        NotifyCenter.registerSubscriber(changeNotifier);
        this.serviceInfoHolder = new ServiceInfoHolder(namespace, this.notifierEventScope, nacosClientProperties);
        this.clientProxy = new NamingClientProxyDelegate(this.namespace, serviceInfoHolder, nacosClientProperties,
                changeNotifier);
    }

    @Deprecated
    private void initLogName(NacosClientProperties properties) {
        logName = properties.getProperty(UtilAndComs.NACOS_NAMING_LOG_NAME, DEFAULT_NAMING_LOG_FILE_PATH);
    }

    @Override
    public void registerInstance(String serviceName, String ip, int port) throws NacosException {
        registerInstance(serviceName, ip, port, Constants.DEFAULT_CLUSTER_NAME);
    }

    @Override
    public void registerInstance(String serviceName, String groupName, String ip, int port) throws NacosException {
        registerInstance(serviceName, groupName, ip, port, Constants.DEFAULT_CLUSTER_NAME);
    }

    @Override
    public void registerInstance(String serviceName, String ip, int port, String clusterName) throws NacosException {
        registerInstance(serviceName, Constants.DEFAULT_GROUP, ip, port, clusterName);
    }

    @Override
    public void registerInstance(String serviceName, String groupName, String ip, int port, String clusterName)
            throws NacosException {
        Instance instance = new Instance();
        instance.setIp(ip);
        instance.setPort(port);
        instance.setWeight(1.0);
        instance.setClusterName(clusterName);
        registerInstance(serviceName, groupName, instance);
    }

    @Override
    public void registerInstance(String serviceName, Instance instance) throws NacosException {
        registerInstance(serviceName, Constants.DEFAULT_GROUP, instance);
    }

    @Override
    public void registerInstance(String serviceName, String groupName, Instance instance) throws NacosException {
        NamingUtils.checkInstanceIsLegal(instance);
        checkAndStripGroupNamePrefix(instance, groupName);
        clientProxy.registerService(serviceName, groupName, instance);
    }

    @Override
    public void batchRegisterInstance(String serviceName, String groupName, List<Instance> instances)
            throws NacosException {
        NamingUtils.batchCheckInstanceIsLegal(instances);
        batchCheckAndStripGroupNamePrefix(instances, groupName);
        clientProxy.batchRegisterService(serviceName, groupName, instances);
    }

    @Override
    public void batchDeregisterInstance(String serviceName, String groupName, List<Instance> instances)
            throws NacosException {
        NamingUtils.batchCheckInstanceIsLegal(instances);
        batchCheckAndStripGroupNamePrefix(instances, groupName);
        clientProxy.batchDeregisterService(serviceName, groupName, instances);
    }

    @Override
    public void deregisterInstance(String serviceName, String ip, int port) throws NacosException {
        deregisterInstance(serviceName, ip, port, Constants.DEFAULT_CLUSTER_NAME);
    }

    @Override
    public void deregisterInstance(String serviceName, String groupName, String ip, int port) throws NacosException {
        deregisterInstance(serviceName, groupName, ip, port, Constants.DEFAULT_CLUSTER_NAME);
    }

    @Override
    public void deregisterInstance(String serviceName, String ip, int port, String clusterName) throws NacosException {
        deregisterInstance(serviceName, Constants.DEFAULT_GROUP, ip, port, clusterName);
    }

    @Override
    public void deregisterInstance(String serviceName, String groupName, String ip, int port, String clusterName)
            throws NacosException {
        Instance instance = new Instance();
        instance.setIp(ip);
        instance.setPort(port);
        instance.setClusterName(clusterName);
        deregisterInstance(serviceName, groupName, instance);
    }

    @Override
    public void deregisterInstance(String serviceName, Instance instance) throws NacosException {
        deregisterInstance(serviceName, Constants.DEFAULT_GROUP, instance);
    }

    @Override
    public void deregisterInstance(String serviceName, String groupName, Instance instance) throws NacosException {
        NamingUtils.checkInstanceIsLegal(instance);
        checkAndStripGroupNamePrefix(instance, groupName);
        clientProxy.deregisterService(serviceName, groupName, instance);
    }

    @Override
    public List<Instance> getAllInstances(String serviceName) throws NacosException {
        return getAllInstances(serviceName, new ArrayList<>());
    }

    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName) throws NacosException {
        return getAllInstances(serviceName, groupName, new ArrayList<>());
    }

    @Override
    public List<Instance> getAllInstances(String serviceName, boolean subscribe) throws NacosException {
        return getAllInstances(serviceName, new ArrayList<>(), subscribe);
    }

    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName, boolean subscribe)
            throws NacosException {
        return getAllInstances(serviceName, groupName, new ArrayList<>(), subscribe);
    }

    @Override
    public List<Instance> getAllInstances(String serviceName, List<String> clusters) throws NacosException {
        return getAllInstances(serviceName, clusters, true);
    }

    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName, List<String> clusters)
            throws NacosException {
        return getAllInstances(serviceName, groupName, clusters, true);
    }

    @Override
    public List<Instance> getAllInstances(String serviceName, List<String> clusters, boolean subscribe)
            throws NacosException {
        return getAllInstances(serviceName, Constants.DEFAULT_GROUP, clusters, subscribe);
    }

    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName, List<String> clusters,
                                          boolean subscribe) throws NacosException {
        List<Instance> list;
        ServiceInfo serviceInfo = getServiceInfo(serviceName, groupName, clusters, subscribe);
        if (serviceInfo == null || CollectionUtils.isEmpty(list = serviceInfo.getHosts())) {
            return new ArrayList<>();
        }
        return list;
    }

    /**
     * TODO com.alibaba.cloud.nacos.discovery.NacosServiceDiscovery#getInstances调用的是这个方法
     *
     * <pre>
     *   TODO
     *     如果是订阅模式，则直接从本地缓存获取服务信息（ServiceInfo），然后从中获取实例列表，
     *          这是因为订阅机制会自动同步服务器实例的变化到本地。如果本地缓存中没有，
     *                  那说明是首次调用，则进行订阅，在订阅完成后会获得到服务信息。
     *    如果是非订阅模式，那就直接请求服务器端，获得服务信息
     * </pre>
     *
     * @param serviceName name of service.
     * @param healthy     a flag to indicate returning healthy or unhealthy instances
     * @return
     * @throws NacosException
     */
    @Override
    public List<Instance> selectInstances(String serviceName, boolean healthy) throws NacosException {
        //TODO 查看
        return selectInstances(serviceName, new ArrayList<>(), healthy);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, String groupName, boolean healthy) throws NacosException {
        return selectInstances(serviceName, groupName, healthy, true);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, boolean healthy, boolean subscribe)
            throws NacosException {
        return selectInstances(serviceName, new ArrayList<>(), healthy, subscribe);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, String groupName, boolean healthy, boolean subscribe)
            throws NacosException {
        return selectInstances(serviceName, groupName, new ArrayList<>(), healthy, subscribe);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, List<String> clusters, boolean healthy)
            throws NacosException {
        // TODO 查看
        return selectInstances(serviceName, clusters, healthy, true);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, String groupName, List<String> clusters, boolean healthy)
            throws NacosException {
        return selectInstances(serviceName, groupName, clusters, healthy, true);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, List<String> clusters, boolean healthy, boolean subscribe)
            throws NacosException {
        // TODO 查看
        return selectInstances(serviceName, Constants.DEFAULT_GROUP, clusters, healthy, subscribe);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, String groupName, List<String> clusters, boolean healthy,
                                          boolean subscribe) throws NacosException {
        ServiceInfo serviceInfo = getServiceInfo(serviceName, groupName, clusters, subscribe);
        // TODO 查看
        // 筛选满足条件的实例
        return selectInstances(serviceInfo, healthy);
    }

    private List<Instance> selectInstances(ServiceInfo serviceInfo, boolean healthy) {
        List<Instance> list;
        if (serviceInfo == null || CollectionUtils.isEmpty(list = serviceInfo.getHosts())) {
            return new ArrayList<>();
        }

        Iterator<Instance> iterator = list.iterator();
        while (iterator.hasNext()) {
            Instance instance = iterator.next();
            // 判断 实例是否健康 || 可用 || 权重 <= 0
            if (healthy != instance.isHealthy() || !instance.isEnabled() || instance.getWeight() <= 0) {
                iterator.remove();
            }
        }
        // 返回最终实例
        return list;
    }

    private ServiceInfo getServiceInfoByFailover(String serviceName, String groupName, String clusterString) {
        return serviceInfoHolder.getFailoverServiceInfo(serviceName, groupName, clusterString);
    }

    private ServiceInfo getServiceInfoBySubscribe(String serviceName, String groupName, String clusterString,
                                                  boolean subscribe) throws NacosException {
        ServiceInfo serviceInfo;
        if (subscribe) {  // 是否订阅，默认是订阅的
            /**
             * 1.从缓存中获取ServiceInfo
             * ConcurrentMap<String, ServiceInfo> serviceInfoMap
             * key:  groupName@@serviceName  或者  groupName@@serviceName@@clusterString
             * value: ServiceInfo
             */
            // 示例：serviceName=discovery-provider   groupName=DEFAULT_GROUP
            serviceInfo = serviceInfoHolder.getServiceInfo(serviceName, groupName, clusterString);
            // 2.缓存为空，执行订阅服
            if (null == serviceInfo || !clientProxy.isSubscribed(serviceName, groupName, clusterString)) {
                serviceInfo = clientProxy.subscribe(serviceName, groupName, clusterString);
            }
        } else {
            // 3.非订阅，通过grpc发送ServiceQueryRequest服务查询请求
            serviceInfo = clientProxy.queryInstancesOfService(serviceName, groupName, clusterString, false);
        }
        return serviceInfo;
    }

    private ServiceInfo getServiceInfo(String serviceName, String groupName, List<String> clusters, boolean subscribe)
            throws NacosException {
        ServiceInfo serviceInfo;
        // 集群名称,使用逗号分隔
        String clusterString = StringUtils.join(clusters, ",");
        if (serviceInfoHolder.isFailoverSwitch()) {// 检查是否启用了 failover（故障转移） 机制
            // 如果是故障切换模式，客户端会尝试从故障转移文件中读取服务信息，而不是直接从 Nacos 服务器请求
            serviceInfo = getServiceInfoByFailover(serviceName, groupName, clusterString);
            /**
             * 如果 getServiceInfoByFailover 成功获取到服务信息（serviceInfo 不为空，且包含 hosts），
             * 则记录日志，并直接返回服务信息。
             * 这样，在发生故障时，客户端依旧可以使用上一次的服务信息继续工作
             */
            if (serviceInfo != null && serviceInfo.getHosts().size() > 0) {
                NAMING_LOGGER.debug("getServiceInfo from failover,serviceName: {}  data:{}", serviceName,
                        JacksonUtils.toJson(serviceInfo.getHosts()));
                return serviceInfo;
            }
        }
        // TODO 查看
        serviceInfo = getServiceInfoBySubscribe(serviceName, groupName, clusterString, subscribe);
        return serviceInfo;
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName) throws NacosException {
        return selectOneHealthyInstance(serviceName, new ArrayList<>());
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, String groupName) throws NacosException {
        return selectOneHealthyInstance(serviceName, groupName, true);
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, boolean subscribe) throws NacosException {
        return selectOneHealthyInstance(serviceName, new ArrayList<>(), subscribe);
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, String groupName, boolean subscribe)
            throws NacosException {
        return selectOneHealthyInstance(serviceName, groupName, new ArrayList<>(), subscribe);
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, List<String> clusters) throws NacosException {
        return selectOneHealthyInstance(serviceName, clusters, true);
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, String groupName, List<String> clusters)
            throws NacosException {
        return selectOneHealthyInstance(serviceName, groupName, clusters, true);
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, List<String> clusters, boolean subscribe)
            throws NacosException {
        return selectOneHealthyInstance(serviceName, Constants.DEFAULT_GROUP, clusters, subscribe);
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, String groupName, List<String> clusters,
                                             boolean subscribe) throws NacosException {
        ServiceInfo serviceInfo = getServiceInfo(serviceName, groupName, clusters, subscribe);
        return Balancer.RandomByWeight.selectHost(serviceInfo);
    }

    @Override
    public void subscribe(String serviceName, EventListener listener) throws NacosException {
        subscribe(serviceName, new ArrayList<>(), listener);
    }

    @Override
    public void subscribe(String serviceName, String groupName, EventListener listener) throws NacosException {
        subscribe(serviceName, groupName, new ArrayList<>(), listener);
    }

    @Override
    public void subscribe(String serviceName, List<String> clusters, EventListener listener) throws NacosException {
        subscribe(serviceName, Constants.DEFAULT_GROUP, clusters, listener);
    }

    @Override
    public void subscribe(String serviceName, String groupName, List<String> clusters, EventListener listener)
            throws NacosException {
        if (null == listener) {
            return;
        }
        String clusterString = StringUtils.join(clusters, ",");
        changeNotifier.registerListener(groupName, serviceName, clusterString, listener);
        clientProxy.subscribe(serviceName, groupName, clusterString);
    }

    @Override
    public void unsubscribe(String serviceName, EventListener listener) throws NacosException {
        unsubscribe(serviceName, new ArrayList<>(), listener);
    }

    @Override
    public void unsubscribe(String serviceName, String groupName, EventListener listener) throws NacosException {
        unsubscribe(serviceName, groupName, new ArrayList<>(), listener);
    }

    @Override
    public void unsubscribe(String serviceName, List<String> clusters, EventListener listener) throws NacosException {
        unsubscribe(serviceName, Constants.DEFAULT_GROUP, clusters, listener);
    }

    @Override
    public void unsubscribe(String serviceName, String groupName, List<String> clusters, EventListener listener)
            throws NacosException {
        String clustersString = StringUtils.join(clusters, ",");
        changeNotifier.deregisterListener(groupName, serviceName, clustersString, listener);
        if (!changeNotifier.isSubscribed(groupName, serviceName, clustersString)) {
            clientProxy.unsubscribe(serviceName, groupName, clustersString);
        }
    }

    @Override
    public ListView<String> getServicesOfServer(int pageNo, int pageSize) throws NacosException {
        return getServicesOfServer(pageNo, pageSize, Constants.DEFAULT_GROUP);
    }

    @Override
    public ListView<String> getServicesOfServer(int pageNo, int pageSize, String groupName) throws NacosException {
        return getServicesOfServer(pageNo, pageSize, groupName, null);
    }

    @Override
    public ListView<String> getServicesOfServer(int pageNo, int pageSize, AbstractSelector selector)
            throws NacosException {
        return getServicesOfServer(pageNo, pageSize, Constants.DEFAULT_GROUP, selector);
    }

    /**
     * TODO 查看
     *
     * @param pageNo    page index
     * @param pageSize  page size
     * @param groupName group name
     * @param selector  selector to filter the resource
     * @return
     * @throws NacosException
     */
    @Override
    public ListView<String> getServicesOfServer(int pageNo, int pageSize, String groupName, AbstractSelector selector)
            throws NacosException {
        // TODO 查看 NamingGrpcClientProxy#getServiceList
        return clientProxy.getServiceList(pageNo, pageSize, groupName, selector);
    }

    @Override
    public List<ServiceInfo> getSubscribeServices() {
        return changeNotifier.getSubscribeServices();
    }

    @Override
    public String getServerStatus() {
        return clientProxy.serverHealthy() ? UP : DOWN;
    }

    @Override
    public void shutDown() throws NacosException {
        serviceInfoHolder.shutdown();
        clientProxy.shutdown();
        NotifyCenter.deregisterSubscriber(changeNotifier);

    }

    private void batchCheckAndStripGroupNamePrefix(List<Instance> instances, String groupName) throws NacosException {
        for (Instance instance : instances) {
            checkAndStripGroupNamePrefix(instance, groupName);
        }
    }

    private void checkAndStripGroupNamePrefix(Instance instance, String groupName) throws NacosException {
        String serviceName = instance.getServiceName();
        if (NamingUtils.isServiceNameCompatibilityMode(serviceName)) {
            String groupNameOfInstance = NamingUtils.getGroupName(serviceName);
            if (!groupName.equals(groupNameOfInstance)) {
                throw new NacosException(NacosException.CLIENT_INVALID_PARAM, String.format(
                        "wrong group name prefix of instance service name! it should be: %s, Instance: %s", groupName, instance));
            }
            instance.setServiceName(NamingUtils.getServiceName(serviceName));
        }
    }
}
