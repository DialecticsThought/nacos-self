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

package com.alibaba.nacos.client.config.impl;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.config.ConfigType;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.config.remote.request.ClientConfigMetricRequest;
import com.alibaba.nacos.api.config.remote.request.ConfigBatchListenRequest;
import com.alibaba.nacos.api.config.remote.request.ConfigChangeNotifyRequest;
import com.alibaba.nacos.api.config.remote.request.ConfigPublishRequest;
import com.alibaba.nacos.api.config.remote.request.ConfigQueryRequest;
import com.alibaba.nacos.api.config.remote.request.ConfigRemoveRequest;
import com.alibaba.nacos.api.config.remote.response.ClientConfigMetricResponse;
import com.alibaba.nacos.api.config.remote.response.ConfigChangeBatchListenResponse;
import com.alibaba.nacos.api.config.remote.response.ConfigChangeNotifyResponse;
import com.alibaba.nacos.api.config.remote.response.ConfigPublishResponse;
import com.alibaba.nacos.api.config.remote.response.ConfigQueryResponse;
import com.alibaba.nacos.api.config.remote.response.ConfigRemoveResponse;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.remote.RemoteConstants;
import com.alibaba.nacos.api.remote.request.Request;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.client.config.common.GroupKey;
import com.alibaba.nacos.client.config.filter.impl.ConfigFilterChainManager;
import com.alibaba.nacos.client.config.filter.impl.ConfigResponse;
import com.alibaba.nacos.client.config.utils.ContentUtils;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.env.SourceType;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.utils.AppNameUtils;
import com.alibaba.nacos.client.utils.EnvUtil;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.client.utils.ParamUtil;
import com.alibaba.nacos.client.utils.TenantUtil;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.labels.impl.DefaultLabelsCollectorManager;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.remote.ConnectionType;
import com.alibaba.nacos.common.remote.client.Connection;
import com.alibaba.nacos.common.remote.client.ConnectionEventListener;
import com.alibaba.nacos.common.remote.client.RpcClient;
import com.alibaba.nacos.common.remote.client.RpcClientFactory;
import com.alibaba.nacos.common.remote.client.RpcClientTlsConfig;
import com.alibaba.nacos.common.remote.client.ServerListFactory;
import com.alibaba.nacos.common.utils.ConnLabelsUtils;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;
import com.alibaba.nacos.common.utils.VersionUtils;
import com.alibaba.nacos.plugin.auth.api.RequestResource;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.slf4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.nacos.api.common.Constants.APP_CONN_PREFIX;
import static com.alibaba.nacos.api.common.Constants.ENCODE;

/**
 * Long polling.
 *
 * @author Nacos
 */
public class ClientWorker implements Closeable {

    private static final Logger LOGGER = LogUtils.logger(ClientWorker.class);

    private static final String NOTIFY_HEADER = "notify";

    private static final String TAG_PARAM = "tag";

    private static final String APP_NAME_PARAM = "appName";

    private static final String BETAIPS_PARAM = "betaIps";

    private static final String TYPE_PARAM = "type";

    private static final String ENCRYPTED_DATA_KEY_PARAM = "encryptedDataKey";

    /**
     * groupKey -> cacheData.
     */
    private final AtomicReference<Map<String, CacheData>> cacheMap = new AtomicReference<>(new HashMap<>());

    private Map<String, String> appLables = new HashMap<>();

    private final ConfigFilterChainManager configFilterChainManager;

    private final String uuid = UUID.randomUUID().toString();

    private long timeout;

    private final ConfigRpcTransportClient agent;

    private int taskPenaltyTime;

    private boolean enableRemoteSyncConfig = false;

    private static final int MIN_THREAD_NUM = 2;

    private static final int THREAD_MULTIPLE = 1;

    /**
     * index(taskId)-> total cache count for this taskId.
     */
    private final List<AtomicInteger> taskIdCacheCountList = new ArrayList<>();

    /**
     * Add listeners for data.
     *
     * @param dataId    dataId of data
     * @param group     group of data
     * @param listeners listeners
     */
    public void addListeners(String dataId, String group, List<? extends Listener> listeners) throws NacosException {
        group = blank2defaultGroup(group);
        CacheData cache = addCacheDataIfAbsent(dataId, group);
        synchronized (cache) {
            for (Listener listener : listeners) {
                cache.addListener(listener);
            }
            cache.setDiscard(false);
            cache.setConsistentWithServer(false);
            // make sure cache exists in cacheMap
            if (getCache(dataId, group) != cache) {
                putCache(GroupKey.getKey(dataId, group), cache);
            }
            agent.notifyListenConfig();
        }
    }

    /**
     * Add listeners for tenant.
     *
     * @param dataId    dataId of data
     * @param group     group of data
     * @param listeners listeners
     * @throws NacosException nacos exception
     */
    public void addTenantListeners(String dataId, String group, List<? extends Listener> listeners)
            throws NacosException {
        // 如果为空，取默认组名DEFAULT_GROUP
        group = blank2defaultGroup(group);
        // 命名空间ID
        String tenant = agent.getTenant();
        // 根据dataId,group和tenant获取一个cacheData
        // TODO 进入
        CacheData cache = addCacheDataIfAbsent(dataId, group, tenant);
        // TODO 遍历传入的 listeners 列表，将每个监听器添加到 CacheData 中
        // 可以看到，一个配置文件可以有多个监听器
        synchronized (cache) {
            // 添加监听器
            for (Listener listener : listeners) {
                cache.addListener(listener);
            }
            // 非丢弃，删除类型
            // TODO 丢弃可能意味着此配置数据在某些情况下（例如未能成功同步到服务器）不再可用
            cache.setDiscard(false);
            // 未同步到服务端
            //  TODO 这个字段用于表示缓存的配置数据与服务器中的配置数据不一致。
            //   也就是说，可能有新的数据尚未同步到服务器，或者客户端的配置数据已经过期
            cache.setConsistentWithServer(false);
            // ensure cache present in cacheMap
            if (getCache(dataId, group, tenant) != cache) {
                putCache(GroupKey.getKeyTenant(dataId, group, tenant), cache);
            }
            // 处理配置类
            agent.notifyListenConfig();
        }

    }

    /**
     * Add listeners for tenant with content.
     *
     * @param dataId           dataId of data
     * @param group            group of data
     * @param content          content
     * @param encryptedDataKey encryptedDataKey
     * @param listeners        listeners
     * @throws NacosException nacos exception
     */
    public void addTenantListenersWithContent(String dataId, String group, String content, String encryptedDataKey,
                                              List<? extends Listener> listeners) throws NacosException {
        group = blank2defaultGroup(group);
        String tenant = agent.getTenant();
        CacheData cache = addCacheDataIfAbsent(dataId, group, tenant);
        synchronized (cache) {
            cache.setEncryptedDataKey(encryptedDataKey);
            cache.setContent(content);
            for (Listener listener : listeners) {
                cache.addListener(listener);
            }
            cache.setDiscard(false);
            cache.setConsistentWithServer(false);
            // make sure cache exists in cacheMap
            if (getCache(dataId, group, tenant) != cache) {
                putCache(GroupKey.getKeyTenant(dataId, group, tenant), cache);
            }
            agent.notifyListenConfig();
        }

    }

    /**
     * Remove listener.
     *
     * @param dataId   dataId of data
     * @param group    group of data
     * @param listener listener
     */
    public void removeListener(String dataId, String group, Listener listener) {
        group = blank2defaultGroup(group);
        CacheData cache = getCache(dataId, group);
        if (null != cache) {
            synchronized (cache) {
                cache.removeListener(listener);
                if (cache.getListeners().isEmpty()) {
                    cache.setConsistentWithServer(false);
                    cache.setDiscard(true);
                    agent.removeCache(dataId, group);
                }
            }

        }
    }

    /**
     * Remove listeners for tenant.
     *
     * @param dataId   dataId of data
     * @param group    group of data
     * @param listener listener
     */
    public void removeTenantListener(String dataId, String group, Listener listener) {
        group = blank2defaultGroup(group);
        String tenant = agent.getTenant();
        CacheData cache = getCache(dataId, group, tenant);
        if (null != cache) {
            synchronized (cache) {
                cache.removeListener(listener);
                if (cache.getListeners().isEmpty()) {
                    cache.setConsistentWithServer(false);
                    cache.setDiscard(true);
                    agent.removeCache(dataId, group);
                }
            }
        }
    }

    void removeCache(String dataId, String group, String tenant) {
        String groupKey = GroupKey.getKeyTenant(dataId, group, tenant);
        synchronized (cacheMap) {
            Map<String, CacheData> copy = new HashMap<>(cacheMap.get());
            CacheData remove = copy.remove(groupKey);
            if (remove != null) {
                decreaseTaskIdCount(remove.getTaskId());
            }
            cacheMap.set(copy);
        }
        LOGGER.info("[{}] [unsubscribe] {}", agent.getName(), groupKey);

        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.get().size());
    }

    /**
     * remove config.
     *
     * @param dataId dataId.
     * @param group  group.
     * @param tenant tenant.
     * @param tag    tag.
     * @return success or not.
     * @throws NacosException exception to throw.
     */
    public boolean removeConfig(String dataId, String group, String tenant, String tag) throws NacosException {
        return agent.removeConfig(dataId, group, tenant, tag);
    }

    /**
     * publish config.
     *
     * @param dataId  dataId.
     * @param group   group.
     * @param tenant  tenant.
     * @param appName appName.
     * @param tag     tag.
     * @param betaIps betaIps.
     * @param content content.
     * @param casMd5  casMd5.
     * @param type    type.
     * @return success or not.
     * @throws NacosException exception throw.
     */
    public boolean publishConfig(String dataId, String group, String tenant, String appName, String tag, String betaIps,
                                 String content, String encryptedDataKey, String casMd5, String type) throws NacosException {
        return agent.publishConfig(dataId, group, tenant, appName, tag, betaIps, content, encryptedDataKey, casMd5,
                type);
    }

    /**
     * Add cache data if absent.
     *
     * @param dataId data id if data
     * @param group  group of data
     * @return cache data
     */
    public CacheData addCacheDataIfAbsent(String dataId, String group) {
        CacheData cache = getCache(dataId, group);
        if (null != cache) {
            return cache;
        }

        String key = GroupKey.getKey(dataId, group);
        cache = new CacheData(configFilterChainManager, agent.getName(), dataId, group);

        synchronized (cacheMap) {
            CacheData cacheFromMap = getCache(dataId, group);
            // multiple listeners on the same dataid+group and race condition,so double check again
            //other listener thread beat me to set to cacheMap
            if (null != cacheFromMap) {
                cache = cacheFromMap;
                //reset so that server not hang this check
                cache.setInitializing(true);
            } else {
                int taskId = calculateTaskId();
                increaseTaskIdCount(taskId);
                cache.setTaskId(taskId);
            }

            Map<String, CacheData> copy = new HashMap<>(cacheMap.get());
            copy.put(key, cache);
            cacheMap.set(copy);
        }

        LOGGER.info("[{}] [subscribe] {}", this.agent.getName(), key);

        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.get().size());

        return cache;
    }

    /**
     * Add cache data if absent.
     *
     * @param dataId data id if data
     * @param group  group of data
     * @param tenant tenant of data
     * @return cache data
     * <p>
     * TODO  ClientWorker#addCacheDataIfAbsent 方法先从 cacheMap 中获取 CacheData 对象，
     *   如果 cacheMap 中没有，就创建一个 CacheData 对象，并加入到 cacheMap 缓存中。
     *   至此需要处理更新的配置都注册了监听器，一旦远程配置中心的配置发生了变化，对应的监听器就会做响应的处理。
     */
    public CacheData addCacheDataIfAbsent(String dataId, String group, String tenant) throws NacosException {
        // 从缓存中获取cacheData
        CacheData cache = getCache(dataId, group, tenant);
        if (null != cache) {
            // 缓存中存在，则直接返回
            return cache;
        }
        String key = GroupKey.getKeyTenant(dataId, group, tenant);
        synchronized (cacheMap) {
            CacheData cacheFromMap = getCache(dataId, group, tenant);
            // multiple listeners on the same dataid+group and race condition,so
            // double check again
            // other listener thread beat me to set to cacheMap


            if (null != cacheFromMap) { // 双重检查
                cache = cacheFromMap;
                // reset so that server not hang this check
                cache.setInitializing(true);
            } else {  // 缓存为空，直接构造一个CacheData
                cache = new CacheData(configFilterChainManager, agent.getName(), dataId, group, tenant);
                int taskId = calculateTaskId();
                increaseTaskIdCount(taskId);
                cache.setTaskId(taskId);
                // fix issue # 1317
                if (enableRemoteSyncConfig) {
                    ConfigResponse response = getServerConfig(dataId, group, tenant, 3000L, false);
                    cache.setEncryptedDataKey(response.getEncryptedDataKey());
                    cache.setContent(response.getContent());
                }
            }
            // 写时复制思想
            Map<String, CacheData> copy = new HashMap<>(this.cacheMap.get());
            // 存入缓存
            copy.put(key, cache);
            cacheMap.set(copy);
        }
        LOGGER.info("[{}] [subscribe] {}", agent.getName(), key);

        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.get().size());

        return cache;
    }

    /**
     * Put cache.
     *
     * @param key   groupKey
     * @param cache cache
     */
    private void putCache(String key, CacheData cache) {
        synchronized (cacheMap) {
            Map<String, CacheData> copy = new HashMap<>(this.cacheMap.get());
            copy.put(key, cache);
            cacheMap.set(copy);
        }
    }

    private void increaseTaskIdCount(int taskId) {
        taskIdCacheCountList.get(taskId).incrementAndGet();
    }

    private void decreaseTaskIdCount(int taskId) {
        taskIdCacheCountList.get(taskId).decrementAndGet();
    }

    private int calculateTaskId() {
        int perTaskSize = (int) ParamUtil.getPerTaskConfigSize();
        for (int index = 0; index < taskIdCacheCountList.size(); index++) {
            if (taskIdCacheCountList.get(index).get() < perTaskSize) {
                return index;
            }
        }
        taskIdCacheCountList.add(new AtomicInteger(0));
        return taskIdCacheCountList.size() - 1;
    }

    public CacheData getCache(String dataId, String group) {
        return getCache(dataId, group, TenantUtil.getUserTenantForAcm());
    }

    public CacheData getCache(String dataId, String group, String tenant) {
        if (null == dataId || null == group) {
            throw new IllegalArgumentException();
        }
        return cacheMap.get().get(GroupKey.getKeyTenant(dataId, group, tenant));
    }

    public ConfigResponse getServerConfig(String dataId, String group, String tenant, long readTimeout, boolean notify)
            throws NacosException {
        if (StringUtils.isBlank(group)) {
            // group 如果为空 设置为 DEFAULT_GROUP
            group = Constants.DEFAULT_GROUP;
        }
        // TODO 进入
        return this.agent.queryConfig(dataId, group, tenant, readTimeout, notify);
    }

    private String blank2defaultGroup(String group) {
        return StringUtils.isBlank(group) ? Constants.DEFAULT_GROUP : group.trim();
    }

    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    public ClientWorker(final ConfigFilterChainManager configFilterChainManager, ServerListManager serverListManager,
                        final NacosClientProperties properties) throws NacosException {
        this.configFilterChainManager = configFilterChainManager;

        // 初始化timeout、taskPenaltyTime、enableRemoteSyncConfig属性
        init(properties);
        // 创建一个用于配置服务端的Rpc通信客户端
        agent = new ConfigRpcTransportClient(properties, serverListManager);
        // 初始化一个线程池
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(initWorkerThreadCount(properties),
                new NameThreadFactory("com.alibaba.nacos.client.Worker"));
        // 配置了一个异步处理线程池
        agent.setExecutor(executorService);
        // 调用start方法
        // TODO 进入
        agent.start();

    }

    void initAppLabels(Properties properties) {
        this.appLables = ConnLabelsUtils.addPrefixForEachKey(defaultLabelsCollectorManager.getLabels(properties),
                APP_CONN_PREFIX);
    }

    private int initWorkerThreadCount(NacosClientProperties properties) {
        int count = ThreadUtils.getSuitableThreadCount(THREAD_MULTIPLE);
        if (properties == null) {
            return count;
        }
        count = Math.min(count, properties.getInteger(PropertyKeyConst.CLIENT_WORKER_MAX_THREAD_COUNT, count));
        count = Math.max(count, MIN_THREAD_NUM);
        return properties.getInteger(PropertyKeyConst.CLIENT_WORKER_THREAD_COUNT, count);
    }

    private void init(NacosClientProperties properties) {

        timeout = Math.max(ConvertUtils.toInt(properties.getProperty(PropertyKeyConst.CONFIG_LONG_POLL_TIMEOUT),
                Constants.CONFIG_LONG_POLL_TIMEOUT), Constants.MIN_CONFIG_LONG_POLL_TIMEOUT);

        taskPenaltyTime = ConvertUtils.toInt(properties.getProperty(PropertyKeyConst.CONFIG_RETRY_TIME),
                Constants.CONFIG_RETRY_TIME);

        this.enableRemoteSyncConfig = Boolean.parseBoolean(
                properties.getProperty(PropertyKeyConst.ENABLE_REMOTE_SYNC_CONFIG));
        initAppLabels(properties.getProperties(SourceType.PROPERTIES));
    }

    Map<String, Object> getMetrics(List<ClientConfigMetricRequest.MetricsKey> metricsKeys) {
        Map<String, Object> metric = new HashMap<>(16);
        metric.put("listenConfigSize", String.valueOf(this.cacheMap.get().size()));
        metric.put("clientVersion", VersionUtils.getFullClientVersion());
        metric.put("snapshotDir", LocalConfigInfoProcessor.LOCAL_SNAPSHOT_PATH);
        boolean isFixServer = agent.serverListManager.isFixed;
        metric.put("isFixedServer", isFixServer);
        metric.put("addressUrl", agent.serverListManager.addressServerUrl);
        metric.put("serverUrls", agent.serverListManager.getUrlString());

        Map<ClientConfigMetricRequest.MetricsKey, Object> metricValues = getMetricsValue(metricsKeys);
        metric.put("metricValues", metricValues);
        Map<String, Object> metrics = new HashMap<>(1);
        metrics.put(uuid, JacksonUtils.toJson(metric));
        return metrics;
    }

    private Map<ClientConfigMetricRequest.MetricsKey, Object> getMetricsValue(
            List<ClientConfigMetricRequest.MetricsKey> metricsKeys) {
        if (metricsKeys == null) {
            return null;
        }
        Map<ClientConfigMetricRequest.MetricsKey, Object> values = new HashMap<>(16);
        for (ClientConfigMetricRequest.MetricsKey metricsKey : metricsKeys) {
            if (ClientConfigMetricRequest.MetricsKey.CACHE_DATA.equals(metricsKey.getType())) {
                CacheData cacheData = cacheMap.get().get(metricsKey.getKey());
                values.putIfAbsent(metricsKey,
                        cacheData == null ? null : cacheData.getContent() + ":" + cacheData.getMd5());
            }
            if (ClientConfigMetricRequest.MetricsKey.SNAPSHOT_DATA.equals(metricsKey.getType())) {
                String[] configStr = GroupKey.parseKey(metricsKey.getKey());
                String snapshot = LocalConfigInfoProcessor.getSnapshot(this.agent.getName(), configStr[0], configStr[1],
                        configStr[2]);
                values.putIfAbsent(metricsKey,
                        snapshot == null ? null : snapshot + ":" + MD5Utils.md5Hex(snapshot, ENCODE));
            }
        }
        return values;
    }

    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        LOGGER.info("{} do shutdown begin", className);
        if (agent != null) {
            agent.shutdown();
        }
        LOGGER.info("{} do shutdown stop", className);
    }

    /**
     * check if it has any connectable server endpoint.
     *
     * @return true: that means has atleast one connected rpc client. flase: that means does not have any connected rpc
     * client.
     */
    public boolean isHealthServer() {
        return agent.isHealthServer();
    }

    private static DefaultLabelsCollectorManager defaultLabelsCollectorManager = new DefaultLabelsCollectorManager();

    public class ConfigRpcTransportClient extends ConfigTransportClient {

        Map<String, ExecutorService> multiTaskExecutor = new HashMap<>();

        private final BlockingQueue<Object> listenExecutebell = new ArrayBlockingQueue<>(1);

        private final Object bellItem = new Object();

        private long lastAllSyncTime = System.currentTimeMillis();

        Subscriber subscriber = null;

        /**
         * 3 minutes to check all listen cache keys.
         */
        private static final long ALL_SYNC_INTERNAL = 3 * 60 * 1000L;

        public ConfigRpcTransportClient(NacosClientProperties properties, ServerListManager serverListManager) {
            super(properties, serverListManager);
        }

        private ConnectionType getConnectionType() {
            return ConnectionType.GRPC;
        }

        @Override
        public void shutdown() throws NacosException {
            super.shutdown();
            synchronized (RpcClientFactory.getAllClientEntries()) {
                LOGGER.info("Trying to shutdown transport client {}", this);
                Set<Map.Entry<String, RpcClient>> allClientEntries = RpcClientFactory.getAllClientEntries();
                Iterator<Map.Entry<String, RpcClient>> iterator = allClientEntries.iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, RpcClient> entry = iterator.next();
                    if (entry.getKey().startsWith(uuid)) {
                        LOGGER.info("Trying to shutdown rpc client {}", entry.getKey());

                        try {
                            entry.getValue().shutdown();
                        } catch (NacosException nacosException) {
                            nacosException.printStackTrace();
                        }
                        LOGGER.info("Remove rpc client {}", entry.getKey());
                        iterator.remove();
                    }
                }

                LOGGER.info("Shutdown executor {}", executor);
                executor.shutdown();
                Map<String, CacheData> stringCacheDataMap = cacheMap.get();
                for (Map.Entry<String, CacheData> entry : stringCacheDataMap.entrySet()) {
                    entry.getValue().setConsistentWithServer(false);
                }
                if (subscriber != null) {
                    NotifyCenter.deregisterSubscriber(subscriber);
                }
            }

        }

        private Map<String, String> getLabels() {

            Map<String, String> labels = new HashMap<>(2, 1);
            labels.put(RemoteConstants.LABEL_SOURCE, RemoteConstants.LABEL_SOURCE_SDK);
            labels.put(RemoteConstants.LABEL_MODULE, RemoteConstants.LABEL_MODULE_CONFIG);
            labels.put(Constants.APPNAME, AppNameUtils.getAppName());
            if (EnvUtil.getSelfVipserverTag() != null) {
                labels.put(Constants.VIPSERVER_TAG, EnvUtil.getSelfVipserverTag());
            }
            if (EnvUtil.getSelfAmoryTag() != null) {
                labels.put(Constants.AMORY_TAG, EnvUtil.getSelfAmoryTag());
            }
            if (EnvUtil.getSelfLocationTag() != null) {
                labels.put(Constants.LOCATION_TAG, EnvUtil.getSelfLocationTag());
            }

            labels.putAll(appLables);
            return labels;
        }

        ConfigChangeNotifyResponse handleConfigChangeNotifyRequest(ConfigChangeNotifyRequest configChangeNotifyRequest,
                                                                   String clientName) {
            LOGGER.info("[{}] [server-push] config changed. dataId={}, group={},tenant={}", clientName,
                    configChangeNotifyRequest.getDataId(), configChangeNotifyRequest.getGroup(),
                    configChangeNotifyRequest.getTenant());
            String groupKey = GroupKey.getKeyTenant(configChangeNotifyRequest.getDataId(),
                    configChangeNotifyRequest.getGroup(), configChangeNotifyRequest.getTenant());

            CacheData cacheData = cacheMap.get().get(groupKey);
            if (cacheData != null) {
                synchronized (cacheData) {
                    cacheData.getReceiveNotifyChanged().set(true);
                    cacheData.setConsistentWithServer(false);
                    notifyListenConfig();
                }

            }
            return new ConfigChangeNotifyResponse();
        }

        ClientConfigMetricResponse handleClientMetricsRequest(ClientConfigMetricRequest configMetricRequest) {
            ClientConfigMetricResponse response = new ClientConfigMetricResponse();
            response.setMetrics(getMetrics(configMetricRequest.getMetricsKeys()));
            return response;
        }

        private void initRpcClientHandler(final RpcClient rpcClientInner) {
            /*
             * Register Config Change /Config ReSync Handler
             */
            rpcClientInner.registerServerRequestHandler((request, connection) -> {
                if (request instanceof ConfigChangeNotifyRequest) {
                    return handleConfigChangeNotifyRequest((ConfigChangeNotifyRequest) request,
                            rpcClientInner.getName());
                }
                return null;
            });

            rpcClientInner.registerServerRequestHandler((request, connection) -> {
                if (request instanceof ClientConfigMetricRequest) {
                    return handleClientMetricsRequest((ClientConfigMetricRequest) request);
                }
                return null;
            });

            rpcClientInner.registerConnectionListener(new ConnectionEventListener() {

                @Override
                public void onConnected(Connection connection) {
                    LOGGER.info("[{}] Connected,notify listen context...", rpcClientInner.getName());
                    notifyListenConfig();
                }

                @Override
                public void onDisConnect(Connection connection) {
                    String taskId = rpcClientInner.getLabels().get("taskId");
                    LOGGER.info("[{}] DisConnected,clear listen context...", rpcClientInner.getName());
                    Collection<CacheData> values = cacheMap.get().values();

                    for (CacheData cacheData : values) {
                        if (StringUtils.isNotBlank(taskId)) {
                            if (Integer.valueOf(taskId).equals(cacheData.getTaskId())) {
                                cacheData.setConsistentWithServer(false);
                            }
                        } else {
                            cacheData.setConsistentWithServer(false);
                        }
                    }
                }

            });

            rpcClientInner.serverListFactory(new ServerListFactory() {
                @Override
                public String genNextServer() {
                    return ConfigRpcTransportClient.super.serverListManager.getNextServerAddr();

                }

                @Override
                public String getCurrentServer() {
                    return ConfigRpcTransportClient.super.serverListManager.getCurrentServerAddr();

                }

                @Override
                public List<String> getServerList() {
                    return ConfigRpcTransportClient.super.serverListManager.getServerUrls();

                }
            });

            subscriber = new Subscriber() {
                @Override
                public void onEvent(Event event) {
                    rpcClientInner.onServerListChange();
                }

                @Override
                public Class<? extends Event> subscribeType() {
                    return ServerListChangeEvent.class;
                }
            };
            NotifyCenter.registerSubscriber(subscriber);
        }

        @Override
        public void startInternal() {
            // 线程池在阻塞等到信号的到来
            executor.schedule(() -> {
                while (!executor.isShutdown() && !executor.isTerminated()) {
                    try {
                        // 获取到listenExecutebell.offer(bellItem)的信号
                        // 如果没有监听器的变动，则等待5s处理一次
                        listenExecutebell.poll(5L, TimeUnit.SECONDS);
                        if (executor.isShutdown() || executor.isTerminated()) {
                            continue;
                        }
                        // 执行配置监听
                        // TODO 进入
                        executeConfigListen();
                    } catch (Throwable e) {
                        LOGGER.error("[rpc listen execute] [rpc listen] exception", e);
                        try {
                            Thread.sleep(50L);
                        } catch (InterruptedException interruptedException) {
                            //ignore
                        }
                        notifyListenConfig();
                    }
                }
            }, 0L, TimeUnit.MILLISECONDS);

        }

        @Override
        public String getName() {
            return serverListManager.getName();
        }

        @Override
        public void notifyListenConfig() {
            listenExecutebell.offer(bellItem);
        }

        /**
         * TODO
         *  一个配置项 一个cacheData
         *  eg:
         *  # 数据库配置
         *  db.url=jdbc:mysql://localhost:3306/mydb
         *  db.username=root
         *  db.password=secret
         *  # 日志配置
         *  log.level=INFO
         *  log.filepath=/var/log/myapp.log
         *  在这个例子中，以下每个行可以视为一个配置项：
         *  db.url（数据库 URL）
         *  db.username（数据库用户名）
         *  db.password（数据库密码）
         *  log.level（日志级别）
         *  log.filepath（日志文件路径）
         *  每个配置项可能对应一个 CacheData 对象，用于缓存其值和状态。例如，db.url、db.username、log.level 等都可以单独作为配置项存在于 Nacos 中
         *  总结
         *  配置是指一组相关的设置（如 application.properties），可以包含多个配置项。
         *  配置项是具体的设置（如 db.url），用于定义应用程序的具体参数
         *
         * @throws NacosException
         */
        @Override
        public void executeConfigListen() throws NacosException {
            // TODO 存放含有listen的cacheData
            Map<String, List<CacheData>> listenCachesMap = new HashMap<>(16);
            // TODO 存放不含有listen的cacheData
            Map<String, List<CacheData>> removeListenCachesMap = new HashMap<>(16);
            long now = System.currentTimeMillis();
            // 当前时间 减去 上一次全量同步的时间，如果大于3分钟，表示到了全量同步的时间
            boolean needAllSync = now - lastAllSyncTime >= ALL_SYNC_INTERNAL;
            // 遍历一个cacheMap 中的所有 CacheData 对象
            for (CacheData cache : cacheMap.get().values()) {
                // 是否和服务端一致
                synchronized (cache) {
                    // 一致则检查md5值，若md5值和上一个不一样，则说明变动了，需要通知监听器
                    checkLocalConfig(cache);
                    // 是否到全量同步时间了，未到则直接跳过
                    // check local listeners consistent.
                    if (cache.isConsistentWithServer()) {
                        // TODO 进入
                        cache.checkListenerMd5();
                        if (!needAllSync) {// TODO 判断是否需要全量同步
                            continue;
                        }
                    }

                    // If local configuration information is used, then skip the processing directly.
                    if (cache.isUseLocalConfigInfo()) {//TODO 如果当前 CacheData 使用的是本地配置，则跳过处理
                        continue;
                    }

                    if (!cache.isDiscard()) {
                        // 非丢弃型，即新增，放入listenCachesMap
                        List<CacheData> cacheDatas = listenCachesMap.computeIfAbsent(String.valueOf(cache.getTaskId()),
                                k -> new LinkedList<>());
                        cacheDatas.add(cache);
                    } else {

                        // 丢弃型，即删除， 放入removeListenCachesMap
                        List<CacheData> cacheDatas = removeListenCachesMap.computeIfAbsent(
                                String.valueOf(cache.getTaskId()), k -> new LinkedList<>());
                        cacheDatas.add(cache);
                    }
                }

            }
            // 如果需要和服务端数据同步，则listenCachesMap和removeListenCachesMap存放了本地数据，需要和服务端对比
            //execute check listen ,return true if has change keys.
            // TODO 比较 listenCachesMap 中的本地数据与服务器数据，返回是否有变更的键
            boolean hasChangedKeys = checkListenCache(listenCachesMap);

            //execute check remove listen.
            checkRemoveListenCache(removeListenCachesMap);

            if (needAllSync) {
                // 更新同步时间
                lastAllSyncTime = now;
            }
            //If has changed keys,notify re sync md5.
            if (hasChangedKeys) {// 如果检测到有变更的键，则调用 notifyListenConfig 方法，通知相关组件进行同步操作
                // 服务端告知了有数据变动，则需要再同步一次
                notifyListenConfig();
            }

        }

        /**
         * Checks and handles local configuration for a given CacheData object. This method evaluates the use of
         * failover files for local configuration storage and updates the CacheData accordingly.
         * <pre>
         *  TODO
         *   在配置管理中，故障转移通常指使用本地配置文件作为备用，以防远程配置源不可用。
         *   这样，当连接到远程服务失败时，系统仍然可以使用本地保存的配置，从而保持正常运行。
         *   例如，如果应用从 Nacos 服务器加载配置，当服务器不可用时，它会切换到本地存储的配置文件，以确保应用不停止。
         * </pre>
         *
         * @param cacheData The CacheData object to be processed.
         */
        public void checkLocalConfig(CacheData cacheData) {
            final String dataId = cacheData.dataId;
            final String group = cacheData.group;
            final String tenant = cacheData.tenant;
            final String envName = cacheData.envName;

            // Check if a failover file exists for the specified dataId, group, and tenant.
            // 调用 LocalConfigInfoProcessor 获取对应 dataId、group 和 tenant 的故障转移文件
            File file = LocalConfigInfoProcessor.getFailoverFile(envName, dataId, group, tenant);

            // If not using local config info and a failover file exists, load and use it.
            // TODO 检查当前是否使用本地配置，且故障转移文件存在
            if (!cacheData.isUseLocalConfigInfo() && file.exists()) {
                // 读取故障转移文件的内容
                // TODO 进入
                String content = LocalConfigInfoProcessor.getFailover(envName, dataId, group, tenant);
                // 计算故障转移文件内容的 MD5 校验值
                final String md5 = MD5Utils.md5Hex(content, Constants.ENCODE);
                //更新 cacheData 的状态为使用本地配置
                cacheData.setUseLocalConfigInfo(true);
                // 设置 cacheData 的本地配置版本为故障转移文件的最后修改时间
                cacheData.setLocalConfigInfoVersion(file.lastModified());
                //将 cacheData 的内容更新为故障转移文件的内容
                cacheData.setContent(content);
                // 记录日志，提示故障转移文件已被创建，并输出相关信息（如 dataId、group、tenant、MD5 和内容）
                LOGGER.warn(
                        "[{}] [failover-change] failover file created. dataId={}, group={}, tenant={}, md5={}, content={}",
                        envName, dataId, group, tenant, md5, ContentUtils.truncateContent(content));
                return;
            }

            // If use local config info, but the failover file is deleted, switch back to server config.
            // TODO 检查当前是否使用本地配置，且故障转移文件不存在
            if (cacheData.isUseLocalConfigInfo() && !file.exists()) {
                // 更新 cacheData 的状态为不使用本地配置
                cacheData.setUseLocalConfigInfo(false);
                LOGGER.warn("[{}] [failover-change] failover file deleted. dataId={}, group={}, tenant={}", envName,
                        dataId, group, tenant);
                return;
            }

            // When the failover file content changes, indicating a change in local configuration.
            // TODO 检查当前是否使用本地配置，故障转移文件存在，并且本地配置版本与文件的最后修改时间不同（表示内容可能已更改）
            if (cacheData.isUseLocalConfigInfo() && file.exists()
                    && cacheData.getLocalConfigInfoVersion() != file.lastModified()) {
                // 读取故障转移文件的新内容。
                String content = LocalConfigInfoProcessor.getFailover(envName, dataId, group, tenant);
                // 计算新内容的 MD5 校验值。
                final String md5 = MD5Utils.md5Hex(content, Constants.ENCODE);
                // 确保 cacheData 的状态为使用本地配置（虽然可能是重复的，但保证状态一致）
                cacheData.setUseLocalConfigInfo(true);
                // 更新 cacheData 的本地配置版本为新读取文件的最后修改时间
                cacheData.setLocalConfigInfoVersion(file.lastModified());
                // 将 cacheData 的内容更新为新读取的故障转移文件内容
                cacheData.setContent(content);
                LOGGER.warn(
                        "[{}] [failover-change] failover file changed. dataId={}, group={}, tenant={}, md5={}, content={}",
                        envName, dataId, group, tenant, md5, ContentUtils.truncateContent(content));
            }
        }

        private ExecutorService ensureSyncExecutor(String taskId) {
            if (!multiTaskExecutor.containsKey(taskId)) {
                multiTaskExecutor.put(taskId,
                        new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), r -> {
                            Thread thread = new Thread(r, "nacos.client.config.listener.task-" + taskId);
                            thread.setDaemon(true);
                            return thread;
                        }));
            }
            return multiTaskExecutor.get(taskId);
        }

        private void refreshContentAndCheck(RpcClient rpcClient, String groupKey, boolean notify) {
            if (cacheMap.get() != null && cacheMap.get().containsKey(groupKey)) {
                CacheData cache = cacheMap.get().get(groupKey);
                refreshContentAndCheck(rpcClient, cache, notify);
            }
        }

        private void refreshContentAndCheck(RpcClient rpcClient, CacheData cacheData, boolean notify) {
            try {

                ConfigResponse response = this.queryConfigInner(rpcClient, cacheData.dataId, cacheData.group,
                        cacheData.tenant, 3000L, notify);
                cacheData.setEncryptedDataKey(response.getEncryptedDataKey());
                cacheData.setContent(response.getContent());
                if (null != response.getConfigType()) {
                    cacheData.setType(response.getConfigType());
                }
                if (notify) {
                    LOGGER.info("[{}] [data-received] dataId={}, group={}, tenant={}, md5={}, content={}, type={}",
                            agent.getName(), cacheData.dataId, cacheData.group, cacheData.tenant, cacheData.getMd5(),
                            ContentUtils.truncateContent(response.getContent()), response.getConfigType());
                }
                cacheData.checkListenerMd5();
            } catch (Exception e) {
                LOGGER.error("refresh content and check md5 fail ,dataId={},group={},tenant={} ", cacheData.dataId,
                        cacheData.group, cacheData.tenant, e);
            }
        }

        private void checkRemoveListenCache(Map<String, List<CacheData>> removeListenCachesMap) throws NacosException {
            if (!removeListenCachesMap.isEmpty()) {
                List<Future> listenFutures = new ArrayList<>();

                for (Map.Entry<String, List<CacheData>> entry : removeListenCachesMap.entrySet()) {
                    String taskId = entry.getKey();
                    RpcClient rpcClient = ensureRpcClient(taskId);

                    ExecutorService executorService = ensureSyncExecutor(taskId);
                    Future future = executorService.submit(() -> {
                        List<CacheData> removeListenCaches = entry.getValue();
                        ConfigBatchListenRequest configChangeListenRequest = buildConfigRequest(removeListenCaches);
                        configChangeListenRequest.setListen(false);
                        try {
                            boolean removeSuccess = unListenConfigChange(rpcClient, configChangeListenRequest);
                            if (removeSuccess) {
                                for (CacheData cacheData : removeListenCaches) {
                                    synchronized (cacheData) {
                                        if (cacheData.isDiscard() && cacheData.getListeners().isEmpty()) {
                                            ClientWorker.this.removeCache(cacheData.dataId, cacheData.group,
                                                    cacheData.tenant);
                                        }
                                    }
                                }
                            }

                        } catch (Throwable e) {
                            LOGGER.error("Async remove listen config change error ", e);
                            try {
                                Thread.sleep(50L);
                            } catch (InterruptedException interruptedException) {
                                //ignore
                            }
                            notifyListenConfig();
                        }
                    });
                    listenFutures.add(future);

                }
                for (Future future : listenFutures) {
                    try {
                        future.get();
                    } catch (Throwable throwable) {
                        LOGGER.error("Async remove listen config change error ", throwable);
                    }
                }
            }
        }

        private boolean checkListenCache(Map<String, List<CacheData>> listenCachesMap) throws NacosException {

            final AtomicBoolean hasChangedKeys = new AtomicBoolean(false);
            if (!listenCachesMap.isEmpty()) {
                List<Future> listenFutures = new ArrayList<>();
                for (Map.Entry<String, List<CacheData>> entry : listenCachesMap.entrySet()) {
                    String taskId = entry.getKey();
                    RpcClient rpcClient = ensureRpcClient(taskId);

                    ExecutorService executorService = ensureSyncExecutor(taskId);
                    Future future = executorService.submit(() -> {
                        List<CacheData> listenCaches = entry.getValue();
                        //reset notify change flag.
                        for (CacheData cacheData : listenCaches) {
                            cacheData.getReceiveNotifyChanged().set(false);
                        }
                        ConfigBatchListenRequest configChangeListenRequest = buildConfigRequest(listenCaches);
                        configChangeListenRequest.setListen(true);
                        try {
                            ConfigChangeBatchListenResponse listenResponse = (ConfigChangeBatchListenResponse) requestProxy(
                                    rpcClient, configChangeListenRequest);
                            if (listenResponse != null && listenResponse.isSuccess()) {

                                Set<String> changeKeys = new HashSet<String>();

                                List<ConfigChangeBatchListenResponse.ConfigContext> changedConfigs = listenResponse.getChangedConfigs();
                                //handle changed keys,notify listener
                                if (!CollectionUtils.isEmpty(changedConfigs)) {
                                    hasChangedKeys.set(true);
                                    for (ConfigChangeBatchListenResponse.ConfigContext changeConfig : changedConfigs) {
                                        String changeKey = GroupKey.getKeyTenant(changeConfig.getDataId(),
                                                changeConfig.getGroup(), changeConfig.getTenant());
                                        changeKeys.add(changeKey);
                                        boolean isInitializing = cacheMap.get().get(changeKey).isInitializing();
                                        refreshContentAndCheck(rpcClient, changeKey, !isInitializing);
                                    }

                                }

                                for (CacheData cacheData : listenCaches) {
                                    if (cacheData.getReceiveNotifyChanged().get()) {
                                        String changeKey = GroupKey.getKeyTenant(cacheData.dataId, cacheData.group,
                                                cacheData.getTenant());
                                        if (!changeKeys.contains(changeKey)) {
                                            boolean isInitializing = cacheMap.get().get(changeKey).isInitializing();
                                            refreshContentAndCheck(rpcClient, changeKey, !isInitializing);
                                        }
                                    }
                                }

                                //handler content configs
                                for (CacheData cacheData : listenCaches) {
                                    cacheData.setInitializing(false);
                                    String groupKey = GroupKey.getKeyTenant(cacheData.dataId, cacheData.group,
                                            cacheData.getTenant());
                                    if (!changeKeys.contains(groupKey)) {
                                        synchronized (cacheData) {
                                            if (!cacheData.getReceiveNotifyChanged().get()) {
                                                cacheData.setConsistentWithServer(true);
                                            }
                                        }
                                    }
                                }

                            }
                        } catch (Throwable e) {
                            LOGGER.error("Execute listen config change error ", e);
                            try {
                                Thread.sleep(50L);
                            } catch (InterruptedException interruptedException) {
                                //ignore
                            }
                            notifyListenConfig();
                        }
                    });
                    listenFutures.add(future);

                }
                for (Future future : listenFutures) {
                    try {
                        future.get();
                    } catch (Throwable throwable) {
                        LOGGER.error("Async listen config change error ", throwable);
                    }
                }

            }
            return hasChangedKeys.get();
        }

        private RpcClient ensureRpcClient(String taskId) throws NacosException {
            synchronized (ClientWorker.this) {

                Map<String, String> labels = getLabels();
                Map<String, String> newLabels = new HashMap<>(labels);
                newLabels.put("taskId", taskId);
                RpcClient rpcClient = RpcClientFactory.createClient(uuid + "_config-" + taskId, getConnectionType(),
                        newLabels, this.properties, RpcClientTlsConfig.properties(this.properties));
                if (rpcClient.isWaitInitiated()) {
                    initRpcClientHandler(rpcClient);
                    rpcClient.setTenant(getTenant());
                    rpcClient.start();
                }

                return rpcClient;
            }

        }

        /**
         * build config string.
         *
         * @param caches caches to build config string.
         * @return request.
         */
        private ConfigBatchListenRequest buildConfigRequest(List<CacheData> caches) {

            ConfigBatchListenRequest configChangeListenRequest = new ConfigBatchListenRequest();
            for (CacheData cacheData : caches) {
                configChangeListenRequest.addConfigListenContext(cacheData.group, cacheData.dataId, cacheData.tenant,
                        cacheData.getMd5());
            }
            return configChangeListenRequest;
        }

        @Override
        public void removeCache(String dataId, String group) {
            // Notify to rpc un listen ,and remove cache if success.
            notifyListenConfig();
        }

        /**
         * send cancel listen config change request .
         *
         * @param configChangeListenRequest request of remove listen config string.
         */
        private boolean unListenConfigChange(RpcClient rpcClient, ConfigBatchListenRequest configChangeListenRequest)
                throws NacosException {

            ConfigChangeBatchListenResponse response = (ConfigChangeBatchListenResponse) requestProxy(rpcClient,
                    configChangeListenRequest);
            return response.isSuccess();
        }

        @Override
        public ConfigResponse queryConfig(String dataId, String group, String tenant, long readTimeouts, boolean notify)
                throws NacosException {
            // 创建一个rpc客户端
            RpcClient rpcClient = getOneRunningClient();
            if (notify) {
                CacheData cacheData = cacheMap.get().get(GroupKey.getKeyTenant(dataId, group, tenant));
                if (cacheData != null) {
                    rpcClient = ensureRpcClient(String.valueOf(cacheData.getTaskId()));
                }
            }
            // TODO 进入
            return queryConfigInner(rpcClient, dataId, group, tenant, readTimeouts, notify);

        }

        /**
         * @param rpcClient    用于发送 RPC 请求的客户端实例
         * @param dataId       要查询的配置的唯一标识符
         * @param group        配置所属的组
         * @param tenant       租户信息（如果使用了多租户）
         * @param readTimeouts 读取超时设置
         * @param notify       是否启用通知功能
         * @return
         * @throws NacosException
         */
        ConfigResponse queryConfigInner(RpcClient rpcClient, String dataId, String group, String tenant,
                                        long readTimeouts, boolean notify) throws NacosException {
            // 请求
            ConfigQueryRequest request = ConfigQueryRequest.build(dataId, group, tenant);
            // 设置请求头
            request.putHeader(NOTIFY_HEADER, String.valueOf(notify));
            // 真正网络调用 得到响应
            ConfigQueryResponse response = (ConfigQueryResponse) requestProxy(rpcClient, request, readTimeouts);
            // 配置响应对象
            // 创建一个 ConfigResponse 对象，用于存储最终的结果
            ConfigResponse configResponse = new ConfigResponse();
            if (response.isSuccess()) {// 成功
                // 保存数据快照到本地
                LocalConfigInfoProcessor.saveSnapshot(this.getName(), dataId, group, tenant, response.getContent());
                configResponse.setContent(response.getContent());
                // 配置类型
                String configType;
                if (StringUtils.isNotBlank(response.getContentType())) {
                    configType = response.getContentType();
                } else {
                    configType = ConfigType.TEXT.getType();
                }
                configResponse.setConfigType(configType);
                // 数据加密key
                String encryptedDataKey = response.getEncryptedDataKey();
                // 数据加密
                LocalEncryptedDataKeyProcessor.saveEncryptDataKeySnapshot(agent.getName(), dataId, group, tenant,
                        encryptedDataKey);
                configResponse.setEncryptedDataKey(encryptedDataKey);
                return configResponse;
            } else if (response.getErrorCode() == ConfigQueryResponse.CONFIG_NOT_FOUND) {//没有找到配置
                //没有找到接口 会删除本地配置快照文件
                LocalConfigInfoProcessor.saveSnapshot(this.getName(), dataId, group, tenant, null);
                //也是删除本地加密的配置快照文件
                LocalEncryptedDataKeyProcessor.saveEncryptDataKeySnapshot(agent.getName(), dataId, group, tenant, null);
                return configResponse;
            } else if (response.getErrorCode() == ConfigQueryResponse.CONFIG_QUERY_CONFLICT) {
                // 冲突 配置正在修改
                LOGGER.error(
                        "[{}] [sub-server-error] get server config being modified concurrently, dataId={}, group={}, "
                                + "tenant={}", this.getName(), dataId, group, tenant);
                throw new NacosException(NacosException.CONFLICT,
                        "data being modified, dataId=" + dataId + ",group=" + group + ",tenant=" + tenant);
            } else {
                LOGGER.error("[{}] [sub-server-error]  dataId={}, group={}, tenant={}, code={}", this.getName(), dataId,
                        group, tenant, response);
                throw new NacosException(response.getErrorCode(),
                        "http error, code=" + response.getErrorCode() + ",msg=" + response.getMessage() + ",dataId="
                                + dataId + ",group=" + group + ",tenant=" + tenant);

            }
        }

        private Response requestProxy(RpcClient rpcClientInner, Request request) throws NacosException {
            return requestProxy(rpcClientInner, request, 3000L);
        }

        private Response requestProxy(RpcClient rpcClientInner, Request request, long timeoutMills)
                throws NacosException {
            try {
                request.putAllHeader(super.getSecurityHeaders(resourceBuild(request)));
                request.putAllHeader(super.getCommonHeader());
            } catch (Exception e) {
                throw new NacosException(NacosException.CLIENT_INVALID_PARAM, e);
            }
            JsonObject asJsonObjectTemp = new Gson().toJsonTree(request).getAsJsonObject();
            asJsonObjectTemp.remove("headers");
            asJsonObjectTemp.remove("requestId");
            boolean limit = Limiter.isLimit(request.getClass() + asJsonObjectTemp.toString());
            if (limit) {
                throw new NacosException(NacosException.CLIENT_OVER_THRESHOLD,
                        "More than client-side current limit threshold");
            }
            return rpcClientInner.request(request, timeoutMills);
        }

        private RequestResource resourceBuild(Request request) {
            if (request instanceof ConfigQueryRequest) {
                String tenant = ((ConfigQueryRequest) request).getTenant();
                String group = ((ConfigQueryRequest) request).getGroup();
                String dataId = ((ConfigQueryRequest) request).getDataId();
                return buildResource(tenant, group, dataId);
            }
            if (request instanceof ConfigPublishRequest) {
                String tenant = ((ConfigPublishRequest) request).getTenant();
                String group = ((ConfigPublishRequest) request).getGroup();
                String dataId = ((ConfigPublishRequest) request).getDataId();
                return buildResource(tenant, group, dataId);
            }

            if (request instanceof ConfigRemoveRequest) {
                String tenant = ((ConfigRemoveRequest) request).getTenant();
                String group = ((ConfigRemoveRequest) request).getGroup();
                String dataId = ((ConfigRemoveRequest) request).getDataId();
                return buildResource(tenant, group, dataId);
            }
            return RequestResource.configBuilder().build();
        }

        RpcClient getOneRunningClient() throws NacosException {
            return ensureRpcClient("0");
        }

        @Override
        public boolean publishConfig(String dataId, String group, String tenant, String appName, String tag,
                                     String betaIps, String content, String encryptedDataKey, String casMd5, String type)
                throws NacosException {
            try {
                ConfigPublishRequest request = new ConfigPublishRequest(dataId, group, tenant, content);
                request.setCasMd5(casMd5);
                request.putAdditionalParam(TAG_PARAM, tag);
                request.putAdditionalParam(APP_NAME_PARAM, appName);
                request.putAdditionalParam(BETAIPS_PARAM, betaIps);
                request.putAdditionalParam(TYPE_PARAM, type);
                request.putAdditionalParam(ENCRYPTED_DATA_KEY_PARAM, encryptedDataKey == null ? "" : encryptedDataKey);
                ConfigPublishResponse response = (ConfigPublishResponse) requestProxy(getOneRunningClient(), request);
                if (!response.isSuccess()) {
                    LOGGER.warn("[{}] [publish-single] fail, dataId={}, group={}, tenant={}, code={}, msg={}",
                            this.getName(), dataId, group, tenant, response.getErrorCode(), response.getMessage());
                    return false;
                } else {
                    LOGGER.info("[{}] [publish-single] ok, dataId={}, group={}, tenant={}, config={}", getName(),
                            dataId, group, tenant, ContentUtils.truncateContent(content));
                    return true;
                }
            } catch (Exception e) {
                LOGGER.warn("[{}] [publish-single] error, dataId={}, group={}, tenant={}, code={}, msg={}",
                        this.getName(), dataId, group, tenant, "unknown", e.getMessage());
                return false;
            }
        }

        @Override
        public boolean removeConfig(String dataId, String group, String tenant, String tag) throws NacosException {
            ConfigRemoveRequest request = new ConfigRemoveRequest(dataId, group, tenant, tag);
            ConfigRemoveResponse response = (ConfigRemoveResponse) requestProxy(getOneRunningClient(), request);
            return response.isSuccess();
        }

        /**
         * check server is health.
         *
         * @return
         */
        public boolean isHealthServer() {
            try {
                return getOneRunningClient().isRunning();
            } catch (NacosException e) {
                LOGGER.warn("check server status failed.", e);
                return false;
            }
        }
    }

    public String getAgentName() {
        return this.agent.getName();
    }

    public ConfigTransportClient getAgent() {
        return this.agent;
    }

}
