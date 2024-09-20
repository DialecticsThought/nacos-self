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

package com.alibaba.nacos.config.server.service.notify;

import com.alibaba.nacos.api.config.remote.request.cluster.ConfigChangeClusterSyncRequest;
import com.alibaba.nacos.api.config.remote.response.cluster.ConfigChangeClusterSyncResponse;
import com.alibaba.nacos.api.remote.RequestCallBack;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.task.AbstractDelayTask;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.model.event.ConfigDataChangeEvent;
import com.alibaba.nacos.config.server.monitor.MetricsMonitor;
import com.alibaba.nacos.config.server.remote.ConfigClusterRpcClientProxy;
import com.alibaba.nacos.config.server.service.trace.ConfigTraceService;
import com.alibaba.nacos.config.server.utils.ConfigExecutor;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.NodeState;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.sys.utils.InetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Async notify service.
 *
 * @author Nacos
 */
@Service
public class AsyncNotifyService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncNotifyService.class);

    private static final int MIN_RETRY_INTERVAL = 500;

    private static final int INCREASE_STEPS = 1000;

    private static final int MAX_COUNT = 6;

    @Autowired
    private ConfigClusterRpcClientProxy configClusterRpcClientProxy;

    private ServerMemberManager memberManager;

    static final List<NodeState> HEALTHY_CHECK_STATUS = new ArrayList<>();

    static {
        HEALTHY_CHECK_STATUS.add(NodeState.UP);
        HEALTHY_CHECK_STATUS.add(NodeState.SUSPICIOUS);
    }

    @Autowired
    public AsyncNotifyService(ServerMemberManager memberManager) {
        this.memberManager = memberManager;

        // Register ConfigDataChangeEvent to NotifyCenter.
        // 注册ConfigDataChangeEvent到NotifyCenter.
        NotifyCenter.registerToPublisher(ConfigDataChangeEvent.class, NotifyCenter.ringBufferSize);

        // Register A Subscriber to subscribe ConfigDataChangeEvent.
        // 注册一个订阅ConfigDataChangeEvent事件的处理类
        NotifyCenter.registerSubscriber(new Subscriber() {

            @Override
            public void onEvent(Event event) {
                // Generate ConfigDataChangeEvent concurrently
                // TODO 进入
                handleConfigDataChangeEvent(event);
            }

            @Override
            public Class<? extends Event> subscribeType() {
                return ConfigDataChangeEvent.class;
            }
        });
    }

    /**
     * 1、获取所有的Nacos服务节点（包括当前客户端）
     * 2、创建一个队列，将相关配置的其他服务节点都存放进来
     * 3、通过线程池执行异步通知
     *
     * @param event
     */
    void handleConfigDataChangeEvent(Event event) {
        if (event instanceof ConfigDataChangeEvent) {
            ConfigDataChangeEvent evt = (ConfigDataChangeEvent) event;
            long dumpTs = evt.lastModifiedTs;
            String dataId = evt.dataId;
            String group = evt.group;
            String tenant = evt.tenant;
            String tag = evt.tag;
            MetricsMonitor.incrementConfigChangeCount(tenant, group, dataId);
            //  // 获取所有的Nacos服务节点（包括当前客户端）
            Collection<Member> ipList = memberManager.allMembersWithoutSelf();

            // In fact, any type of queue here can be
            // 创建一个队列，将相关配置的其他服务节点都存放进来
            Queue<NotifySingleRpcTask> rpcQueue = new LinkedList<>();

            for (Member member : ipList) {
                // grpc report data change only
                rpcQueue.add(
                        new NotifySingleRpcTask(dataId, group, tenant, tag, dumpTs, evt.isBeta, evt.isBatch, member));
            }
            if (!rpcQueue.isEmpty()) {
                // 通过线程池执行异步通知
                // AsyncRpcTask实现了runnable接口，关注其run方法
                // TODO 查看 AsyncRpcTask
                // 只要队列不为空，就会从队列中取出NotifySingleRpcTask任务来执行
                // 然后构造配置变动集群同步的请求对象，包括namespaceId、dataId、groupId、标签等，然后通知目标节点
                ConfigExecutor.executeAsyncNotify(new AsyncRpcTask(rpcQueue));
            }
        }
    }

    private boolean isUnHealthy(String targetIp) {
        return !memberManager.stateCheck(targetIp, HEALTHY_CHECK_STATUS);
    }

    void executeAsyncRpcTask(Queue<NotifySingleRpcTask> queue) {
        while (!queue.isEmpty()) {
            // 从队列中取出任务
            NotifySingleRpcTask task = queue.poll();
            // 构造配置变动集群同步请求
            ConfigChangeClusterSyncRequest syncRequest = new ConfigChangeClusterSyncRequest();
            syncRequest.setDataId(task.getDataId());
            syncRequest.setGroup(task.getGroup());
            syncRequest.setBeta(task.isBeta());
            syncRequest.setLastModified(task.getLastModified());
            syncRequest.setTag(task.getTag());
            syncRequest.setBatch(task.isBatch());
            syncRequest.setTenant(task.getTenant());
            // 通知的目标节点
            Member member = task.member;

            String event = getNotifyEvent(task);
            if (memberManager.hasMember(member.getAddress())) {
                // start the health check and there are ips that are not monitored, put them directly in the notification queue, otherwise notify
                // 启动健康检查，有IP未被监控，直接放入通知队列，否则通知
                boolean unHealthNeedDelay = isUnHealthy(member.getAddress());
                if (unHealthNeedDelay) {
                    // target ip is unhealthy, then put it in the notification list
                    // 目标 IP 运行状况不健康，然后将其放入通知列表中
                    ConfigTraceService.logNotifyEvent(task.getDataId(), task.getGroup(), task.getTenant(), null,
                            task.getLastModified(), InetUtils.getSelfIP(), event,
                            ConfigTraceService.NOTIFY_TYPE_UNHEALTH, 0, member.getAddress());
                    // get delay time and set fail count to the task
                    // 异步任务执行
                    // 可延迟的处理，因为是不健康的节点，不知道什么时候能恢复
                    asyncTaskExecute(task);
                } else {

                    // grpc report data change only
                    // 发送grpc请求
                    try {
                        configClusterRpcClientProxy.syncConfigChange(member, syncRequest,
                                new AsyncRpcNotifyCallBack(AsyncNotifyService.this, task));
                    } catch (Exception e) {
                        MetricsMonitor.getConfigNotifyException().increment();
                        asyncTaskExecute(task);
                    }

                }
            } else {
                //No nothing if  member has offline.
            }

        }
    }

    public class AsyncRpcTask implements Runnable {

        private Queue<NotifySingleRpcTask> queue;

        public AsyncRpcTask(Queue<NotifySingleRpcTask> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            // TODO 进入
            executeAsyncRpcTask(queue);
        }
    }

    public static class NotifySingleRpcTask extends AbstractDelayTask {

        private String dataId;

        private String group;

        private String tenant;

        private long lastModified;

        private int failCount;

        private Member member;

        private boolean isBeta;

        private String tag;

        private boolean isBatch;

        public NotifySingleRpcTask(String dataId, String group, String tenant, String tag, long lastModified,
                boolean isBeta, boolean isBatch, Member member) {
            this(dataId, group, tenant, lastModified);
            this.member = member;
            this.isBeta = isBeta;
            this.tag = tag;
            this.isBatch = isBatch;
        }

        private NotifySingleRpcTask(String dataId, String group, String tenant, long lastModified) {
            this.dataId = dataId;
            this.group = group;
            this.tenant = tenant;
            this.lastModified = lastModified;
            setTaskInterval(3000L);
        }

        public boolean isBeta() {
            return isBeta;
        }

        public void setBeta(boolean beta) {
            isBeta = beta;
        }

        public String getTag() {
            return tag;
        }

        public void setTag(String tag) {
            this.tag = tag;
        }

        public boolean isBatch() {
            return isBatch;
        }

        public void setBatch(boolean batch) {
            isBatch = batch;
        }

        public String getDataId() {
            return dataId;
        }

        public String getGroup() {
            return group;
        }

        public int getFailCount() {
            return failCount;
        }

        public void setFailCount(int failCount) {
            this.failCount = failCount;
        }

        public long getLastModified() {
            return lastModified;
        }

        @Override
        public void merge(AbstractDelayTask task) {
            // Perform merge, but do nothing, tasks with the same dataId and group, later will replace the previous

        }

        public String getTenant() {
            return tenant;
        }

    }

    private void asyncTaskExecute(NotifySingleRpcTask task) {
        int delay = getDelayTime(task);
        Queue<NotifySingleRpcTask> queue = new LinkedList<>();
        queue.add(task);
        AsyncRpcTask asyncTask = new AsyncRpcTask(queue);
        ConfigExecutor.scheduleAsyncNotify(asyncTask, delay, TimeUnit.MILLISECONDS);
    }

    private static String getNotifyEvent(NotifySingleRpcTask task) {
        String event = ConfigTraceService.NOTIFY_EVENT;
        if (task.isBeta()) {
            event = ConfigTraceService.NOTIFY_EVENT_BETA;
        } else if (!StringUtils.isBlank(task.tag)) {
            event = ConfigTraceService.NOTIFY_EVENT_TAG + "-" + task.tag;
        } else if (task.isBatch()) {
            event = ConfigTraceService.NOTIFY_EVENT_BATCH;
        }
        return event;
    }

    public static class AsyncRpcNotifyCallBack implements RequestCallBack<ConfigChangeClusterSyncResponse> {

        private NotifySingleRpcTask task;

        AsyncNotifyService asyncNotifyService;

        public AsyncRpcNotifyCallBack(AsyncNotifyService asyncNotifyService, NotifySingleRpcTask task) {
            this.task = task;
            this.asyncNotifyService = asyncNotifyService;
        }

        @Override
        public Executor getExecutor() {
            return ConfigExecutor.getConfigSubServiceExecutor();
        }

        @Override
        public long getTimeout() {
            return 1000L;
        }

        @Override
        public void onResponse(ConfigChangeClusterSyncResponse response) {
            String event = getNotifyEvent(task);

            long delayed = System.currentTimeMillis() - task.getLastModified();
            if (response.isSuccess()) {
                ConfigTraceService.logNotifyEvent(task.getDataId(), task.getGroup(), task.getTenant(), null,
                        task.getLastModified(), InetUtils.getSelfIP(), event, ConfigTraceService.NOTIFY_TYPE_OK,
                        delayed, task.member.getAddress());
            } else {
                LOGGER.error("[notify-error] target:{} dataId:{} group:{} ts:{} code:{}", task.member.getAddress(),
                        task.getDataId(), task.getGroup(), task.getLastModified(), response.getErrorCode());
                ConfigTraceService.logNotifyEvent(task.getDataId(), task.getGroup(), task.getTenant(), null,
                        task.getLastModified(), InetUtils.getSelfIP(), event, ConfigTraceService.NOTIFY_TYPE_ERROR,
                        delayed, task.member.getAddress());

                //get delay time and set fail count to the task
                asyncNotifyService.asyncTaskExecute(task);

                LogUtil.NOTIFY_LOG.error("[notify-retry] target:{} dataId:{} group:{} ts:{}", task.member.getAddress(),
                        task.getDataId(), task.getGroup(), task.getLastModified());

                MetricsMonitor.getConfigNotifyException().increment();
            }
        }

        @Override
        public void onException(Throwable ex) {
            String event = getNotifyEvent(task);

            long delayed = System.currentTimeMillis() - task.getLastModified();
            LOGGER.error("[notify-exception] target:{} dataId:{} group:{} ts:{} ex:{}", task.member.getAddress(),
                    task.getDataId(), task.getGroup(), task.getLastModified(), ex);
            ConfigTraceService.logNotifyEvent(task.getDataId(), task.getGroup(), task.getTenant(), null,
                    task.getLastModified(), InetUtils.getSelfIP(), event, ConfigTraceService.NOTIFY_TYPE_EXCEPTION,
                    delayed, task.member.getAddress());

            //get delay time and set fail count to the task
            asyncNotifyService.asyncTaskExecute(task);
            LogUtil.NOTIFY_LOG.error("[notify-retry] target:{} dataId:{} group:{} ts:{}", task.member.getAddress(),
                    task.getDataId(), task.getGroup(), task.getLastModified());

            MetricsMonitor.getConfigNotifyException().increment();
        }
    }

    /**
     * get delayTime and also set failCount to task; The failure time index increases, so as not to retry invalid tasks
     * in the offline scene, which affects the normal synchronization.
     *
     * @param task notify task
     * @return delay
     */
    private static int getDelayTime(NotifySingleRpcTask task) {
        int failCount = task.getFailCount();
        int delay = MIN_RETRY_INTERVAL + failCount * failCount * INCREASE_STEPS;
        if (failCount <= MAX_COUNT) {
            task.setFailCount(failCount + 1);
        }
        return delay;
    }
}
