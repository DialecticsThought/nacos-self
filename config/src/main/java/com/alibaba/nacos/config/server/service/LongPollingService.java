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

package com.alibaba.nacos.config.server.service;

import com.alibaba.nacos.api.remote.RpcScheduledExecutor;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.utils.ExceptionUtil;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.model.SampleResult;
import com.alibaba.nacos.config.server.model.event.LocalDataChangeEvent;
import com.alibaba.nacos.config.server.monitor.MetricsMonitor;
import com.alibaba.nacos.config.server.utils.ConfigExecutor;
import com.alibaba.nacos.config.server.utils.GroupKey;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.alibaba.nacos.config.server.utils.MD5Util;
import com.alibaba.nacos.config.server.utils.RequestUtil;
import com.alibaba.nacos.plugin.control.ControlManagerCenter;
import com.alibaba.nacos.plugin.control.connection.request.ConnectionCheckRequest;
import com.alibaba.nacos.plugin.control.connection.response.ConnectionCheckResponse;
import org.springframework.stereotype.Service;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.config.server.utils.LogUtil.MEMORY_LOG;
import static com.alibaba.nacos.config.server.utils.LogUtil.PULL_LOG;

/**
 * LongPollingService.
 *
 * @author Nacos
 */
@Service
public class LongPollingService {

    private static final int SAMPLE_PERIOD = 100;

    private static final int SAMPLE_TIMES = 3;

    private static final String TRUE_STR = "true";

    private Map<String, Long> retainIps = new ConcurrentHashMap<>();

    public SampleResult getSubscribleInfo(String dataId, String group, String tenant) {
        String groupKey = GroupKey.getKeyTenant(dataId, group, tenant);
        SampleResult sampleResult = new SampleResult();
        Map<String, String> lisentersGroupkeyStatus = new HashMap<>(50);

        for (ClientLongPolling clientLongPolling : allSubs) {
            if (clientLongPolling.clientMd5Map.containsKey(groupKey)) {
                lisentersGroupkeyStatus.put(clientLongPolling.ip, clientLongPolling.clientMd5Map.get(groupKey));
            }
        }
        sampleResult.setLisentersGroupkeyStatus(lisentersGroupkeyStatus);
        return sampleResult;
    }

    public SampleResult getSubscribleInfoByIp(String clientIp) {
        SampleResult sampleResult = new SampleResult();
        Map<String, String> lisentersGroupkeyStatus = new HashMap<>(50);

        for (ClientLongPolling clientLongPolling : allSubs) {
            if (clientLongPolling.ip.equals(clientIp)) {
                // One ip can have multiple listener.
                if (!lisentersGroupkeyStatus.equals(clientLongPolling.clientMd5Map)) {
                    lisentersGroupkeyStatus.putAll(clientLongPolling.clientMd5Map);
                }
            }
        }
        sampleResult.setLisentersGroupkeyStatus(lisentersGroupkeyStatus);
        return sampleResult;
    }

    /**
     * Aggregate the sampling IP and monitoring configuration information in the sampling results. There is no problem
     * for the merging strategy to cover the previous one with the latter.
     *
     * @param sampleResults sample Results.
     * @return Results.
     */
    public SampleResult mergeSampleResult(List<SampleResult> sampleResults) {
        SampleResult mergeResult = new SampleResult();
        Map<String, String> lisentersGroupkeyStatus = new HashMap<>(50);
        for (SampleResult sampleResult : sampleResults) {
            Map<String, String> lisentersGroupkeyStatusTmp = sampleResult.getLisentersGroupkeyStatus();
            for (Map.Entry<String, String> entry : lisentersGroupkeyStatusTmp.entrySet()) {
                lisentersGroupkeyStatus.put(entry.getKey(), entry.getValue());
            }
        }
        mergeResult.setLisentersGroupkeyStatus(lisentersGroupkeyStatus);
        return mergeResult;
    }

    public SampleResult getCollectSubscribleInfo(String dataId, String group, String tenant) {
        List<SampleResult> sampleResultLst = new ArrayList<>(50);
        for (int i = 0; i < SAMPLE_TIMES; i++) {
            SampleResult sampleTmp = getSubscribleInfo(dataId, group, tenant);
            if (sampleTmp != null) {
                sampleResultLst.add(sampleTmp);
            }
            if (i < SAMPLE_TIMES - 1) {
                try {
                    Thread.sleep(SAMPLE_PERIOD);
                } catch (InterruptedException e) {
                    LogUtil.CLIENT_LOG.error("sleep wrong", e);
                }
            }
        }

        return mergeSampleResult(sampleResultLst);
    }

    public SampleResult getCollectSubscribleInfoByIp(String ip) {
        SampleResult sampleResult = new SampleResult();
        sampleResult.setLisentersGroupkeyStatus(new HashMap<String, String>(50));
        for (int i = 0; i < SAMPLE_TIMES; i++) {
            SampleResult sampleTmp = getSubscribleInfoByIp(ip);
            if (sampleTmp != null) {
                if (sampleTmp.getLisentersGroupkeyStatus() != null && !sampleResult.getLisentersGroupkeyStatus()
                        .equals(sampleTmp.getLisentersGroupkeyStatus())) {
                    sampleResult.getLisentersGroupkeyStatus().putAll(sampleTmp.getLisentersGroupkeyStatus());
                }
            }
            if (i < SAMPLE_TIMES - 1) {
                try {
                    Thread.sleep(SAMPLE_PERIOD);
                } catch (InterruptedException e) {
                    LogUtil.CLIENT_LOG.error("sleep wrong", e);
                }
            }
        }
        return sampleResult;
    }

    /**
     * Add LongPollingClient.
     *
     * @param req              HttpServletRequest.
     * @param rsp              HttpServletResponse.
     * @param clientMd5Map     clientMd5Map.
     * @param probeRequestSize probeRequestSize.
     */
    public void addLongPollingClient(HttpServletRequest req, HttpServletResponse rsp, Map<String, String> clientMd5Map,
                                     int probeRequestSize) {

        String noHangUpFlag = req.getHeader(LongPollingService.LONG_POLLING_NO_HANG_UP_HEADER);

        long start = System.currentTimeMillis();
        List<String> changedGroups = MD5Util.compareMd5(req, rsp, clientMd5Map);
        if (changedGroups.size() > 0) {
            generateResponse(req, rsp, changedGroups);
            LogUtil.CLIENT_LOG.info("{}|{}|{}|{}|{}|{}|{}", System.currentTimeMillis() - start, "instant",
                    RequestUtil.getRemoteIp(req), "polling", clientMd5Map.size(), probeRequestSize,
                    changedGroups.size());
            return;
        } else if (noHangUpFlag != null && noHangUpFlag.equalsIgnoreCase(TRUE_STR)) {
            LogUtil.CLIENT_LOG.info("{}|{}|{}|{}|{}|{}|{}", System.currentTimeMillis() - start, "nohangup",
                    RequestUtil.getRemoteIp(req), "polling", clientMd5Map.size(), probeRequestSize,
                    changedGroups.size());
            return;
        }

        // Must be called by http thread, or send response.
        final AsyncContext asyncContext = req.startAsync();
        // AsyncContext.setTimeout() is incorrect, Control by oneself
        asyncContext.setTimeout(0L);
        String ip = RequestUtil.getRemoteIp(req);
        ConnectionCheckResponse connectionCheckResponse = checkLimit(req);
        if (!connectionCheckResponse.isSuccess()) {
            RpcScheduledExecutor.CONTROL_SCHEDULER.schedule(
                    () -> generate503Response(asyncContext, rsp, connectionCheckResponse.getMessage()),
                    1000L + new Random().nextInt(2000), TimeUnit.MILLISECONDS);
            return;
        }

        String appName = req.getHeader(RequestUtil.CLIENT_APPNAME_HEADER);
        String tag = req.getHeader("Vipserver-Tag");
        int delayTime = SwitchService.getSwitchInteger(SwitchService.FIXED_DELAY_TIME, 500);
        int minLongPoolingTimeout = SwitchService.getSwitchInteger("MIN_LONG_POOLING_TIMEOUT", 10000);

        // Add delay time for LoadBalance, and one response is returned 500 ms in advance to avoid client timeout.
        String requestLongPollingTimeOut = req.getHeader(LongPollingService.LONG_POLLING_HEADER);
        long timeout = Math.max(minLongPoolingTimeout, Long.parseLong(requestLongPollingTimeOut) - delayTime);
        ConfigExecutor.executeLongPolling(
                new ClientLongPolling(asyncContext, clientMd5Map, ip, probeRequestSize, timeout, appName, tag));
    }

    private ConnectionCheckResponse checkLimit(HttpServletRequest httpServletRequest) {
        String ip = RequestUtil.getRemoteIp(httpServletRequest);
        String appName = httpServletRequest.getHeader(RequestUtil.CLIENT_APPNAME_HEADER);
        ConnectionCheckRequest connectionCheckRequest = new ConnectionCheckRequest(ip, appName, "LongPolling");
        ConnectionCheckResponse checkResponse = ControlManagerCenter.getInstance().getConnectionControlManager()
                .check(connectionCheckRequest);
        return checkResponse;
    }

    public static boolean isSupportLongPolling(HttpServletRequest req) {
        return null != req.getHeader(LONG_POLLING_HEADER);
    }

    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    public LongPollingService() {
        allSubs = new ConcurrentLinkedQueue<>();

        ConfigExecutor.scheduleLongPolling(new StatTask(), 0L, 10L, TimeUnit.SECONDS);

        // Register LocalDataChangeEvent to NotifyCenter.
        NotifyCenter.registerToPublisher(LocalDataChangeEvent.class, NotifyCenter.ringBufferSize);

        // Register A Subscriber to subscribe LocalDataChangeEvent.
        NotifyCenter.registerSubscriber(new Subscriber() {

            @Override
            public void onEvent(Event event) {
                if (event instanceof LocalDataChangeEvent) {
                    LocalDataChangeEvent evt = (LocalDataChangeEvent) event;
                    ConfigExecutor.executeLongPolling(new DataChangeTask(evt.groupKey, evt.isBeta, evt.betaIps));
                }

            }

            @Override
            public Class<? extends Event> subscribeType() {
                return LocalDataChangeEvent.class;
            }
        });

    }

    public static final String LONG_POLLING_HEADER = "Long-Pulling-Timeout";

    public static final String LONG_POLLING_NO_HANG_UP_HEADER = "Long-Pulling-Timeout-No-Hangup";

    /**
     * ClientLongPolling subscibers.
     */
    final Queue<ClientLongPolling> allSubs;


    class DataChangeTask implements Runnable {

        @Override
        public void run() {
            try {
                // 作用: 遍历所有订阅者（allSubs），ClientLongPolling 是表示一个长轮询客户端的对象，使用迭代器来遍历每个订阅该配置的客户端
                for (Iterator<ClientLongPolling> iter = allSubs.iterator(); iter.hasNext(); ) {
                    ClientLongPolling clientSub = iter.next();
                    // 检查客户端的 clientMd5Map 中是否包含当前变更的 groupKey。
                    // clientMd5Map 是客户端持有的配置项及其对应的 MD5 值，用于验证配置是否已变更。
                    if (clientSub.clientMd5Map.containsKey(groupKey)) {

                        // If published tag is not in the tag list, then it skipped.
                        // 如果当前变更的配置项有 tag 且与客户端的 tag 不匹配，则跳过此客户端。这是为了确保客户端只接收匹配标签的配置项更新
                        if (StringUtils.isNotBlank(tag) && !tag.equals(clientSub.tag)) {
                            continue;
                        }
                        // 将当前客户端的 IP 和时间戳添加到 retainIps 中，表示该客户端成功接收到了配置更新
                        getRetainIps().put(clientSub.ip, System.currentTimeMillis());
                        //  从订阅列表中移除该客户端，因为该客户端已经收到最新的配置，不再需要继续长轮询
                        iter.remove(); // Delete subscribers' relationships.
                        LogUtil.CLIENT_LOG.info("{}|{}|{}|{}|{}|{}|{}", (System.currentTimeMillis() - changeTime),
                                "in-advance",
                                RequestUtil.getRemoteIp((HttpServletRequest) clientSub.asyncContext.getRequest()),
                                "polling", clientSub.clientMd5Map.size(), clientSub.probeRequestSize, groupKey);
                        // 调用客户端的 sendResponse 方法，通知该客户端变更的配置项（groupKey）。
                        // Collections.singletonList(groupKey) 表示只发送这个变更的配置项
                        clientSub.sendResponse(Collections.singletonList(groupKey));
                    }
                }

            } catch (Throwable t) {
                LogUtil.DEFAULT_LOG.error("data change error: {}", ExceptionUtil.getStackTrace(t));
            }
        }

        DataChangeTask(String groupKey, boolean isBeta, List<String> betaIps) {
            this(groupKey, isBeta, betaIps, null);
        }

        DataChangeTask(String groupKey, boolean isBeta, List<String> betaIps, String tag) {
            this.groupKey = groupKey;
            this.isBeta = isBeta;
            this.betaIps = betaIps;
            this.tag = tag;
        }

        /**
         * 作用: 用于标识一个配置项的唯一标识符，通常由 dataId、group 和 tenant 组合而成。
         * 例子: "exampleDataId+DEFAULT_GROUP+namespaceId"
         */
        final String groupKey;
        /**
         * 作用: 记录配置数据发生变更的时间戳，单位为毫秒。
         * 例子: 如果当前时间是 2024-01-01 12:00:00，则 changeTime 可能是 1704091200000。
         */
        final long changeTime = System.currentTimeMillis();
        /**
         * 作用: 标识该配置是否是 Beta 版本，Beta 版本通常用于灰度发布，特定的客户端会接收到 Beta 版本配置。
         * 例子: true 表示是 Beta 版本配置，false 表示不是
         */
        final boolean isBeta;
        /**
         * 作用: 存储接收 Beta 版本配置的客户端 IP 地址列表，只有这些 IP 的客户端才会收到 Beta 版本的配置更新。
         * 例子: ["192.168.1.10", "192.168.1.20"]，这表示这些 IP 的客户端会接收到 Beta 版本的配置。
         */
        final List<String> betaIps;
        /**
         * 作用: 配置项的标签，用于标识特定的配置，如果配置项有标签，客户端会根据标签订阅不同的配置。
         * 例子: "release"，代表该配置是用于正式发布的配置。
         */
        final String tag;
    }

    class StatTask implements Runnable {

        @Override
        public void run() {
            MEMORY_LOG.info("[long-pulling] client count " + allSubs.size());
            MetricsMonitor.getLongPollingMonitor().set(allSubs.size());
        }
    }

    /**
     * ClientLongPolling 类 实现了 Nacos 的长轮询机制，允许客户端通过异步方式持续监听配置变更。
     * 长轮询流程: 客户端发起长轮询请求 -> 服务器保持连接 -> 配置变更或超时时，服务器通知客户端并关闭连接。
     * 主要功能: 管理长轮询的超时任务、处理配置变更的通知、生成 HTTP 响应、取消超时任务等。
     */
    public class ClientLongPolling implements Runnable {

        @Override
        public void run() {
            // 作用: 通过 ConfigExecutor 安排一个长轮询超时任务。如果客户端在长轮询超时时间内没有收到配置变更通知，这个任务会执行超时逻辑。
            asyncTimeoutFuture = ConfigExecutor.scheduleLongPolling(() -> {
                try {
                    // 将客户端的 IP 和当前时间戳记录到 retainIps 中，用于跟踪客户端连接的活动状态。
                    getRetainIps().put(ClientLongPolling.this.ip, System.currentTimeMillis());

                    // Delete subscriber's relations.
                    // 从订阅者列表中移除当前客户端的长轮询任务，因为它已经超时或收到通知。
                    boolean removeFlag = allSubs.remove(ClientLongPolling.this);

                    if (removeFlag) {// 判断是否成功移除了订阅者关系，如果成功则继续处理
                        // 记录长轮询超时事件的日志信息，包括持续时间、客户端 IP、订阅的配置数量等
                        LogUtil.CLIENT_LOG.info("{}|{}|{}|{}|{}|{}", (System.currentTimeMillis() - createTime),
                                "timeout", RequestUtil.getRemoteIp((HttpServletRequest) asyncContext.getRequest()),
                                "polling", clientMd5Map.size(), probeRequestSize);
                        //  调用 sendResponse 方法，通知客户端配置没有变化，并结束长轮询请求。
                        sendResponse(null);

                    } else {// 如果订阅者关系删除失败，记录警告日志，提示问题
                        LogUtil.DEFAULT_LOG.warn("client subsciber's relations delete fail.");
                    }
                } catch (Throwable t) {
                    // 捕获任何异常并记录错误日志，防止任务崩溃
                    LogUtil.DEFAULT_LOG.error("long polling error:" + t.getMessage(), t.getCause());
                }

            }, timeoutTime, TimeUnit.MILLISECONDS);// 设置长轮询的超时时间，单位为毫秒。一旦超时，则执行上面的超时逻辑
            // 将当前客户端的长轮询任务添加到订阅者列表 allSubs 中，表示该客户端正在等待配置变更通知
            allSubs.add(this);
        }

        /**
         * 向客户端发送响应，通知配置的变化或超时
         *
         * @param changedGroups
         */
        void sendResponse(List<String> changedGroups) {

            // Cancel time out task.
            if (null != asyncTimeoutFuture) {// 如果超时任务尚未执行，取消该任务，防止它在发送响应后继续执行
                asyncTimeoutFuture.cancel(false);
            }
            //调用 generateResponse 方法，生成并发送 HTTP 响应给客户端
            generateResponse(changedGroups);
        }

        /**
         * 作用: 根据传入的 changedGroups 生成响应，通知客户端哪些配置项发生了变化
         *
         * @param changedGroups
         */
        void generateResponse(List<String> changedGroups) {
            // 如果 changedGroups 为 null，表示没有配置变更，直接完成异步请求并返回
            if (null == changedGroups) {
                // Tell web container to send http response.
                asyncContext.complete();
                return;
            }
            // 获取异步上下文的 HTTP 响应对象，用于向客户端发送数据
            HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();

            try {
                // 通过 MD5Util.compareMd5ResultString 方法，将变更的配置项生成响应字符串
                final String respString = MD5Util.compareMd5ResultString(changedGroups);

                // Disable cache. 服务器希望强制客户端每次请求时都从服务器重新获取数据，而不是从缓存中获取旧的内容
                // 设置响应头，禁用缓存，并将状态码设置为 200 (OK)
                /**
                 * 1. Pragma: no-cache
                 * 作用: 这是 HTTP/1.0 的头部字段，用于告诉客户端不要缓存响应内容。虽然它是为早期的 HTTP 协议设计的，但为了向后兼容，仍然广泛使用。
                 * 解释: 这个头意味着客户端在处理响应时不应该将其存储在缓存中，每次请求都应该直接从服务器获取最新的内容。
                 *
                 * 2. Expires: 0
                 * 作用: 这是一个 HTTP 头部，表示资源的过期时间。
                 * 解释: Expires: 0 意味着该响应在接收后立即过期。客户端收到响应后不会将其缓存，并且下次访问时必须从服务器重新请求数据。
                 *
                 * 3. Cache-Control: no-cache, no-store
                 * no-cache:
                 * 作用: 告诉客户端，在使用缓存的副本之前，必须向服务器验证响应的有效性。
                 * 解释: 即使客户端有缓存副本，也必须先向服务器检查响应是否已经过期或更新，不能直接使用缓存。
                 * no-store:
                 * 作用: 更为严格，表示客户端和中间代理都不应该存储任何响应的副本（包括内存缓存和硬盘缓存）。
                 * 解释: 服务器要求客户端和代理服务器完全不要缓存或保存响应内容。
                 *
                 * 4. HttpServletResponse.SC_OK
                 * 作用: 设置 HTTP 状态码为 200 OK，表示请求成功，服务器已经正常处理了请求。
                 * 解释: 尽管服务器响应成功，它仍然不希望客户端缓存这个结果，因此通过前面几个响应头明确禁止缓存行为。
                 */
                response.setHeader("Pragma", "no-cache");
                response.setDateHeader("Expires", 0);
                response.setHeader("Cache-Control", "no-cache,no-store");
                response.setStatus(HttpServletResponse.SC_OK);
                // 向客户端写入响应内容，通知客户端哪些配置项发生了变化
                response.getWriter().println(respString);
                // 完成异步处理，关闭连接，告诉服务器可以向客户端发送响应
                asyncContext.complete();
            } catch (Exception ex) {
                PULL_LOG.error(ex.toString(), ex);
                asyncContext.complete();
            }
        }

        ClientLongPolling(AsyncContext ac, Map<String, String> clientMd5Map, String ip, int probeRequestSize,
                          long timeoutTime, String appName, String tag) {
            this.asyncContext = ac;
            this.clientMd5Map = clientMd5Map;
            this.probeRequestSize = probeRequestSize;
            this.createTime = System.currentTimeMillis();
            this.ip = ip;
            this.timeoutTime = timeoutTime;
            this.appName = appName;
            this.tag = tag;
        }

        /**
         * 作用: AsyncContext 对象，代表异步处理的上下文，用于处理客户端的异步请求。
         * 长轮询的核心机制是异步处理，这允许服务器在保持连接的情况下等待配置变更。
         */
        final AsyncContext asyncContext;
        /**
         * 作用: 客户端持有的配置项及其对应的 MD5 值，用于和服务器端的配置进行比对，看是否有配置项发生了变化。
         * 例子: {"dataId1+groupId1": "md5_value_1", "dataId2+groupId2": "md5_value_2"}
         */
        final Map<String, String> clientMd5Map;
        /**
         * 作用: 记录客户端请求长轮询的创建时间，用于计算超时和统计相关信息
         */
        final long createTime;
        /**
         * 作用: 客户端的 IP 地址，用于记录客户端的连接和判断长轮询的来源。
         * 例子: "192.168.1.100"
         */
        final String ip;
        /**
         * 作用: 客户端的应用名称，用于标识是哪个应用发起的长轮询请求。
         * 例子: "myApp"
         */
        final String appName;
        /**
         * 作用: 配置的标签，用于区分不同环境、版本或场景的配置。客户端通过标签订阅不同的配置。
         * 例子: "release" 或 "beta"
         */
        final String tag;
        /**
         * 作用: 客户端发出的探测请求的大小，表示客户端一次请求中包含多少个配置项的 MD5 值。
         * 例子: 10，表示客户端正在请求 10 个配置项的状态
         */
        final int probeRequestSize;
        /**
         * 作用: 客户端设置的长轮询超时时间，表示长轮询的最长等待时间。
         * 例子: 30000 毫秒 (30 秒)。
         */
        final long timeoutTime;
        /**
         * 作用: 表示一个异步任务的 Future 对象，用于跟踪和管理长轮询任务的超时处理。
         * 例子: Future<?> future = executor.schedule(...);
         */
        Future<?> asyncTimeoutFuture;

        @Override
        public String toString() {
            return "ClientLongPolling{" + "clientMd5Map=" + clientMd5Map + ", createTime=" + createTime + ", ip='" + ip
                    + '\'' + ", appName='" + appName + '\'' + ", tag='" + tag + '\'' + ", probeRequestSize="
                    + probeRequestSize + ", timeoutTime=" + timeoutTime + '}';
        }
    }

    void generateResponse(HttpServletRequest request, HttpServletResponse response, List<String> changedGroups) {
        if (null == changedGroups) {
            return;
        }

        try {
            final String respString = MD5Util.compareMd5ResultString(changedGroups);
            // Disable cache.
            response.setHeader("Pragma", "no-cache");
            response.setDateHeader("Expires", 0);
            response.setHeader("Cache-Control", "no-cache,no-store");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println(respString);
        } catch (Exception ex) {
            PULL_LOG.error(ex.toString(), ex);
        }
    }

    void generate503Response(AsyncContext asyncContext, HttpServletResponse response, String message) {

        try {

            // Disable cache.
            response.setHeader("Pragma", "no-cache");
            response.setDateHeader("Expires", 0);
            response.setHeader("Cache-Control", "no-cache,no-store");
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            response.getWriter().println(message);
            asyncContext.complete();
        } catch (Exception ex) {
            PULL_LOG.error(ex.toString(), ex);
        }
    }

    public Map<String, Long> getRetainIps() {
        return retainIps;
    }

    public int getSubscriberCount() {
        return allSubs.size();
    }
}
