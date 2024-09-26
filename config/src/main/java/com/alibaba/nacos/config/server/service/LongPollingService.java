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
        /**
         * 作用: 创建一个 SampleResult 对象，用于保存该 IP 地址下的长轮询订阅者的配置信息。
         * 解释: SampleResult 是一个简单的数据封装类，用于存储客户端订阅的 groupKey 和其对应的 MD5 值。
         */
        SampleResult sampleResult = new SampleResult();
        /**
         * 作用: 初始化一个空的 Map，用于存储来自该 IP 地址的所有订阅者的 groupKey 和其对应的 MD5 值。
         * 解释: groupKey 是用于唯一标识客户端订阅配置的键，MD5 值用于检查客户端持有的配置版本是否与服务器上的一致。
         */
        Map<String, String> lisentersGroupkeyStatus = new HashMap<>(50);
        /**
         * 作用: 遍历所有长轮询订阅者 (allSubs)，检查客户端的 ip 是否与传入的 clientIp 匹配。
         * 解释: 通过过滤匹配的客户端，确保只收集来自特定 IP 地址的订阅者。
         */
        for (ClientLongPolling clientLongPolling : allSubs) {
            if (clientLongPolling.ip.equals(clientIp)) {
                // One ip can have multiple listener.
                /**
                 *
                 * 如果 lisentersGroupkeyStatus 与当前客户端的 clientMd5Map 不相同，
                 * 则将 clientMd5Map 中的所有 groupKey 和 MD5 值加入到 lisentersGroupkeyStatus 中
                 *
                 * clientMd5Map 是每个长轮询客户端保存的其订阅的配置及其 MD5 值。
                 * 通过将这些数据添加到 lisentersGroupkeyStatus 中，可以将该 IP 下的所有订阅者的配置信息合并
                 */
                if (!lisentersGroupkeyStatus.equals(clientLongPolling.clientMd5Map)) {
                    lisentersGroupkeyStatus.putAll(clientLongPolling.clientMd5Map);
                }
            }
        }
        // 作用: 将 lisentersGroupkeyStatus 设置到 SampleResult 对象中，表示该 IP 地址下的所有长轮询订阅者的订阅信息已经收集完毕。
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
        /**
         * 作用: 初始化 SampleResult 对象，并为其设置一个空的 Map，用于存储最终汇总的订阅信息。
         * 解释: 与 getSubscribleInfoByIp() 方法类似，sampleResult 将保存该 IP 地址下所有订阅者的 groupKey 和其对应的 MD5 值。
         */
        SampleResult sampleResult = new SampleResult();
        sampleResult.setLisentersGroupkeyStatus(new HashMap<String, String>(50));
        /**
         * 作用: 设置一个循环，执行 SAMPLE_TIMES 次采样。SAMPLE_TIMES 的值为 3（在类中定义），意味着系统将执行 3 次采样操作。
         * 解释: 这种多次采样可以确保数据的准确性，避免瞬时的状态导致遗漏一些订阅者信息。
         */
        for (int i = 0; i < SAMPLE_TIMES; i++) {
            /**
             * 作用: 调用 getSubscribleInfoByIp(ip) 方法获取该 IP 地址下的长轮询订阅者的配置信息。
             * 解释: 每次采样都会重新获取当前 IP 地址的订阅者信息。
             */
            SampleResult sampleTmp = getSubscribleInfoByIp(ip);
            /**
             * 作用: 如果当前采样结果不为空，则将该采样结果的订阅信息合并到最终的 sampleResult 中。
             * 第一个条件 (sampleTmp.getLisentersGroupkeyStatus() != null)：确保采样结果不为空。
             * 第二个条件：确保当前采样结果与已收集的信息不同，如果不同，则合并采样结果。
             * 解释: 通过多次采样并合并结果，确保最终的 sampleResult 包含完整的订阅信息。
             */
            if (sampleTmp != null) {
                if (sampleTmp.getLisentersGroupkeyStatus() != null && !sampleResult.getLisentersGroupkeyStatus()
                        .equals(sampleTmp.getLisentersGroupkeyStatus())) {
                    sampleResult.getLisentersGroupkeyStatus().putAll(sampleTmp.getLisentersGroupkeyStatus());
                }
            }
            /**
             * 作用: 在每次采样之后，系统会等待 SAMPLE_PERIOD 毫秒（设置为 100 毫秒）再进行下一次采样。
             * Thread.sleep(SAMPLE_PERIOD)：暂停当前线程一段时间（即 100 毫秒）。
             * 解释: 通过间隔一定时间的多次采样，避免数据瞬时变化的影响，同时给系统留出一定的缓冲时间
             */
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
     * 它的主要功能是添加一个长轮询客户端并处理相关的逻辑。
     * 长轮询是服务器与客户端保持一个长时间的 HTTP 连接，直到服务器有新数据或者超时，再返回给客户端
     *
     * @param req              HttpServletRequest. HTTP 请求对象，包含了客户端的请求信息和头部参数。
     * @param rsp              HttpServletResponse. HTTP 响应对象，用于返回给客户端数据。
     * @param clientMd5Map     clientMd5Map. 客户端提交的配置数据的 MD5 值集合，键是 dataId、group 和 tenant 生成的 groupKey，值是 MD5。
     * @param probeRequestSize probeRequestSize. 客户端请求的配置项数量，用于记录客户端请求包含的 MD5 键值对的数量
     */
    public void addLongPollingClient(HttpServletRequest req, HttpServletResponse rsp, Map<String, String> clientMd5Map,
                                     int probeRequestSize) {
        // 读取请求头 Long-Pulling-Timeout-No-Hangup 的值，用于判断客户端是否支持不挂起长轮询。
        // 如果客户端设置了这个头部，并且值为 "true"，那么服务器在收到这个请求后不会保持连接
        String noHangUpFlag = req.getHeader(LongPollingService.LONG_POLLING_NO_HANG_UP_HEADER);
        // 通过 MD5Util.compareMd5() 检查客户端提交的 MD5 是否和服务器端保存的配置项的 MD5 相同。
        // 如果不同，意味着配置项有变化，服务器应该立即返回这些变化给客户端
        long start = System.currentTimeMillis();
        List<String> changedGroups = MD5Util.compareMd5(req, rsp, clientMd5Map);
        //如果服务器检测到配置有变化，调用 generateResponse() 方法立即返回变化的配置给客户端，同时记录日志。
        if (changedGroups.size() > 0) {
            generateResponse(req, rsp, changedGroups);
            // 记录即时返回的日志
            LogUtil.CLIENT_LOG.info("{}|{}|{}|{}|{}|{}|{}", System.currentTimeMillis() - start, "instant",
                    RequestUtil.getRemoteIp(req), "polling", clientMd5Map.size(), probeRequestSize,
                    changedGroups.size());
            return;
        } else if (noHangUpFlag != null && noHangUpFlag.equalsIgnoreCase(TRUE_STR)) {
            // 如果客户端的请求头中包含 no-hangup 标志，表示客户端不希望服务器保持长时间的连接，服务器直接返回日志而不挂起连接
            LogUtil.CLIENT_LOG.info("{}|{}|{}|{}|{}|{}|{}", System.currentTimeMillis() - start, "nohangup",
                    RequestUtil.getRemoteIp(req), "polling", clientMd5Map.size(), probeRequestSize,
                    changedGroups.size());
            return;
        }

        // Must be called by http thread, or send response.
        // 果没有检测到配置变化，且客户端未设置 no-hangup，服务器会启动异步处理。
        // AsyncContext 允许服务器挂起该请求，而不占用线程资源。此时，线程会被释放，等待有新数据或超时后再处理
        final AsyncContext asyncContext = req.startAsync();


        // AsyncContext.setTimeout() is incorrect, Control by oneself
        // 将超时时间设置为 0 表示不使用容器的超时控制，超时控制将由业务逻辑自己管理
        asyncContext.setTimeout(0L);
        /**
         * 作用: 通过 RequestUtil.getRemoteIp() 方法从 HttpServletRequest 中提取客户端的 IP 地址。
         * 解释: 每个客户端请求都会携带 IP 地址，服务器使用该 IP 来进行连接限制、日志记录等操作
         */
        String ip = RequestUtil.getRemoteIp(req);
        ConnectionCheckResponse connectionCheckResponse = checkLimit(req);
        if (!connectionCheckResponse.isSuccess()) {
            /**
             * 使用 RpcScheduledExecutor.CONTROL_SCHEDULER 调度一个异步任务，1 到 3 秒后向客户端返回 503 Service Unavailable 响应。
             *
             * generate503Response(): 用于生成 503 响应，告诉客户端服务器当前无法处理请求。
             * asyncContext: 表示异步处理的上下文，它允许在异步处理中延迟返回响应。
             * rsp: HttpServletResponse 对象，用于发送 HTTP 响应。
             * connectionCheckResponse.getMessage(): 包含连接检查失败的详细信息，将会包含在返回的 503 错误响应中。
             */
            RpcScheduledExecutor.CONTROL_SCHEDULER.schedule(
                    () -> generate503Response(asyncContext, rsp, connectionCheckResponse.getMessage()),
                    1000L + new Random().nextInt(2000), TimeUnit.MILLISECONDS);
            /**
             * 作用: 终止方法的执行。由于连接检查失败，服务器不再继续处理请求，直接返回。
             * 解释: return 结束了 addLongPollingClient 方法的进一步执行，表示已经向客户端发送了错误响应。
             */
            return;
        }
        /**
         * 作用: 从请求头中提取客户端的应用名称。
         * 解释: appName 是客户端应用的标识，用于记录和跟踪请求的来源。RequestUtil.CLIENT_APPNAME_HEADER 表示客户端发送的请求头名称。
         * 例子: 如果请求头 Client-AppName 为 "myApp"，则 appName 变量将被赋值为 "myApp"。
         */
        String appName = req.getHeader(RequestUtil.CLIENT_APPNAME_HEADER);
        /**
         * 作用: 从请求头中提取配置的标签值。
         * 解释: tag 用于区分不同版本或环境的配置（如 beta、release 等），可以让服务器根据标签返回特定版本的配置。
         * 例子: 如果客户端的请求头 Vipserver-Tag 为 "release"，则 tag 变量会保存 "release"
         */
        String tag = req.getHeader("Vipserver-Tag");
        /**
         * 解释: delayTime 是为了负载均衡或其他策略所增加的延迟时间，通常用于防止客户端超时。服务器在响应客户端之前会延迟一小段时间。
         * 例子: 如果 FIXED_DELAY_TIME 配置为 300，则 delayTime 为 300；如果没有配置该参数，则使用默认值 500。
         * java
         */
        int delayTime = SwitchService.getSwitchInteger(SwitchService.FIXED_DELAY_TIME, 500);
        /**
         * 作用: 获取 MIN_LONG_POOLING_TIMEOUT 的配置值，表示服务器允许的最小长轮询超时时间，如果该值不存在，则默认设置为 10000 毫秒（10 秒）。
         * 解释: minLongPoolingTimeout 确保客户端的长轮询请求有一个最低的等待时间，防止客户端设置过低的超时时间，造成频繁请求服务器。
         */
        int minLongPoolingTimeout = SwitchService.getSwitchInteger("MIN_LONG_POOLING_TIMEOUT", 10000);

        // Add delay time for LoadBalance, and one response is returned 500 ms in advance to avoid client timeout.

        // 作用: 从客户端请求头中读取 Long-Pulling-Timeout，这是客户端希望长轮询保持的最大等待时间。
        String requestLongPollingTimeOut = req.getHeader(LongPollingService.LONG_POLLING_HEADER);
        // 计算服务器设置的实际长轮询超时时间。timeout 的值是客户端请求的超时时间减去延迟时间 delayTime 和最小超时时间 minLongPoolingTimeout 中的最大值
        long timeout = Math.max(minLongPoolingTimeout, Long.parseLong(requestLongPollingTimeOut) - delayTime);
        // 启动一个新的 ClientLongPolling 任务，负责长轮询处理。
        // 这个任务会异步执行，并在服务器检测到配置有变化时，或者超时时返回响应给客户端。
        /**
         * 参数解释:
         * AsyncContext asyncContext: 异步上下文，包含了客户端请求的处理上下文。
         * Map<String, String> clientMd5Map: 客户端提交的 MD5 值集合。
         * String ip: 客户端 IP 地址。
         * int probeRequestSize: 客户端请求的配置项数量。
         * long timeout: 长轮询的超时时间。
         * String appName: 客户端的应用名称。
         * String tag: 配置的标签（如果有）
         */
        ConfigExecutor.executeLongPolling(
                new ClientLongPolling(asyncContext, clientMd5Map, ip, probeRequestSize, timeout, appName, tag));
    }

    private ConnectionCheckResponse checkLimit(HttpServletRequest httpServletRequest) {
        /**
         * 作用: 从 HttpServletRequest 中提取客户端的 IP 地址。
         * 解释: RequestUtil.getRemoteIp() 是一个工具方法，用于获取客户端的 IP。
         * 服务器会使用这个 IP 来检查该客户端的连接是否合法或是否超出了限制。
         * 例子: 如果客户端 IP 是 192.168.1.100，那么 ip 变量就会存储这个 IP 地址。
         */
        String ip = RequestUtil.getRemoteIp(httpServletRequest);
        /**
         * 作用: 从请求头中提取客户端的应用名称。
         * 解释: appName 是客户端的应用标识，用于区分不同的应用程序。服务器会根据这个字段来判断是否对某个应用有特殊的连接限制
         */
        String appName = httpServletRequest.getHeader(RequestUtil.CLIENT_APPNAME_HEADER);
        /**
         * 作用: 该类用于封装客户端发起的连接检查请求。它主要包含以下几个字段：
         * clientIp: 客户端的 IP 地址，用于识别请求来源。
         * appName: 客户端的应用名称，用于对不同的应用进行连接限制管理。
         * source: 连接的来源（如 "LongPolling"），用于区分不同类型的请求。
         * labels: 额外的标签信息，可能用于细粒度的连接控制。
         */
        ConnectionCheckRequest connectionCheckRequest = new ConnectionCheckRequest(ip, appName, "LongPolling");
        ConnectionCheckResponse checkResponse = ControlManagerCenter.getInstance().getConnectionControlManager()
                // TODO 查看 com.alibaba.nacos.plugin.control.impl.NacosConnectionControlManager.check
                // TODO 查看 com.alibaba.nacos.config.server.service.LongPollingConnectionMetricsCollector
                .check(connectionCheckRequest);
        return checkResponse;
    }

    public static boolean isSupportLongPolling(HttpServletRequest req) {
        return null != req.getHeader(LONG_POLLING_HEADER);
    }

    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    public LongPollingService() {
        //订阅列表
        allSubs = new ConcurrentLinkedQueue<>();
        //长轮训任务  10 秒执行一次 长轮训监控使用的
        ConfigExecutor.scheduleLongPolling(new StatTask(), 0L, 10L, TimeUnit.SECONDS);

        // Register LocalDataChangeEvent to NotifyCenter.
        //注册一个 LocalDataChangeEvent 事件
        NotifyCenter.registerToPublisher(LocalDataChangeEvent.class, NotifyCenter.ringBufferSize);

        // Register A Subscriber to subscribe LocalDataChangeEvent.
        //注册订阅者来订阅 LocalDataChangeEvent 事件
        NotifyCenter.registerSubscriber(new Subscriber() {

            @Override
            public void onEvent(Event event) {
                if (event instanceof LocalDataChangeEvent) { //事件类型判断
                    //是 LocalDataChangeEvent 类型事件 强转为 LocalDataChangeEvent 事件
                    LocalDataChangeEvent evt = (LocalDataChangeEvent) event;
                    //异步执行任务 重点关注 DataChangeTask
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
            response.getWriter().println(respString);
        } catch (Exception ex) {
            PULL_LOG.error(ex.toString(), ex);
        }
    }

    void generate503Response(AsyncContext asyncContext, HttpServletResponse response, String message) {

        try {

            // Disable cache.
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
