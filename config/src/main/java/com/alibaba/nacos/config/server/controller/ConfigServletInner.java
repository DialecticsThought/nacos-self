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

package com.alibaba.nacos.config.server.controller;

import com.alibaba.nacos.api.model.v2.ErrorCode;
import com.alibaba.nacos.api.model.v2.Result;
import com.alibaba.nacos.common.constant.HttpHeaderConsts;
import com.alibaba.nacos.common.http.param.MediaType;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.Pair;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.constant.Constants;
import com.alibaba.nacos.config.server.enums.FileTypeEnum;
import com.alibaba.nacos.config.server.model.CacheItem;
import com.alibaba.nacos.config.server.model.ConfigCache;
import com.alibaba.nacos.config.server.service.ConfigCacheService;
import com.alibaba.nacos.config.server.service.LongPollingService;
import com.alibaba.nacos.config.server.service.dump.disk.ConfigDiskServiceFactory;
import com.alibaba.nacos.config.server.service.trace.ConfigTraceService;
import com.alibaba.nacos.config.server.utils.GroupKey2;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.alibaba.nacos.config.server.utils.MD5Util;
import com.alibaba.nacos.config.server.utils.Protocol;
import com.alibaba.nacos.config.server.utils.RequestUtil;
import com.alibaba.nacos.config.server.utils.TimeUtils;
import com.alibaba.nacos.plugin.encryption.handler.EncryptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static com.alibaba.nacos.config.server.constant.Constants.ENCODE_UTF8;
import static com.alibaba.nacos.config.server.utils.LogUtil.PULL_LOG;

/**
 * ConfigServlet inner for aop.
 *
 * @author Nacos
 */
@Service
public class ConfigServletInner {

    private static final int TRY_GET_LOCK_TIMES = 9;

    private static final int START_LONG_POLLING_VERSION_NUM = 204;

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigServletInner.class);

    private final LongPollingService longPollingService;

    public ConfigServletInner(LongPollingService longPollingService) {
        this.longPollingService = longPollingService;
    }

    /**
     * long polling the config.
     */
    public String doPollingConfig(HttpServletRequest request, HttpServletResponse response,
                                  Map<String, String> clientMd5Map, int probeRequestSize) throws IOException {

        // Long polling.
        if (LongPollingService.isSupportLongPolling(request)) {
            longPollingService.addLongPollingClient(request, response, clientMd5Map, probeRequestSize);
            return HttpServletResponse.SC_OK + "";
        }

        // Compatible with short polling logic.
        List<String> changedGroups = MD5Util.compareMd5(request, response, clientMd5Map);

        // Compatible with short polling result.
        String oldResult = MD5Util.compareMd5OldResult(changedGroups);
        String newResult = MD5Util.compareMd5ResultString(changedGroups);

        String version = request.getHeader(Constants.CLIENT_VERSION_HEADER);
        if (version == null) {
            version = "2.0.0";
        }
        int versionNum = Protocol.getVersionNumber(version);

        // Before 2.0.4 version, return value is put into header.
        if (versionNum < START_LONG_POLLING_VERSION_NUM) {
            response.addHeader(Constants.PROBE_MODIFY_RESPONSE, oldResult);
            response.addHeader(Constants.PROBE_MODIFY_RESPONSE_NEW, newResult);
        } else {
            request.setAttribute("content", newResult);
        }

        // Disable cache.
        response.setHeader("Pragma", "no-cache");
        response.setDateHeader("Expires", 0);
        response.setHeader("Cache-Control", "no-cache,no-store");
        response.setStatus(HttpServletResponse.SC_OK);
        return HttpServletResponse.SC_OK + "";
    }

    /**
     * Execute to get config [API V1].
     */
    public String doGetConfig(HttpServletRequest request, HttpServletResponse response, String dataId, String group,
                              String tenant, String tag, String isNotify, String clientIp) throws IOException, ServletException {
        // TODO 进入
        return doGetConfig(request, response, dataId, group, tenant, tag, isNotify, clientIp, false);
    }

    /**
     * Execute to get config [API V1] or [API V2].
     * <pre>
     *    ConfigServletInner#doGetConfig 方法代码比较多，主要做了如下几件事：
     *   - 自旋获取读锁，最多自旋10次。
     *   - 根据 beta，tag，autoTag 来判断读什么配置，其中有多个分支，具体看源码注释。
     *   - 判断从数据库读取配置还是从本地文件读取配置，这个判断使用了多次。
     *   - 获取到的配置结果写入到 response 中，如果读取的是本地文件，这里会使用零拷贝技术。
     *   - 释放读锁，关闭流。
     * </pre>
     */
    public String doGetConfig(HttpServletRequest request, HttpServletResponse response, String dataId, String group,
                              String tenant, String tag, String isNotify, String clientIp, boolean isV2) throws IOException {
        // 检查 isNotify 参数是否不为空，并将其转换为布尔值，决定是否启用通知
        boolean notify = StringUtils.isNotBlank(isNotify) && Boolean.parseBoolean(isNotify);
        // 设置字符编码为 UTF-8
        String acceptCharset = ENCODE_UTF8;
        // 如果请求的 isV2 为 true，设置响应头的 Content-Type 为 application/json
        if (isV2) {
            response.setHeader(HttpHeaderConsts.CONTENT_TYPE, MediaType.APPLICATION_JSON);
        }
        // 使用 dataId、group 和 tenant（租户）生成一个 groupKey，用于标识某个配置项
        final String groupKey = GroupKey2.getKey(dataId, group, tenant);

        // 从请求头中获取 autoTag，同时从请求中获取 requestIpApp（请求应用的名称）
        String autoTag = request.getHeader(com.alibaba.nacos.api.common.Constants.VIPSERVER_TAG);
        String requestIpApp = RequestUtil.getAppName(request);
        // 尝试获取 groupKey 的读锁，以确保在读取配置内容时不会发生并发冲突。如果锁获取成功，lockResult 会大于 0
        int lockResult = ConfigCacheService.tryConfigReadLock(groupKey);
        // 从缓存中获取与 groupKey 对应的 CacheItem，如果存在，则后续用来读取配置内容
        CacheItem cacheItem = ConfigCacheService.getContentCache(groupKey);

        // 从请求中提取客户端的 IP 地址
        final String requestIp = RequestUtil.getRemoteIp(request);

        // 判断是否成功获取锁，并且 cacheItem 不为空
        if (lockResult > 0 && cacheItem != null) {
            try {
                long lastModified;
                //检查当前配置是否是 Beta 版本，同时客户端的 IP 地址是否在 Beta 版本允许的 IP 列表中
                boolean isBeta =
                        cacheItem.isBeta() && cacheItem.getConfigCacheBeta() != null && cacheItem.getIps4Beta() != null
                                && cacheItem.getIps4Beta().contains(clientIp);
                // 如果配置有指定类型，则设置响应头的 CONFIG_TYPE 为该类型。如果没有指定类型，默认为文本文件类型
                final String configType =
                        (null != cacheItem.getType()) ? cacheItem.getType() : FileTypeEnum.TEXT.getFileType();
                // 设置响应头的 CONFIG_TYPE，用以标识配置内容的类型
                response.setHeader(com.alibaba.nacos.api.common.Constants.CONFIG_TYPE, configType);
                // 根据 configType 获取对应的文件类型枚举 fileTypeEnum
                FileTypeEnum fileTypeEnum = FileTypeEnum.getFileTypeEnumByFileExtensionOrFileType(configType);
                // 获取 fileTypeEnum 对应的内容类型 contentTypeHeader
                String contentTypeHeader = fileTypeEnum.getContentType();
                response.setHeader(HttpHeaderConsts.CONTENT_TYPE,
                        isV2 ? MediaType.APPLICATION_JSON : contentTypeHeader);
                // 定义一个字符串变量 pullEvent，用于跟踪配置拉取的事件类型
                String pullEvent;
                // 定义一个字符串变量 content，用于保存拉取的配置内容
                String content;
                // 定义一个字符串变量 md5，用于保存配置内容的 MD5 校验值
                String md5;
                // 定义一个字符串变量 encryptedDataKey，用于保存加密配置的加密密钥
                String encryptedDataKey;
                if (isBeta) {// 判断是否是 Beta 版本的配置
                    // 获取 cacheItem 中的 Beta 版本缓存 configCacheBeta
                    ConfigCache configCacheBeta = cacheItem.getConfigCacheBeta();
                    // 设置 pullEvent 为 Beta 版本拉取事件类型
                    pullEvent = ConfigTraceService.PULL_EVENT_BETA;
                    // 获取 Beta 版本配置的 MD5 值
                    md5 = configCacheBeta.getMd5(acceptCharset);
                    // 获取 Beta 版本配置的最后修改时间
                    lastModified = configCacheBeta.getLastModifiedTs();
                    // 获取 Beta 版本配置的加密密钥
                    encryptedDataKey = configCacheBeta.getEncryptedDataKey();
                    // 从磁盘中读取 Beta 版本的配置内容
                    content = ConfigDiskServiceFactory.getInstance().getBetaContent(dataId, group, tenant);
                    // 在响应头中标记 isBeta 为 true，表明返回的是 Beta 版本配置
                    response.setHeader("isBeta", "true");
                } else { // 否则，处理非 Beta 版本的配置逻辑
                    if (StringUtils.isBlank(tag)) { // 如果 tag 为空，执行自动标签的逻辑
                        if (isUseTag(cacheItem, autoTag)) { // 判断是否应该使用 autoTag 标签
                            // 获取与 autoTag 对应的标签配置缓存 configCacheTag
                            ConfigCache configCacheTag = cacheItem.getConfigCacheTags().get(autoTag);
                            // 获取标签配置的 MD5 值
                            md5 = configCacheTag.getMd5(acceptCharset);
                            // 获取标签配置的最后修改时间
                            lastModified = configCacheTag.getLastModifiedTs();
                            // 获取标签配置的加密密钥
                            encryptedDataKey = configCacheTag.getEncryptedDataKey();
                            // 从磁盘中读取带有标签的配置内容
                            content = ConfigDiskServiceFactory.getInstance()
                                    .getTagContent(dataId, group, tenant, autoTag);
                            // 设置拉取事件类型为标签配置拉取，带上 autoTag
                            pullEvent = ConfigTraceService.PULL_EVENT_TAG + "-" + autoTag;
                            // 设置响应头中的 VIPSERVER_TAG 为 URL 编码后的 autoTag
                            response.setHeader(com.alibaba.nacos.api.common.Constants.VIPSERVER_TAG,
                                    URLEncoder.encode(autoTag, StandardCharsets.UTF_8.displayName()));
                        } else { // 如果不使用自动标签，则处理普通版本的配置逻辑
                            // 设置拉取事件类型为普通版本的配置拉取
                            pullEvent = ConfigTraceService.PULL_EVENT;
                            // 获取普通版本配置的 MD5 值
                            md5 = cacheItem.getConfigCache().getMd5(acceptCharset);
                            // 获取普通版本配置的最后修改时间
                            lastModified = cacheItem.getConfigCache().getLastModifiedTs();
                            // 获取普通版本配置的加密密钥
                            encryptedDataKey = cacheItem.getConfigCache().getEncryptedDataKey();
                            // 从磁盘中读取普通版本的配置内容
                            content = ConfigDiskServiceFactory.getInstance().getContent(dataId, group, tenant);
                        }
                    } else {// 如果标签 tag 不为空，则处理标签版本的配置
                        // 获取标签版本配置的 MD5 值
                        md5 = cacheItem.getTagMd5(tag, acceptCharset);
                        // 获取标签版本配置的最后修改时间
                        lastModified = cacheItem.getTagLastModified(tag);
                        // 获取标签版本配置的最后修改时间
                        encryptedDataKey = cacheItem.getTagEncryptedDataKey(tag);
                        // 从磁盘中读取
                        content = ConfigDiskServiceFactory.getInstance().getTagContent(dataId, group, tenant, tag);
                        //设置拉取事件类型为标签配置拉取，带上 tag
                        pullEvent = ConfigTraceService.PULL_EVENT_TAG + "-" + tag;
                    }
                }

                if (content == null) {// 如果 content 为空，说明配置不存在
                    // 记录配置拉取失败的日志
                    ConfigTraceService.logPullEvent(dataId, group, tenant, requestIpApp, -1, pullEvent,
                            ConfigTraceService.PULL_TYPE_NOTFOUND, -1, requestIp, notify, "http");
                    // 返回 404 错误结果
                    return get404Result(response, isV2);

                }
                // 设置响应头的 MD5 值
                response.setHeader(Constants.CONTENT_MD5, md5);

                // Disable cache.
                response.setHeader("Pragma", "no-cache");
                // 设置 Expires 头为 0，表示立即过期
                response.setDateHeader("Expires", 0);
                // 设置 Cache-Control 响应头，禁止客户端缓存
                response.setHeader("Cache-Control", "no-cache,no-store");
                // 设置响应头的 Last-Modified 为配置的最后修改时间
                response.setDateHeader("Last-Modified", lastModified);
                if (encryptedDataKey != null) {
                    response.setHeader("Encrypted-Data-Key", encryptedDataKey);
                }
                // 声明 PrintWriter 用于输出配置内容到响应
                PrintWriter out;
                // 调用解密处理器，解密加密的配置内容
                Pair<String, String> pair = EncryptionHandler.decryptHandler(dataId, encryptedDataKey, content);
                // 获取解密后的配置内容
                String decryptContent = pair.getSecond();
                // 获取响应的 PrintWriter 对象，用于输出内容
                out = response.getWriter();
                // 如果 isV2 为 true，将解密后的内容转换为 JSON 格式并写入响应；否则直接输出解密后的配置内容
                if (isV2) {
                    out.print(JacksonUtils.toJson(Result.success(decryptContent)));
                } else {
                    out.print(decryptContent);
                }
                // 刷新输出流，确保内容被写入
                out.flush();
                // 关闭输出流，释放资源
                out.close();

                LogUtil.PULL_CHECK_LOG.warn("{}|{}|{}|{}", groupKey, requestIp, md5, TimeUtils.getCurrentTimeStr());
                // 计算延迟时间，如果需要通知则设置为 -1，否则计算当前时间与最后修改时间的差值
                final long delayed = notify ? -1 : System.currentTimeMillis() - lastModified;
                // 记录成功的配置拉取事件日志
                ConfigTraceService.logPullEvent(dataId, group, tenant, requestIpApp, lastModified, pullEvent,
                        ConfigTraceService.PULL_TYPE_OK, delayed, clientIp, notify, "http");
            } finally {
                // 在 finally 块中，释放之前获取的读锁
                ConfigCacheService.releaseReadLock(groupKey);
            }
        } else if (lockResult == 0 || cacheItem == null) {// 如果没有成功获取锁或 cacheItem 为空，处理未找到配置的情况
            // 未找到配置的日志事件
            ConfigTraceService.logPullEvent(dataId, group, tenant, requestIpApp, -1, ConfigTraceService.PULL_EVENT,
                    ConfigTraceService.PULL_TYPE_NOTFOUND, -1, requestIp, notify, "http");
            // 返回 404 错误结果
            return get404Result(response, isV2);

        } else {// 如果配置正在被修改（锁未获取），返回 409 冲突
            // 记录客户端获取数据时遇到冲突的日志
            PULL_LOG.info("[client-get] clientIp={}, {}, get data during dump", clientIp, groupKey);
            // 返回 409 冲突错误结果
            return get409Result(response, isV2);
        }

        return HttpServletResponse.SC_OK + "";
    }

    private String get404Result(HttpServletResponse response, boolean isV2) throws IOException {
        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        PrintWriter writer = response.getWriter();
        if (isV2) {
            writer.println(JacksonUtils.toJson(Result.failure(ErrorCode.RESOURCE_NOT_FOUND, "config data not exist")));
        } else {
            writer.println("config data not exist");
        }
        return HttpServletResponse.SC_NOT_FOUND + "";
    }

    private String get409Result(HttpServletResponse response, boolean isV2) throws IOException {
        response.setStatus(HttpServletResponse.SC_CONFLICT);
        PrintWriter writer = response.getWriter();
        if (isV2) {
            writer.println(JacksonUtils.toJson(Result.failure(ErrorCode.RESOURCE_CONFLICT,
                    "requested file is being modified, please try later.")));
        } else {
            writer.println("requested file is being modified, please try later.");
        }
        return HttpServletResponse.SC_CONFLICT + "";
    }

    private static boolean isUseTag(CacheItem cacheItem, String tag) {
        return cacheItem != null && cacheItem.getConfigCacheTags() != null && cacheItem.getConfigCacheTags()
                .containsKey(tag);
    }

}
