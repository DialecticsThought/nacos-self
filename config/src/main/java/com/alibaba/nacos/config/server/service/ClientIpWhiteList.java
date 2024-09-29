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

import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.config.server.model.AclInfo;
import com.alibaba.nacos.common.utils.StringUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.nacos.config.server.utils.LogUtil.DEFAULT_LOG;

/**
 * Client ip whitelist.
 *
 * @author Nacos
 * <pre>
 *   白名单判断：可以判断给定的客户端 IP 是否在白名单中。
 *   启用/关闭白名单：通过设置 isOpen 来控制白名单功能是否启用。
 *   加载白名单：从字符串内容中加载白名单，并更新白名单的状态和内容
 * </pre>
 */
@Service
public class ClientIpWhiteList {
    // 常量，表示存储客户端 IP 白名单的元数据标识符，可能用于在配置中心中标记白名单数据
    public static final String CLIENT_IP_WHITELIST_METADATA = "com.alibaba.nacos.metadata.clientIpWhitelist";
    // 这是一个 AtomicReference 对象，存储当前的 IP 白名单列表。使用 AtomicReference 主要是为了确保在多线程环境中更新列表时的线程安全性
    private static final AtomicReference<List<String>> CLIENT_IP_WHITELIST = new AtomicReference<>(
            new ArrayList<>());
    // 这是一个 AtomicReference 对象，存储当前的 IP 白名单列表。使用 AtomicReference 主要是为了确保在多线程环境中更新列表时的线程安全性
    private static Boolean isOpen = false;

    /**
     * Judge whether specified client ip includes in the whitelist.
     *
     * @param clientIp clientIp string value.
     * @return Judge result.
     *
     * TODO 用于判断传入的 clientIp 是否在白名单中
     */
    public static boolean isLegalClient(String clientIp) {
        if (StringUtils.isBlank(clientIp)) {
            throw new IllegalArgumentException("clientIp is empty");
        }
        // 对 clientIp 进行去除空白字符的处理，
        clientIp = clientIp.trim();
        // 然后检查它是否存在于 CLIENT_IP_WHITELIST 中
        if (CLIENT_IP_WHITELIST.get().contains(clientIp)) {
            return true;
        }
        return false;
    }

    /**
     * Whether start client ip whitelist.
     *
     * @return true: enable ; false disable
     */
    public static boolean isEnableWhitelist() {
        return isOpen;
    }

    /**
     * Load white lists based content parameter value.
     *
     * @param content content string value.
     *
     * TODO 加载白名单内容，并根据加载的结果更新白名单和是否启用白名单的标志。
     */
    public static void load(String content) {
        if (StringUtils.isBlank(content)) {
            DEFAULT_LOG.warn("clientIpWhiteList is blank.close whitelist.");
            isOpen = false;
            CLIENT_IP_WHITELIST.get().clear();
            return;
        }
        DEFAULT_LOG.warn("[clientIpWhiteList] {}", content);
        try {
            // 通过 JacksonUtils.toObj 将 content 转换为 AclInfo 对象
            AclInfo acl = JacksonUtils.toObj(content, AclInfo.class);
            // 根据转换的结果，设置白名单是否启用
            isOpen = acl.getIsOpen();
            // 将 IP 列表设置为新的白名单
            CLIENT_IP_WHITELIST.set(acl.getIps());
        } catch (Exception ioe) {
            DEFAULT_LOG.error("failed to load clientIpWhiteList, " + ioe.toString(), ioe);
        }
    }
}
