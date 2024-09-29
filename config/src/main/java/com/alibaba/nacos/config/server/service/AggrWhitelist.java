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

import com.alibaba.nacos.common.utils.IoUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.utils.RegexParser;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static com.alibaba.nacos.config.server.utils.LogUtil.DEFAULT_LOG;
import static com.alibaba.nacos.config.server.utils.LogUtil.FATAL_LOG;

/**
 * AggrWhitelist.
 * AggrWhitelist：这是一个工具类，专门用于处理 Nacos 中的聚合白名单（Aggregation Whitelist）
 * @author Nacos
 */
public class AggrWhitelist {
    // AGGRIDS_METADATA：这是一个常量，表示存储聚合白名单的 dataId 的元数据信息，用于标识聚合数据的唯一标识符
    public static final String AGGRIDS_METADATA = "com.alibaba.nacos.metadata.aggrIDs";
    // 这是一个 AtomicReference，用于存储当前的白名单，它包含了一组 Pattern 对象（正则表达式）
    // 使用 AtomicReference 是为了确保在多线程环境下白名单的安全更新
    static final AtomicReference<List<Pattern>> AGGR_DATAID_WHITELIST = new AtomicReference<>(
            new ArrayList<>());

    /**
     * Judge whether specified dataId includes aggregation white list.
     *
     * @param dataId dataId string value.
     * @return Whether to match aggregation rules.
     */
    public static boolean isAggrDataId(String dataId) {
        if (null == dataId) {
            throw new IllegalArgumentException("dataId is null");
        }
        // 遍历白名单：通过遍历 AGGR_DATAID_WHITELIST 中的正则表达式列表，检查 dataId 是否与其中某个正则表达式匹配。
        for (Pattern pattern : AGGR_DATAID_WHITELIST.get()) {
            // 如果有任何正则表达式匹配该 dataId，则返回 true，否则返回 false
            if (pattern.matcher(dataId).matches()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Load aggregation white lists based content parameter value.
     *
     * @param content content string value.
     */
    public static void load(String content) {
        if (StringUtils.isBlank(content)) {
            FATAL_LOG.warn("aggr dataId whitelist is blank.");
            return;
        }
        DEFAULT_LOG.warn("[aggr-dataIds] {}", content);

        try {
            // 使用 IoUtils.readLines 读取 content 字符串中的每一行数据，并将其转换为一个 List<String>
            // 每一行代表一个白名单条目（或正则表达式）
            List<String> lines = IoUtils.readLines(new StringReader(content));
            // 调用 compile 方法，将每一行白名单条目编译为正则表达式并存储到白名单中
            compile(lines);
        } catch (Exception ioe) {
            DEFAULT_LOG.error("failed to load aggr whitelist, " + ioe, ioe);
        }
    }

    static void compile(List<String> whitelist) {
        // 创建 list：创建一个空的 ArrayList，用于存储编译后的正则表达式
        List<Pattern> list = new ArrayList<>(whitelist.size());
        // 对 whitelist 中的每一行，去除空白字符后，
        // 如果不为空，则调用 RegexParser.regexFormat 将条目格式化为正则表达式格式，然后编译为 Pattern 对象
        for (String line : whitelist) {
            if (!StringUtils.isBlank(line)) {
                String regex = RegexParser.regexFormat(line.trim());
                list.add(Pattern.compile(regex));
            }
        }
        // 将生成的正则表达式列表通过 AGGR_DATAID_WHITELIST.set(list) 设置为当前白名单
        AGGR_DATAID_WHITELIST.set(list);
    }

    public static List<Pattern> getWhiteList() {
        return AGGR_DATAID_WHITELIST.get();
    }
}
