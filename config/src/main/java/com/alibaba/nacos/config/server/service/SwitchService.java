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
import com.alibaba.nacos.config.server.utils.LogUtil;

import com.alibaba.nacos.common.utils.StringUtils;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.nacos.config.server.utils.LogUtil.FATAL_LOG;

/**
 * SwitchService.
 *
 * @author Nacos
 *
 * TODO Nacos 配置服务中的一个工具类，主要用于加载和管理开关配置。开关配置一般用于控制系统的行为，如延时配置等
 */
@Service
public class SwitchService {
    // 这是存储开关配置元数据的标识符，常用于标识特定的配置项（如在数据库或配置文件中）
    public static final String SWITCH_META_DATA_ID = "com.alibaba.nacos.meta.switch";
    // 表示一个固定延迟时间的开关配置，可能用于控制系统中的某种定时行为
    public static final String FIXED_DELAY_TIME = "fixedDelayTime";
    // 这是一个 Map，用于存储当前加载的开关配置，键是开关的名称，值是对应的开关值。
    // 使用 volatile 确保多线程环境下对该变量的可见性，即所有线程能看到最新的开关配置
    private static volatile Map<String, String> switches = new HashMap<>();
    // 于获取开关的整数值。如果开关配置中没有对应的值，或者值无法解析为整数，则返回默认值
    public static int getSwitchInteger(String key, int defaultValue) {
        int rtn;
        try {
            String status = switches.get(key);
            rtn = status != null ? Integer.parseInt(status) : defaultValue;
        } catch (Exception e) {
            rtn = defaultValue;
            LogUtil.FATAL_LOG.error("corrupt switch value {}={}", key, switches.get(key));
        }
        return rtn;
    }

    /**
     * Load config.
     *
     * @param config config content string value.
     */
    public static void load(String config) {
        if (StringUtils.isBlank(config)) {
            FATAL_LOG.warn("switch config is blank.");
            return;
        }
        FATAL_LOG.warn("[switch-config] {}", config);
        // 创建一个空的 HashMap，用于存储解析出来的键值对
        Map<String, String> map = new HashMap<>(30);
        try {
            // IoUtils.readLines(new StringReader(config))：逐行读取配置内容，每行代表一个配置项
            for (String line : IoUtils.readLines(new StringReader(config))) {
                // 如果某行为空，或以 # 开头（表示注释），则跳过处理
                if (!StringUtils.isBlank(line) && !line.startsWith("#")) {
                    // 对于每行非空且不是注释的行，通过 = 号分割，解析为键和值，并将它们加入到 map 中。
                    // 如果某行没有正确的 = 分隔符，记录错误日志并跳过该行
                    String[] array = line.split("=");

                    if (array.length != 2) {
                        LogUtil.FATAL_LOG.error("corrupt switch record {}", line);
                        continue;
                    }

                    String key = array[0].trim();
                    String value = array[1].trim();

                    map.put(key, value);
                }
                // 将新解析的配置 map 更新到 switches，并记录当前重新加载的开关内容
                switches = map;
                FATAL_LOG.warn("[reload-switches] {}", getSwitches());
            }
        } catch (IOException e) {
            LogUtil.FATAL_LOG.warn("[reload-switches] error! {}", config);
        }
    }

    // 用于返回当前所有开关配置的字符串表示形式。通过遍历 switches 中的键值对，构建一个以 key=value 形式的字符串，多个键值对之间用 ; 分隔
    public static String getSwitches() {
        // 通过 StringBuilder 将每个开关的 key=value 组合起来，并以 ; 分隔不同的开关配置
        StringBuilder sb = new StringBuilder();

        String split = "";
        for (Map.Entry<String, String> entry : switches.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            sb.append(split);
            sb.append(key);
            sb.append('=');
            sb.append(value);
            split = "; ";
        }

        return sb.toString();
    }
}
