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

import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.config.server.model.event.ConfigDataChangeEvent;
import com.alibaba.nacos.persistence.configuration.DatasourceConfiguration;
import com.alibaba.nacos.sys.env.EnvUtil;

/**
 * ConfigChangePublisher.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
public class ConfigChangePublisher {

    /**
     * Notify ConfigChange.
     *
     * @param event ConfigDataChangeEvent instance.
     */
    public static void notifyConfigChange(ConfigDataChangeEvent event) {
        // 如果是内部存储并且Nacos非单机模式启动，就不处理了
        if (DatasourceConfiguration.isEmbeddedStorage() && !EnvUtil.getStandaloneMode()) {
            return;
        }
        // TODO 查看 ConfigDataChangeEvent的onEvent方法 就是AsyncNotifyService 类里面
        NotifyCenter.publishEvent(event);
    }

}
