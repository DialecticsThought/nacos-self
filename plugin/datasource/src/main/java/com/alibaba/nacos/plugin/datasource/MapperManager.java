/*
 * Copyright 1999-2022 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.plugin.datasource;

import com.alibaba.nacos.api.exception.runtime.NacosRuntimeException;
import com.alibaba.nacos.common.spi.NacosServiceLoader;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.plugin.datasource.mapper.Mapper;
import com.alibaba.nacos.plugin.datasource.proxy.MapperProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.alibaba.nacos.api.common.Constants.Exception.FIND_DATASOURCE_ERROR_CODE;
import static com.alibaba.nacos.api.common.Constants.Exception.FIND_TABLE_ERROR_CODE;

/**
 * DataSource Plugin Mapper Management.
 *
 * @author hyx
 * <pre>
 * TODO
 * 主要作用是管理 Mapper 对象的实例。
 * 它通过 SPI（Service Provider Interface）机制加载 Mapper 实现类，并根据 dataSource 和 tableName 提供相应的 Mapper 实例
 * </pre>
 **/
public class MapperManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapperManager.class);

    /**
     * 作用: 一个静态的 Map，用于缓存 Mapper 实例。键是 dataSource，值是另一个 Map，其中键为 tableName，值为具体的 Mapper 实例。
     * 解释: MAPPER_SPI_MAP 通过 dataSource 和 tableName 进行映射，便于根据这些信息快速查找对应的 Mapper 实例
     */
    public static final Map<String, Map<String, Mapper>> MAPPER_SPI_MAP = new HashMap<>();
    //MapperManager 类的唯一实例，使用单例模式确保全局唯一性。
    private static final MapperManager INSTANCE = new MapperManager();
    // 标志是否启用了 dataSource 相关的日志记录。此属性在 findMapper 方法中控制是否返回带有日志代理的 Mapper
    private boolean dataSourceLogEnable;

    private MapperManager() {
        loadInitial();
    }

    /**
     * Get the instance of MapperManager.
     *
     * @return The instance of MapperManager.
     */
    public static MapperManager instance(boolean isDataSourceLogEnable) {
        INSTANCE.dataSourceLogEnable = isDataSourceLogEnable;
        return INSTANCE;
    }

    /**
     * The init method.
     */
    public synchronized void loadInitial() {
        // 通过 SPI 机制加载 Mapper 的实现类
        Collection<Mapper> mappers = NacosServiceLoader.load(Mapper.class);
        // 遍历所有加载的 Mapper 实例，调用 putMapper 方法将它们存入 MAPPER_SPI_MAP
        for (Mapper mapper : mappers) {
            putMapper(mapper);
            // 记录每个 Mapper 的加载日志，显示其对应的 dataSource 和 tableName
            LOGGER.info("[MapperManager] Load Mapper({}) datasource({}) tableName({}) successfully.",
                    mapper.getClass(), mapper.getDataSource(), mapper.getTableName());
        }
    }

    /**
     * To join mapper in MAPPER_SPI_MAP.
     *
     * @param mapper The mapper you want join.
     */
    public static synchronized void join(Mapper mapper) {
        if (Objects.isNull(mapper)) {
            return;
        }
        putMapper(mapper);
        LOGGER.info("[MapperManager] join successfully.");
    }

    private static void putMapper(Mapper mapper) {
        //使用 computeIfAbsent 方法：如果 MAPPER_SPI_MAP 中已经存在该 dataSource 的 Map，直接使用；否则创建一个新的 Map
        Map<String, Mapper> mapperMap = MAPPER_SPI_MAP.computeIfAbsent(mapper.getDataSource(), key ->
                new HashMap<>(16));
        // 使用 putIfAbsent 方法：将 mapper 按照其 tableName 放入 dataSource 对应的 Map 中，但如果已经存在相同的 tableName，则不覆盖原有的 Mapper
        mapperMap.putIfAbsent(mapper.getTableName(), mapper);
    }

    /**
     * Get the mapper by table name.
     *
     * @param tableName  table name.
     * @param dataSource the datasource.
     * @return mapper.
     */
    public <R extends Mapper> R findMapper(String dataSource, String tableName) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[MapperManager] findMapper dataSource: {}, tableName: {}", dataSource, tableName);
        }
        if (StringUtils.isBlank(dataSource) || StringUtils.isBlank(tableName)) {
            throw new NacosRuntimeException(FIND_DATASOURCE_ERROR_CODE, "dataSource or tableName is null");
        }
        // 从SPI缓存中获取，这个是在MapperManager构造方法中初始化的
        // 查看META-INF/services/com.alibaba,nacos.plugin.datasource.mapper.Mapper
        Map<String, Mapper> tableMapper = MAPPER_SPI_MAP.get(dataSource);
        if (Objects.isNull(tableMapper)) {
            throw new NacosRuntimeException(FIND_DATASOURCE_ERROR_CODE,
                    "[MapperManager] Failed to find the datasource,dataSource:" + dataSource);
        }
        // 根据 tableName 获取 Mapper 实例。如果未找到 tableName 对应的 Mapper，则抛出异常
        Mapper mapper = tableMapper.get(tableName);
        if (Objects.isNull(mapper)) {
            throw new NacosRuntimeException(FIND_TABLE_ERROR_CODE,
                    "[MapperManager] Failed to find the table ,tableName:" + tableName);
        }
        if (dataSourceLogEnable) {
            return MapperProxy.createSingleProxy(mapper);
        }
        return (R) mapper;
    }
}
