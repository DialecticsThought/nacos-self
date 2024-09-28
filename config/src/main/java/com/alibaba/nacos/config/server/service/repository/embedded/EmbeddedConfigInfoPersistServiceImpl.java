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

package com.alibaba.nacos.config.server.service.repository.embedded;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.exception.runtime.NacosRuntimeException;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nacos.common.utils.Pair;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.constant.Constants;
import com.alibaba.nacos.config.server.enums.FileTypeEnum;
import com.alibaba.nacos.config.server.exception.NacosConfigException;
import com.alibaba.nacos.config.server.model.ConfigAdvanceInfo;
import com.alibaba.nacos.config.server.model.ConfigAllInfo;
import com.alibaba.nacos.config.server.model.ConfigInfo;
import com.alibaba.nacos.config.server.model.ConfigInfoStateWrapper;
import com.alibaba.nacos.config.server.model.ConfigInfoWrapper;
import com.alibaba.nacos.config.server.model.ConfigOperateResult;
import com.alibaba.nacos.config.server.model.SameConfigPolicy;
import com.alibaba.nacos.config.server.service.repository.ConfigInfoPersistService;
import com.alibaba.nacos.config.server.service.repository.HistoryConfigInfoPersistService;
import com.alibaba.nacos.config.server.service.sql.EmbeddedStorageContextUtils;
import com.alibaba.nacos.config.server.utils.ParamUtils;
import com.alibaba.nacos.core.distributed.id.IdGeneratorManager;
import com.alibaba.nacos.persistence.configuration.condition.ConditionOnEmbeddedStorage;
import com.alibaba.nacos.persistence.datasource.DataSourceService;
import com.alibaba.nacos.persistence.datasource.DynamicDataSource;
import com.alibaba.nacos.persistence.model.Page;
import com.alibaba.nacos.persistence.model.event.DerbyImportEvent;
import com.alibaba.nacos.persistence.repository.PaginationHelper;
import com.alibaba.nacos.persistence.repository.embedded.EmbeddedPaginationHelperImpl;
import com.alibaba.nacos.persistence.repository.embedded.EmbeddedStorageContextHolder;
import com.alibaba.nacos.persistence.repository.embedded.operate.DatabaseOperate;
import com.alibaba.nacos.plugin.datasource.MapperManager;
import com.alibaba.nacos.plugin.datasource.constants.CommonConstant;
import com.alibaba.nacos.plugin.datasource.constants.ContextConstant;
import com.alibaba.nacos.plugin.datasource.constants.FieldConstant;
import com.alibaba.nacos.plugin.datasource.constants.TableConstant;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoMapper;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigTagsRelationMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;
import com.alibaba.nacos.plugin.encryption.handler.EncryptionHandler;
import com.alibaba.nacos.sys.env.EnvUtil;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.alibaba.nacos.config.server.service.repository.ConfigRowMapperInjector.CONFIG_ADVANCE_INFO_ROW_MAPPER;
import static com.alibaba.nacos.config.server.service.repository.ConfigRowMapperInjector.CONFIG_ALL_INFO_ROW_MAPPER;
import static com.alibaba.nacos.config.server.service.repository.ConfigRowMapperInjector.CONFIG_INFO_ROW_MAPPER;
import static com.alibaba.nacos.config.server.service.repository.ConfigRowMapperInjector.CONFIG_INFO_STATE_WRAPPER_ROW_MAPPER;
import static com.alibaba.nacos.config.server.service.repository.ConfigRowMapperInjector.CONFIG_INFO_WRAPPER_ROW_MAPPER;
import static com.alibaba.nacos.config.server.utils.LogUtil.DEFAULT_LOG;
import static com.alibaba.nacos.persistence.repository.RowMapperManager.MAP_ROW_MAPPER;

/**
 * EmbeddedConfigInfoPersistServiceImpl.
 *
 * @author lixiaoshuang
 */
@SuppressWarnings({"PMD.MethodReturnWrapperTypeRule", "checkstyle:linelength"})
@Conditional(value = ConditionOnEmbeddedStorage.class)
@Service("embeddedConfigInfoPersistServiceImpl")
public class EmbeddedConfigInfoPersistServiceImpl implements ConfigInfoPersistService {

    private static final String RESOURCE_CONFIG_INFO_ID = "config-info-id";

    private static final String RESOURCE_CONFIG_HISTORY_ID = "config-history-id";

    private static final String RESOURCE_CONFIG_TAG_RELATION_ID = "config-tag-relation-id";

    private static final String RESOURCE_APP_CONFIGDATA_RELATION_SUBS = "app-configdata-relation-subs";

    private static final String RESOURCE_CONFIG_BETA_ID = "config-beta-id";

    private static final String RESOURCE_NAMESPACE_ID = "namespace-id";

    private static final String RESOURCE_USER_ID = "user-id";

    private static final String RESOURCE_ROLE_ID = "role-id";

    private static final String RESOURCE_PERMISSIONS_ID = "permissions_id";

    private static final String DATA_ID = "dataId";

    private static final String GROUP = "group";

    private static final String APP_NAME = "appName";

    private static final String CONTENT = "content";

    private static final String TENANT = "tenant_id";

    public static final String SPOT = ".";

    private DataSourceService dataSourceService;

    private final DatabaseOperate databaseOperate;

    private final IdGeneratorManager idGeneratorManager;

    MapperManager mapperManager;

    private HistoryConfigInfoPersistService historyConfigInfoPersistService;

    /**
     * The constructor sets the dependency injection order.
     *
     * @param databaseOperate    databaseOperate.
     * @param idGeneratorManager {@link IdGeneratorManager}
     */
    public EmbeddedConfigInfoPersistServiceImpl(DatabaseOperate databaseOperate, IdGeneratorManager idGeneratorManager,
                                                @Qualifier("embeddedHistoryConfigInfoPersistServiceImpl") HistoryConfigInfoPersistService historyConfigInfoPersistService) {
        this.databaseOperate = databaseOperate;
        this.idGeneratorManager = idGeneratorManager;
        idGeneratorManager.register(RESOURCE_CONFIG_INFO_ID, RESOURCE_CONFIG_HISTORY_ID,
                RESOURCE_CONFIG_TAG_RELATION_ID, RESOURCE_APP_CONFIGDATA_RELATION_SUBS, RESOURCE_CONFIG_BETA_ID,
                RESOURCE_NAMESPACE_ID, RESOURCE_USER_ID, RESOURCE_ROLE_ID, RESOURCE_PERMISSIONS_ID);
        this.dataSourceService = DynamicDataSource.getInstance().getDataSource();
        Boolean isDataSourceLogEnable = EnvUtil.getProperty(CommonConstant.NACOS_PLUGIN_DATASOURCE_LOG, Boolean.class,
                false);
        this.mapperManager = MapperManager.instance(isDataSourceLogEnable);
        this.historyConfigInfoPersistService = historyConfigInfoPersistService;
        NotifyCenter.registerToSharePublisher(DerbyImportEvent.class);

    }

    @Override
    public <E> PaginationHelper<E> createPaginationHelper() {
        return new EmbeddedPaginationHelperImpl<>(databaseOperate);
    }

    @Override
    public String generateLikeArgument(String s) {
        String fuzzySearchSign = "\\*";
        String sqlLikePercentSign = "%";
        if (s.contains(PATTERN_STR)) {
            return s.replaceAll(fuzzySearchSign, sqlLikePercentSign);
        } else {
            return s;
        }
    }

    @Override
    public ConfigInfoStateWrapper findConfigInfoState(final String dataId, final String group, final String tenant) {
        // 如果 tenant 参数为空或空字符串，
        // 那么将 tenantTmp 设为空字符串（StringUtils.EMPTY）；
        // 否则，将 tenantTmp 设为 tenant 的值。
        // 这一步确保了即使 tenant 参数为空，也能正常执行后续查询
        final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;

        // 通过 mapperManager 根据当前数据源类型（dataSourceService.getDataSourceType()）来查找对应的 ConfigInfoMapper 实例。
        // ConfigInfoMapper 是一个用于生成 SQL 查询语句的映射器。
        // TableConstant.CONFIG_INFO 指定要操作的表为 CONFIG_INFO，该表存储配置信息
        // TODO mapper是通过SPI机制得到的
        ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                TableConstant.CONFIG_INFO);
        // select 方法的第一个参数是一个字段列表，表示我们希望从数据库中查询哪些字段：
        // id、data_id、group_id、tenant_id 和 gmt_modified（最后修改时间）。
        // 第二个参数是查询的条件字段，表示我们需要根据 data_id、group_id 和 tenant_id 来筛选配置信息。
        // 最终生成的 SQL 查询语句可能是类似于：
        // SELECT id, data_id, group_id, tenant_id, gmt_modified
        // FROM CONFIG_INFO
        // WHERE data_id = ? AND group_id = ? AND tenant_id = ?
        // TODO 进入查看
        final String sql = configInfoMapper.select(
                Arrays.asList("id", "data_id", "group_id", "tenant_id", "gmt_modified"),
                Arrays.asList("data_id", "group_id", "tenant_id"));

        /**
         * 使用 databaseOperate 执行 SQL 查询，查找符合条件的一条记录。
         * sql 是生成的查询语句。
         * new Object[] {dataId, group, tenantTmp} 是用于填充 SQL 查询中的占位符的参数列表，将 dataId、group 和 tenantTmp 传入查询。
         * CONFIG_INFO_STATE_WRAPPER_ROW_MAPPER 是一个行映射器，用于将查询结果映射为 ConfigInfoStateWrapper 对象。
         * queryOne 方法会返回查询结果中的第一条记录，并通过映射器转换成 ConfigInfoStateWrapper 对象。如果没有找到结果，可能返回 null
         *
         *
         * TODO
         *   RowMapper 主要用于从数据库查询结果中提取数据并将其转换为 Java 对象。
         *   每一行查询结果对应一个 Java 对象，
         *   RowMapper 通过实现 mapRow 方法来定义如何将数据库中的一行数据映射为 Java 对象
         *   CONFIG_INFO_STATE_WRAPPER_ROW_MAPPER 是  RowMapper的实现
         */
        // TODO 进入
        return databaseOperate.queryOne(sql, new Object[]{dataId, group, tenantTmp},
                CONFIG_INFO_STATE_WRAPPER_ROW_MAPPER);

    }

    private ConfigOperateResult getConfigInfoOperateResult(String dataId, String group, String tenant) {
        ConfigInfoStateWrapper configInfo4 = this.findConfigInfoState(dataId, group, tenant);
        if (configInfo4 == null) {
            return new ConfigOperateResult(false);
        }
        return new ConfigOperateResult(configInfo4.getId(), configInfo4.getLastModified());

    }

    @Override
    public ConfigOperateResult addConfigInfo(final String srcIp, final String srcUser, final ConfigInfo configInfo,
                                             final Map<String, Object> configAdvanceInfo) {
        // TODO 进入
        return addConfigInfo(srcIp, srcUser, configInfo, configAdvanceInfo, null);
    }

    /**
     *
     * @param srcIp 发起请求的源 IP
     * @param srcUser 发起请求的用户
     * @param configInfo  包含配置的具体内容的 ConfigInfo 对象
     * @param configAdvanceInfo   包含高级配置的 Map，例如标签、描述等
     * @param consumer 一个 BiConsumer 接口，负责处理 SQL 执行的异步回调
     * @return
     */
    private ConfigOperateResult addConfigInfo(final String srcIp, final String srcUser, final ConfigInfo configInfo,
                                              final Map<String, Object> configAdvanceInfo, BiConsumer<Boolean, Throwable> consumer) {

        try {
            // 检查 configInfo 中的 tenant 字段是否为空，如果为空则设置为空字符串。这确保 tenant 字段总有值，不会出现 null
            final String tenantTmp =
                    StringUtils.isBlank(configInfo.getTenant()) ? StringUtils.EMPTY : configInfo.getTenant();
            configInfo.setTenant(tenantTmp);
            // 使用 idGeneratorManager 生成唯一的配置 ID 和历史记录 ID。这两个 ID 分别用于新插入的配置信息和其历史记录
            long configId = idGeneratorManager.nextId(RESOURCE_CONFIG_INFO_ID);
            long hisId = idGeneratorManager.nextId(RESOURCE_CONFIG_HISTORY_ID);
            // TODO  进入
            // 将配置信息以原子方式插入数据库，这里封装了 SQL 插入操作
            addConfigInfoAtomic(configId, srcIp, srcUser, configInfo, configAdvanceInfo);
            // 如果 configAdvanceInfo 不为 null，从中获取 config_tags 字段（即标签信息），否则标签为空
            String configTags = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("config_tags");
            // 将标签与配置信息建立关系，这将配置信息和标签存入相关的数据库表中
            // TODO 进入
            addConfigTagsRelation(configId, configTags, configInfo.getDataId(), configInfo.getGroup(),
                    configInfo.getTenant());

            // 获取当前时间戳，并通过 historyConfigInfoPersistService 将配置信息作为历史记录插入到数据库中，标记操作类型为 "I"（插入）
            Timestamp now = new Timestamp(System.currentTimeMillis());
            historyConfigInfoPersistService.insertConfigHistoryAtomic(hisId, configInfo, srcIp, srcUser, now, "I");
            // 触发配置修改的上下文事件，通知系统配置信息已经被修改并需要同步或更新
            EmbeddedStorageContextUtils.onModifyConfigInfo(configInfo, srcIp, now);
            // 使用 databaseOperate.blockUpdate 以异步方式执行数据库操作，consumer 用于处理 SQL 操作的回调函数
            // TODO 进入
            databaseOperate.blockUpdate(consumer);
            // 调用 getConfigInfoOperateResult 方法返回一个 ConfigOperateResult 对象，包含配置信息的操作结果
            return getConfigInfoOperateResult(configInfo.getDataId(), configInfo.getGroup(), tenantTmp);
        } finally {
            // 在 finally 块中清理上下文，确保无论操作成功还是失败，系统都不会保留遗留的上下文数据
            EmbeddedStorageContextHolder.cleanAllContext();
        }
    }

    @Override
    public ConfigOperateResult insertOrUpdate(String srcIp, String srcUser, ConfigInfo configInfo,
                                              Map<String, Object> configAdvanceInfo) {
        if (Objects.isNull(
                findConfigInfoState(configInfo.getDataId(), configInfo.getGroup(), configInfo.getTenant()))) {
            return addConfigInfo(srcIp, srcUser, configInfo, configAdvanceInfo);
        } else {
            return updateConfigInfo(configInfo, srcIp, srcUser, configAdvanceInfo);
        }
    }

    @Override
    public ConfigOperateResult insertOrUpdateCas(String srcIp, String srcUser, ConfigInfo configInfo,
                                                 Map<String, Object> configAdvanceInfo) {
        // 查询配置信息
        if (Objects.isNull(
                // TODO 进入
                findConfigInfoState(configInfo.getDataId(), configInfo.getGroup(), configInfo.getTenant()))) {
            // 找不到 表示新增
            // TODO 进入
            return addConfigInfo(srcIp, srcUser, configInfo, configAdvanceInfo);
        } else {
            //可以找到 更新配置
            return updateConfigInfoCas(configInfo, srcIp, srcUser, configAdvanceInfo);
        }
    }

    /**
     *
     * @param id                id  配置信息的唯一标识
     * @param srcIp             ip  配置变更请求的源 IP
     * @param srcUser           user  配置变更请求的发起用户
     * @param configInfo        info  包含配置信息的数据结构，存储了配置信息的核心内容
     * @param configAdvanceInfo advance info  一个 Map，包含一些高级配置信息（如描述、用途、类型等）
     * @return
     */
    @Override
    public long addConfigInfoAtomic(final long id, final String srcIp, final String srcUser,
                                    final ConfigInfo configInfo, Map<String, Object> configAdvanceInfo) {
        // 如果 configInfo 的 appName 或 tenant 为空或空白，使用默认空字符串处理，确保不会插入 null 值
        final String appNameTmp = StringUtils.defaultEmptyIfBlank(configInfo.getAppName());
        final String tenantTmp = StringUtils.defaultEmptyIfBlank(configInfo.getTenant());
        // 如果 configAdvanceInfo 是 null，高级配置信息（描述、用途、效果、类型、模式）也为 null，否则从 Map 中提取相应值
        final String desc = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("desc");
        final String use = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("use");
        final String effect = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("effect");
        final String type = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("type");
        final String schema = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("schema");
        // 对 configInfo 中的内容进行 MD5 哈希计算，作为数据的完整性校验
        final String md5Tmp = MD5Utils.md5Hex(configInfo.getContent(), Constants.PERSIST_ENCODE);
        // 如果 configInfo 中的 encryptedDataKey 为空，使用默认的空字符串
        final String encryptedDataKey =
                configInfo.getEncryptedDataKey() == null ? StringUtils.EMPTY : configInfo.getEncryptedDataKey();
        // 用 mapperManager 根据数据源类型找到与 CONFIG_INFO 表相关的 ConfigInfoMapper，这是执行 SQL 插入操作的接口
        ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                TableConstant.CONFIG_INFO);
        Timestamp time = new Timestamp(System.currentTimeMillis());
        // 调用 ConfigInfoMapper 的 insert 方法，生成插入操作的 SQL 语句
        final String sql = configInfoMapper.insert(
                Arrays.asList("id", "data_id", "group_id", "tenant_id", "app_name", "content", "md5", "src_ip",
                        "src_user", "gmt_create", "gmt_modified", "c_desc", "c_use", "effect", "type", "c_schema",
                        "encrypted_data_key"));
        // 构造 SQL 插入操作的参数数组，其中包括 ID、数据 ID、组 ID、租户、应用名称、内容、MD5、源 IP、用户、时间戳以及高级配置信息
        final Object[] args = new Object[]{id, configInfo.getDataId(), configInfo.getGroup(), tenantTmp, appNameTmp,
                configInfo.getContent(), md5Tmp, srcIp, srcUser, time, time, desc, use, effect, type, schema,
                encryptedDataKey};
        // TODO 进入
        // 存储要执行的sql和对应的个参数
        EmbeddedStorageContextHolder.addSqlContext(sql, args);
        return id;
    }

    /**
     *
     * @param configId id  配置项的唯一 ID
     * @param tagName  tag 要添加的标签名称
     * @param dataId   data id 配置项的数据 ID
     * @param group    group  配置项的组 ID
     * @param tenant   tenant 租户信息
     */
    @Override
    public void addConfigTagRelationAtomic(long configId, String tagName, String dataId, String group, String tenant) {
        // 通过 mapperManager 根据数据源类型找到与 CONFIG_TAGS_RELATION 表相关的 ConfigTagsRelationMapper。
        // 这个 Mapper 用于执行与配置标签关系相关的 SQL 操作
        // dataSourceService.getDataSourceType() 获取当前数据源类型，
        // TableConstant.CONFIG_TAGS_RELATION 标识标签关系的表名称
        ConfigTagsRelationMapper configTagsRelationMapper = mapperManager.findMapper(
                dataSourceService.getDataSourceType(), TableConstant.CONFIG_TAGS_RELATION);
        // 使用 ConfigTagsRelationMapper 的 insert 方法生成一条插入语句。
        // 插入的字段包括 id（配置 ID）、tag_name（标签名称）、tag_type（标签类型，默认为空）、
        // data_id（数据 ID）、group_id（组 ID）和 tenant_id（租户 ID）
        final String sql = configTagsRelationMapper.insert(
                Arrays.asList("id", "tag_name", "tag_type", "data_id", "group_id", "tenant_id"));
        // 创建一个参数数组，将 SQL 语句中对应的值填入。参数包括：
        //configId: 配置 ID。
        //tagName: 标签名称。
        //StringUtils.EMPTY: 标签类型（默认空字符串）。
        //dataId: 配置项的数据 ID。
        //group: 配置项的组 ID。
        //tenant: 配置项的租户信息
        final Object[] args = new Object[]{configId, tagName, StringUtils.EMPTY, dataId, group, tenant};
        // 把sql和对应的参数 存到上下文
        EmbeddedStorageContextHolder.addSqlContext(sql, args);
    }

    /**
     *
     * @param configId   config id 配置项的唯一标识符
     * @param configTags tags  配置项关联的标签，多个标签之间以逗号分隔
     * @param dataId     dataId 配置项的数据 ID，用于标识具体的数据
     * @param group      group  配置项所属的组 ID
     * @param tenant     tenant  租户 ID，用于多租户环境下的资源隔离
     */
    @Override
    public void addConfigTagsRelation(long configId, String configTags, String dataId, String group, String tenant) {
        // 如果 configTags 为空或仅包含空白字符，则方法不进行任何操作，直接返回
        if (StringUtils.isNotBlank(configTags)) {
            // 使用 split 方法将 configTags 按照逗号（,）分割，得到一个字符串数组 tagArr，其中每个元素代表一个标签
            String[] tagArr = configTags.split(",");
            // 使用 for 循环遍历 tagArr 数组中的每一个标签
            for (int i = 0; i < tagArr.length; i++) {
                // 对于每个标签，调用 addConfigTagRelationAtomic 方法，将当前配置项 (configId) 与该标签 (tagArr[i]) 建立关联
                addConfigTagRelationAtomic(configId, tagArr[i], dataId, group, tenant);
            }
        }
    }

    @Override
    public Map<String, Object> batchInsertOrUpdate(List<ConfigAllInfo> configInfoList, String srcUser, String srcIp,
                                                   Map<String, Object> configAdvanceInfo, SameConfigPolicy policy) throws NacosException {
        int succCount = 0;
        int skipCount = 0;
        List<Map<String, String>> failData = null;
        List<Map<String, String>> skipData = null;

        final BiConsumer<Boolean, Throwable> callFinally = (result, t) -> {
            if (t != null) {
                throw new NacosRuntimeException(0, t);
            }
        };

        for (int i = 0; i < configInfoList.size(); i++) {
            ConfigAllInfo configInfo = configInfoList.get(i);
            try {
                ParamUtils.checkParam(configInfo.getDataId(), configInfo.getGroup(), "datumId",
                        configInfo.getContent());
            } catch (Throwable e) {
                DEFAULT_LOG.error("data verification failed", e);
                throw e;
            }
            ConfigInfo configInfo2Save = new ConfigInfo(configInfo.getDataId(), configInfo.getGroup(),
                    configInfo.getTenant(), configInfo.getAppName(), configInfo.getContent());
            configInfo2Save.setEncryptedDataKey(
                    configInfo.getEncryptedDataKey() == null ? "" : configInfo.getEncryptedDataKey());
            String type = configInfo.getType();
            if (StringUtils.isBlank(type)) {
                // simple judgment of file type based on suffix
                if (configInfo.getDataId().contains(SPOT)) {
                    String extName = configInfo.getDataId().substring(configInfo.getDataId().lastIndexOf(SPOT) + 1);
                    FileTypeEnum fileTypeEnum = FileTypeEnum.getFileTypeEnumByFileExtensionOrFileType(extName);
                    type = fileTypeEnum.getFileType();
                } else {
                    type = FileTypeEnum.getFileTypeEnumByFileExtensionOrFileType(null).getFileType();
                }
            }
            if (configAdvanceInfo == null) {
                configAdvanceInfo = new HashMap<>(16);
            }
            configAdvanceInfo.put("type", type);
            configAdvanceInfo.put("desc", configInfo.getDesc());
            try {
                ConfigInfoStateWrapper foundCfg = findConfigInfoState(configInfo2Save.getDataId(),
                        configInfo2Save.getGroup(), configInfo2Save.getTenant());
                if (foundCfg != null) {
                    throw new Throwable("DuplicateKeyException: config already exists, should be overridden");
                }
                addConfigInfo(srcIp, srcUser, configInfo2Save, configAdvanceInfo, callFinally);
                succCount++;
            } catch (Throwable e) {
                if (!StringUtils.contains(e.toString(), "DuplicateKeyException")) {
                    throw new NacosException(NacosException.SERVER_ERROR, e);
                }
                // uniqueness constraint conflict
                if (SameConfigPolicy.ABORT.equals(policy)) {
                    failData = new ArrayList<>();
                    skipData = new ArrayList<>();
                    Map<String, String> faileditem = new HashMap<>(2);
                    faileditem.put("dataId", configInfo2Save.getDataId());
                    faileditem.put("group", configInfo2Save.getGroup());
                    failData.add(faileditem);
                    for (int j = (i + 1); j < configInfoList.size(); j++) {
                        ConfigInfo skipConfigInfo = configInfoList.get(j);
                        Map<String, String> skipitem = new HashMap<>(2);
                        skipitem.put("dataId", skipConfigInfo.getDataId());
                        skipitem.put("group", skipConfigInfo.getGroup());
                        skipData.add(skipitem);
                        skipCount++;
                    }
                    break;
                } else if (SameConfigPolicy.SKIP.equals(policy)) {
                    skipCount++;
                    if (skipData == null) {
                        skipData = new ArrayList<>();
                    }
                    Map<String, String> skipitem = new HashMap<>(2);
                    skipitem.put("dataId", configInfo2Save.getDataId());
                    skipitem.put("group", configInfo2Save.getGroup());
                    skipData.add(skipitem);
                } else if (SameConfigPolicy.OVERWRITE.equals(policy)) {
                    succCount++;
                    updateConfigInfo(configInfo2Save, srcIp, srcUser, configAdvanceInfo);
                }
            }
        }
        Map<String, Object> result = new HashMap<>(4);
        result.put("succCount", succCount);
        result.put("skipCount", skipCount);
        if (failData != null && !failData.isEmpty()) {
            result.put("failData", failData);
        }
        if (skipData != null && !skipData.isEmpty()) {
            result.put("skipData", skipData);
        }
        return result;
    }

    @Override
    public void removeConfigInfo(final String dataId, final String group, final String tenant, final String srcIp,
                                 final String srcUser) {
        final Timestamp time = new Timestamp(System.currentTimeMillis());
        ConfigInfo configInfo = findConfigInfo(dataId, group, tenant);
        if (Objects.nonNull(configInfo)) {
            try {
                String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;

                removeConfigInfoAtomic(dataId, group, tenantTmp, srcIp, srcUser);
                removeTagByIdAtomic(configInfo.getId());
                historyConfigInfoPersistService.insertConfigHistoryAtomic(configInfo.getId(), configInfo, srcIp,
                        srcUser, time, "D");

                EmbeddedStorageContextUtils.onDeleteConfigInfo(tenantTmp, group, dataId, srcIp, time);

                boolean result = databaseOperate.update(EmbeddedStorageContextHolder.getCurrentSqlContext());
                if (!result) {
                    throw new NacosConfigException("config deletion failed");
                }
            } finally {
                EmbeddedStorageContextHolder.cleanAllContext();
            }
        }
    }

    @Override
    public List<ConfigInfo> removeConfigInfoByIds(final List<Long> ids, final String srcIp, final String srcUser) {
        if (CollectionUtils.isEmpty(ids)) {
            return null;
        }
        ids.removeAll(Collections.singleton(null));
        final Timestamp time = new Timestamp(System.currentTimeMillis());
        try {
            String idsStr = StringUtils.join(ids, StringUtils.COMMA);
            List<ConfigInfo> configInfoList = findConfigInfosByIds(idsStr);
            if (CollectionUtils.isNotEmpty(configInfoList)) {
                removeConfigInfoByIdsAtomic(idsStr);
                for (ConfigInfo configInfo : configInfoList) {
                    removeTagByIdAtomic(configInfo.getId());
                    historyConfigInfoPersistService.insertConfigHistoryAtomic(configInfo.getId(), configInfo, srcIp,
                            srcUser, time, "D");
                }
            }

            EmbeddedStorageContextUtils.onBatchDeleteConfigInfo(configInfoList);
            boolean result = databaseOperate.update(EmbeddedStorageContextHolder.getCurrentSqlContext());
            if (!result) {
                throw new NacosConfigException("Failed to config batch deletion");
            }

            return configInfoList;
        } finally {
            EmbeddedStorageContextHolder.cleanAllContext();
        }
    }

    @Override
    public void removeTagByIdAtomic(long id) {
        ConfigTagsRelationMapper configTagsRelationMapper = mapperManager.findMapper(
                dataSourceService.getDataSourceType(), TableConstant.CONFIG_TAGS_RELATION);
        final String sql = configTagsRelationMapper.delete(Collections.singletonList("id"));
        final Object[] args = new Object[]{id};
        EmbeddedStorageContextHolder.addSqlContext(sql, args);
    }

    @Override
    public void removeConfigInfoAtomic(final String dataId, final String group, final String tenant, final String srcIp,
                                       final String srcUser) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                TableConstant.CONFIG_INFO);
        final String sql = configInfoMapper.delete(Arrays.asList("data_id", "group_id", "tenant_id"));
        final Object[] args = new Object[]{dataId, group, tenantTmp};

        EmbeddedStorageContextHolder.addSqlContext(sql, args);
    }

    @Override
    public void removeConfigInfoByIdsAtomic(final String ids) {
        if (StringUtils.isBlank(ids)) {
            return;
        }
        List<Long> paramList = new ArrayList<>();
        String[] idArr = ids.split(",");
        for (int i = 0; i < idArr.length; i++) {
            paramList.add(Long.parseLong(idArr[i]));
        }
        ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                TableConstant.CONFIG_INFO);
        MapperContext context = new MapperContext();
        context.putWhereParameter(FieldConstant.IDS, paramList);
        MapperResult result = configInfoMapper.removeConfigInfoByIdsAtomic(context);
        EmbeddedStorageContextHolder.addSqlContext(result.getSql(), result.getParamList().toArray());
    }

    @Override
    public ConfigOperateResult updateConfigInfo(final ConfigInfo configInfo, final String srcIp, final String srcUser,
                                                final Map<String, Object> configAdvanceInfo) {
        try {
            // 调用 findConfigInfo 方法，根据 dataId、group 和 tenant 查找当前存储的配置信息。返回值 oldConfigInfo 是数据库中已有的配置
            ConfigInfo oldConfigInfo = findConfigInfo(configInfo.getDataId(), configInfo.getGroup(),
                    configInfo.getTenant());

            //检查新配置信息的 tenant 字段是否为空，如果为空则设置为空字符串。
            final String tenantTmp =
                    StringUtils.isBlank(configInfo.getTenant()) ? StringUtils.EMPTY : configInfo.getTenant();
            // 然后将该租户信息设置到旧的配置信息对象 oldConfigInfo 中
            oldConfigInfo.setTenant(tenantTmp);

            // 检查新配置信息的 appName 是否为 null，如果是，则使用旧配置信息中的 appName。这确保在没有提供新 appName 时，保留数据库中的原值
            String appNameTmp = oldConfigInfo.getAppName();
            // If the appName passed by the user is not empty, the appName of the user is persisted;
            // otherwise, the appName of db is used. Empty string is required to clear appName
            if (configInfo.getAppName() == null) {
                configInfo.setAppName(appNameTmp);
            }
            // 调用 updateConfigInfoAtomic 方法执行配置信息的原子更新操作。该方法将生成并执行更新 SQL
            updateConfigInfoAtomic(configInfo, srcIp, srcUser, configAdvanceInfo);

            String configTags = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("config_tags");
            if (configTags != null) {
                // Delete all tags and recreate them
                // 先调用 removeTagByIdAtomic 方法删除旧的所有标签
                removeTagByIdAtomic(oldConfigInfo.getId());
                // 然后调用 addConfigTagsRelation 方法重新添加新的标签，并与配置关联
                addConfigTagsRelation(oldConfigInfo.getId(), configTags, configInfo.getDataId(), configInfo.getGroup(),
                        configInfo.getTenant());
            }
            // 调用 historyConfigInfoPersistService.insertConfigHistoryAtomic 方法，
            // 将旧的配置信息作为历史记录插入数据库。标记为 "U"（表示更新操作）
            Timestamp time = new Timestamp(System.currentTimeMillis());
            historyConfigInfoPersistService.insertConfigHistoryAtomic(oldConfigInfo.getId(), oldConfigInfo, srcIp,
                    srcUser, time, "U");
            // 调用 EmbeddedStorageContextUtils.onModifyConfigInfo 方法，触发配置信息修改事件，确保相关的服务或组件能同步到新的配置信息
            EmbeddedStorageContextUtils.onModifyConfigInfo(configInfo, srcIp, time);
            // 调用 databaseOperate.blockUpdate 方法执行阻塞更新操作，确保所有的数据库操作（如配置信息更新、标签更新等）在事务内完成
            databaseOperate.blockUpdate();
            // 调用 getConfigInfoOperateResult 方法，生成并返回一个 ConfigOperateResult 对象，表示配置信息更新操作的结果
            return getConfigInfoOperateResult(configInfo.getDataId(), configInfo.getGroup(), tenantTmp);

        } finally {
            // 清理上下文，防止后续操作受到未清理数据的影响
            EmbeddedStorageContextHolder.cleanAllContext();
        }
    }

    @Override
    public ConfigOperateResult updateConfigInfoCas(final ConfigInfo configInfo, final String srcIp,
                                                   final String srcUser, final Map<String, Object> configAdvanceInfo) {
        try {
            ConfigInfo oldConfigInfo = findConfigInfo(configInfo.getDataId(), configInfo.getGroup(),
                    configInfo.getTenant());

            final String tenantTmp =
                    StringUtils.isBlank(configInfo.getTenant()) ? StringUtils.EMPTY : configInfo.getTenant();

            oldConfigInfo.setTenant(tenantTmp);

            String appNameTmp = oldConfigInfo.getAppName();
            // If the appName passed by the user is not empty, the appName of the user is persisted;
            // otherwise, the appName of db is used. Empty string is required to clear appName
            if (configInfo.getAppName() == null) {
                configInfo.setAppName(appNameTmp);
            }

            updateConfigInfoAtomicCas(configInfo, srcIp, srcUser, configAdvanceInfo);

            String configTags = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("config_tags");
            if (configTags != null) {
                // Delete all tags and recreate them
                removeTagByIdAtomic(oldConfigInfo.getId());
                addConfigTagsRelation(oldConfigInfo.getId(), configTags, configInfo.getDataId(), configInfo.getGroup(),
                        configInfo.getTenant());
            }
            Timestamp time = new Timestamp(System.currentTimeMillis());

            historyConfigInfoPersistService.insertConfigHistoryAtomic(oldConfigInfo.getId(), oldConfigInfo, srcIp,
                    srcUser, time, "U");

            EmbeddedStorageContextUtils.onModifyConfigInfo(configInfo, srcIp, time);
            boolean success = databaseOperate.blockUpdate();
            if (success) {
                return getConfigInfoOperateResult(configInfo.getDataId(), configInfo.getGroup(), tenantTmp);
            } else {
                return new ConfigOperateResult(false);
            }
        } finally {
            EmbeddedStorageContextHolder.cleanAllContext();
        }
    }

    private ConfigOperateResult updateConfigInfoAtomicCas(final ConfigInfo configInfo, final String srcIp,
                                                          final String srcUser, Map<String, Object> configAdvanceInfo) {
        final String appNameTmp = StringUtils.defaultEmptyIfBlank(configInfo.getAppName());
        final String tenantTmp = StringUtils.defaultEmptyIfBlank(configInfo.getTenant());
        final String md5Tmp = MD5Utils.md5Hex(configInfo.getContent(), Constants.ENCODE);
        final String desc = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("desc");
        final String use = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("use");
        final String effect = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("effect");
        final String type = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("type");
        final String schema = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("schema");
        final String encryptedDataKey =
                configInfo.getEncryptedDataKey() == null ? StringUtils.EMPTY : configInfo.getEncryptedDataKey();
        ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                TableConstant.CONFIG_INFO);
        Timestamp time = new Timestamp(System.currentTimeMillis());
        MapperContext context = new MapperContext();
        context.putUpdateParameter(FieldConstant.CONTENT, configInfo.getContent());
        context.putUpdateParameter(FieldConstant.MD5, md5Tmp);
        context.putUpdateParameter(FieldConstant.SRC_IP, srcIp);
        context.putUpdateParameter(FieldConstant.SRC_USER, srcUser);
        context.putUpdateParameter(FieldConstant.GMT_MODIFIED, time);
        context.putUpdateParameter(FieldConstant.APP_NAME, appNameTmp);
        context.putUpdateParameter(FieldConstant.C_DESC, desc);
        context.putUpdateParameter(FieldConstant.C_USE, use);
        context.putUpdateParameter(FieldConstant.EFFECT, effect);
        context.putUpdateParameter(FieldConstant.TYPE, type);
        context.putUpdateParameter(FieldConstant.C_SCHEMA, schema);
        context.putUpdateParameter(FieldConstant.ENCRYPTED_DATA_KEY, encryptedDataKey);
        context.putWhereParameter(FieldConstant.DATA_ID, configInfo.getDataId());
        context.putWhereParameter(FieldConstant.GROUP_ID, configInfo.getGroup());
        context.putWhereParameter(FieldConstant.TENANT_ID, tenantTmp);
        context.putWhereParameter(FieldConstant.MD5, configInfo.getMd5());
        MapperResult mapperResult = configInfoMapper.updateConfigInfoAtomicCas(context);

        EmbeddedStorageContextHolder.addSqlContext(Boolean.TRUE, mapperResult.getSql(), mapperResult.getParamList().toArray());
        return getConfigInfoOperateResult(configInfo.getDataId(), configInfo.getGroup(), tenantTmp);

    }

    @Override
    public void updateConfigInfoAtomic(final ConfigInfo configInfo, final String srcIp, final String srcUser,
                                       Map<String, Object> configAdvanceInfo) {
        final String appNameTmp = StringUtils.defaultEmptyIfBlank(configInfo.getAppName());
        final String tenantTmp = StringUtils.defaultEmptyIfBlank(configInfo.getTenant());
        final String md5Tmp = MD5Utils.md5Hex(configInfo.getContent(), Constants.ENCODE);
        final String desc = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("desc");
        final String use = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("use");
        final String effect = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("effect");
        final String type = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("type");
        final String schema = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("schema");
        final String encryptedDataKey =
                configInfo.getEncryptedDataKey() == null ? StringUtils.EMPTY : configInfo.getEncryptedDataKey();

        ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                TableConstant.CONFIG_INFO);
        final String sql = configInfoMapper.update(
                Arrays.asList("content", "md5", "src_ip", "src_user", "gmt_modified", "app_name", "c_desc", "c_use",
                        "effect", "type", "c_schema", "encrypted_data_key"),
                Arrays.asList("data_id", "group_id", "tenant_id"));
        Timestamp time = new Timestamp(System.currentTimeMillis());

        final Object[] args = new Object[]{configInfo.getContent(), md5Tmp, srcIp, srcUser, time, appNameTmp, desc,
                use, effect, type, schema, encryptedDataKey, configInfo.getDataId(), configInfo.getGroup(), tenantTmp};

        EmbeddedStorageContextHolder.addSqlContext(sql, args);
    }

    @Override
    public long findConfigMaxId() {
        ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                TableConstant.CONFIG_INFO);
        MapperResult mapperResult = configInfoMapper.findConfigMaxId(null);
        return Optional.ofNullable(databaseOperate.queryOne(mapperResult.getSql(), Long.class)).orElse(0L);
    }

    @Override
    public ConfigInfo findConfigInfo(long id) {
        ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                TableConstant.CONFIG_INFO);
        final String sql = configInfoMapper.select(
                Arrays.asList("id", "data_id", "group_id", "tenant_id", "app_name", "content"),
                Collections.singletonList("id"));
        return databaseOperate.queryOne(sql, new Object[]{id}, CONFIG_INFO_ROW_MAPPER);
    }

    @Override
    public ConfigInfoWrapper findConfigInfo(final String dataId, final String group, final String tenant) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                TableConstant.CONFIG_INFO);
        final String sql = configInfoMapper.select(
                Arrays.asList("id", "data_id", "group_id", "tenant_id", "app_name", "content", "md5", "type",
                        "encrypted_data_key", "gmt_modified"), Arrays.asList("data_id", "group_id", "tenant_id"));
        final Object[] args = new Object[]{dataId, group, tenantTmp};
        return databaseOperate.queryOne(sql, args, CONFIG_INFO_WRAPPER_ROW_MAPPER);

    }

    @Override
    public Page<ConfigInfo> findConfigInfo4Page(final int pageNo, final int pageSize, final String dataId,
                                                final String group, final String tenant, final Map<String, Object> configAdvanceInfo) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        final String appName = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("appName");
        final String content = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("content");
        final String configTags = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("config_tags");
        MapperResult sql;
        MapperResult sqlCount;

        final MapperContext context = new MapperContext();
        context.putWhereParameter(FieldConstant.TENANT_ID, tenantTmp);
        if (StringUtils.isNotBlank(dataId)) {
            context.putWhereParameter(FieldConstant.DATA_ID, dataId);
        }
        if (StringUtils.isNotBlank(group)) {
            context.putWhereParameter(FieldConstant.GROUP_ID, group);
        }
        if (StringUtils.isNotBlank(appName)) {
            context.putWhereParameter(FieldConstant.APP_NAME, appName);
        }
        if (!StringUtils.isBlank(content)) {
            context.putWhereParameter(FieldConstant.CONTENT, content);
        }
        context.setStartRow((pageNo - 1) * pageSize);
        context.setPageSize(pageSize);

        if (StringUtils.isNotBlank(configTags)) {
            String[] tagArr = configTags.split(",");
            context.putWhereParameter(FieldConstant.TAG_ARR, tagArr);
            ConfigTagsRelationMapper configTagsRelationMapper = mapperManager.findMapper(
                    dataSourceService.getDataSourceType(), TableConstant.CONFIG_TAGS_RELATION);
            sqlCount = configTagsRelationMapper.findConfigInfo4PageCountRows(context);
            sql = configTagsRelationMapper.findConfigInfo4PageFetchRows(context);
        } else {
            ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                    TableConstant.CONFIG_INFO);

            sqlCount = configInfoMapper.findConfigInfo4PageCountRows(context);
            sql = configInfoMapper.findConfigInfo4PageFetchRows(context);
        }
        PaginationHelper<ConfigInfo> helper = createPaginationHelper();
        Page<ConfigInfo> page = helper.fetchPageLimit(sqlCount, sql, pageNo, pageSize, CONFIG_INFO_ROW_MAPPER);

        for (ConfigInfo configInfo : page.getPageItems()) {
            Pair<String, String> pair = EncryptionHandler.decryptHandler(configInfo.getDataId(),
                    configInfo.getEncryptedDataKey(), configInfo.getContent());
            configInfo.setContent(pair.getSecond());
        }

        return page;
    }

    @Override
    public int configInfoCount() {
        ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                TableConstant.CONFIG_INFO);
        String sql = configInfoMapper.count(null);
        Integer result = databaseOperate.queryOne(sql, Integer.class);
        if (result == null) {
            throw new IllegalArgumentException("configInfoCount error");
        }
        return result;
    }

    @Override
    public int configInfoCount(String tenant) {
        ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                TableConstant.CONFIG_INFO);
        MapperContext context = new MapperContext();
        context.putWhereParameter(FieldConstant.TENANT_ID, tenant);
        MapperResult mapperResult = configInfoMapper.configInfoLikeTenantCount(context);
        Integer result = databaseOperate.queryOne(mapperResult.getSql(), mapperResult.getParamList().toArray(),
                Integer.class);
        if (result == null) {
            throw new IllegalArgumentException("configInfoCount error");
        }
        return result;
    }

    @Override
    public List<String> getTenantIdList(int page, int pageSize) {
        PaginationHelper<Map<String, Object>> helper = createPaginationHelper();

        ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                TableConstant.CONFIG_INFO);
        int from = (page - 1) * pageSize;
        MapperResult mapperResult = configInfoMapper.getTenantIdList(new MapperContext(from, pageSize));

        Page<Map<String, Object>> pageList = helper.fetchPageLimit(mapperResult.getSql(),
                mapperResult.getParamList().toArray(), page, pageSize, MAP_ROW_MAPPER);
        return pageList.getPageItems().stream().map(map -> String.valueOf(map.get("TENANT_ID")))
                .collect(Collectors.toList());
    }

    @Override
    public List<String> getGroupIdList(int page, int pageSize) {
        PaginationHelper<Map<String, Object>> helper = createPaginationHelper();

        ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                TableConstant.CONFIG_INFO);
        int from = (page - 1) * pageSize;
        MapperResult mapperResult = configInfoMapper.getGroupIdList(new MapperContext(from, pageSize));

        Page<Map<String, Object>> pageList = helper.fetchPageLimit(mapperResult.getSql(),
                mapperResult.getParamList().toArray(), page, pageSize, MAP_ROW_MAPPER);
        return pageList.getPageItems().stream().map(map -> String.valueOf(map.get("GROUP_ID")))
                .collect(Collectors.toList());
    }

    @Override
    public Page<ConfigInfoWrapper> findAllConfigInfoFragment(final long lastMaxId, final int pageSize,
                                                             boolean needContent) {
        ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                TableConstant.CONFIG_INFO);
        MapperContext context = new MapperContext(0, pageSize);
        context.putContextParameter(ContextConstant.NEED_CONTENT, String.valueOf(needContent));
        context.putWhereParameter(FieldConstant.ID, lastMaxId);
        MapperResult select = configInfoMapper.findAllConfigInfoFragment(context);
        PaginationHelper<ConfigInfoWrapper> helper = createPaginationHelper();
        return helper.fetchPageLimit(select.getSql(), select.getParamList().toArray(), 1, pageSize,
                CONFIG_INFO_WRAPPER_ROW_MAPPER);

    }

    @Override
    public Page<ConfigInfo> findConfigInfoLike4Page(final int pageNo, final int pageSize, final String dataId,
                                                    final String group, final String tenant, final Map<String, Object> configAdvanceInfo) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        final String appName = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("appName");
        final String content = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("content");
        final String configTags = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("config_tags");
        MapperResult sqlCountRows;
        MapperResult sqlFetchRows;

        MapperContext context = new MapperContext((pageNo - 1) * pageSize, pageSize);
        context.putWhereParameter(FieldConstant.TENANT_ID, generateLikeArgument(tenantTmp));

        if (!StringUtils.isBlank(dataId)) {
            context.putWhereParameter(FieldConstant.DATA_ID, generateLikeArgument(dataId));
        }
        if (!StringUtils.isBlank(group)) {
            context.putWhereParameter(FieldConstant.GROUP_ID, generateLikeArgument(group));
        }
        if (!StringUtils.isBlank(appName)) {
            context.putWhereParameter(FieldConstant.APP_NAME, appName);
        }
        if (!StringUtils.isBlank(content)) {
            context.putWhereParameter(FieldConstant.CONTENT, generateLikeArgument(content));
        }

        if (StringUtils.isNotBlank(configTags)) {
            String[] tagArr = configTags.split(",");
            context.putWhereParameter(FieldConstant.TAG_ARR, tagArr);
            ConfigTagsRelationMapper configTagsRelationMapper = mapperManager.findMapper(
                    dataSourceService.getDataSourceType(), TableConstant.CONFIG_TAGS_RELATION);
            sqlCountRows = configTagsRelationMapper.findConfigInfoLike4PageCountRows(context);
            sqlFetchRows = configTagsRelationMapper.findConfigInfoLike4PageFetchRows(context);
        } else {
            ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                    TableConstant.CONFIG_INFO);
            sqlCountRows = configInfoMapper.findConfigInfoLike4PageCountRows(context);
            sqlFetchRows = configInfoMapper.findConfigInfoLike4PageFetchRows(context);
        }
        PaginationHelper<ConfigInfo> helper = createPaginationHelper();
        Page<ConfigInfo> page = helper.fetchPageLimit(sqlCountRows, sqlFetchRows, pageNo, pageSize,
                CONFIG_INFO_ROW_MAPPER);
        for (ConfigInfo configInfo : page.getPageItems()) {
            Pair<String, String> pair = EncryptionHandler.decryptHandler(configInfo.getDataId(),
                    configInfo.getEncryptedDataKey(), configInfo.getContent());
            configInfo.setContent(pair.getSecond());
        }
        return page;

    }

    @Override
    public List<ConfigInfoStateWrapper> findChangeConfig(final Timestamp startTime, long lastMaxId,
                                                         final int pageSize) {
        ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                TableConstant.CONFIG_INFO);

        MapperContext context = new MapperContext();
        context.putWhereParameter(FieldConstant.START_TIME, startTime);
        context.putWhereParameter(FieldConstant.PAGE_SIZE, pageSize);
        context.putWhereParameter(FieldConstant.LAST_MAX_ID, lastMaxId);

        MapperResult mapperResult = configInfoMapper.findChangeConfig(context);
        return databaseOperate.queryMany(mapperResult.getSql(), mapperResult.getParamList().toArray(),
                CONFIG_INFO_STATE_WRAPPER_ROW_MAPPER);

    }

    @Override
    public List<String> selectTagByConfig(String dataId, String group, String tenant) {
        ConfigTagsRelationMapper configTagsRelationMapper = mapperManager.findMapper(
                dataSourceService.getDataSourceType(), TableConstant.CONFIG_TAGS_RELATION);
        String sql = configTagsRelationMapper.select(Collections.singletonList("tag_name"),
                Arrays.asList("data_id", "group_id", "tenant_id"));
        return databaseOperate.queryMany(sql, new Object[]{dataId, group, tenant}, String.class);
    }

    @Override
    public List<ConfigInfo> findConfigInfosByIds(final String ids) {
        if (StringUtils.isBlank(ids)) {
            return null;
        }
        List<Long> paramList = new ArrayList<>();
        String[] idArr = ids.split(",");
        for (int i = 0; i < idArr.length; i++) {
            paramList.add(Long.parseLong(idArr[i]));
        }
        ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                TableConstant.CONFIG_INFO);
        MapperContext context = new MapperContext();
        context.putWhereParameter(FieldConstant.IDS, paramList);
        MapperResult mapperResult = configInfoMapper.findConfigInfosByIds(context);
        return databaseOperate.queryMany(mapperResult.getSql(), mapperResult.getParamList().toArray(),
                CONFIG_INFO_ROW_MAPPER);

    }

    @Override
    public ConfigAdvanceInfo findConfigAdvanceInfo(final String dataId, final String group, final String tenant) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        List<String> configTagList = this.selectTagByConfig(dataId, group, tenant);

        ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                TableConstant.CONFIG_INFO);
        ConfigAdvanceInfo configAdvance = databaseOperate.queryOne(configInfoMapper.select(
                        Arrays.asList("gmt_create", "gmt_modified", "src_user", "src_ip", "c_desc", "c_use", "effect", "type",
                                "c_schema"), Arrays.asList("data_id", "group_id", "tenant_id")),
                new Object[]{dataId, group, tenantTmp}, CONFIG_ADVANCE_INFO_ROW_MAPPER);

        if (CollectionUtils.isNotEmpty(configTagList)) {
            StringBuilder configTagsTmp = new StringBuilder();
            for (String configTag : configTagList) {
                if (configTagsTmp.length() == 0) {
                    configTagsTmp.append(configTag);
                } else {
                    configTagsTmp.append(',').append(configTag);
                }
            }
            configAdvance.setConfigTags(configTagsTmp.toString());
        }
        return configAdvance;
    }

    @Override
    public ConfigAllInfo findConfigAllInfo(final String dataId, final String group, final String tenant) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;

        ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                TableConstant.CONFIG_INFO);
        final String sql = configInfoMapper.select(
                Arrays.asList("id", "data_id", "group_id", "tenant_id", "app_name", "content", "md5", "gmt_create",
                        "gmt_modified", "src_user", "src_ip", "c_desc", "c_use", "effect", "type", "c_schema",
                        "encrypted_data_key"), Arrays.asList("data_id", "group_id", "tenant_id"));

        List<String> configTagList = selectTagByConfig(dataId, group, tenant);

        ConfigAllInfo configAdvance = databaseOperate.queryOne(sql, new Object[]{dataId, group, tenantTmp},
                CONFIG_ALL_INFO_ROW_MAPPER);

        if (configTagList != null && !configTagList.isEmpty()) {
            StringBuilder configTagsTmp = new StringBuilder();
            for (String configTag : configTagList) {
                if (configTagsTmp.length() == 0) {
                    configTagsTmp.append(configTag);
                } else {
                    configTagsTmp.append(',').append(configTag);
                }
            }
            configAdvance.setConfigTags(configTagsTmp.toString());
        }
        return configAdvance;
    }

    @Override
    public List<ConfigAllInfo> findAllConfigInfo4Export(final String dataId, final String group, final String tenant,
                                                        final String appName, final List<Long> ids) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                TableConstant.CONFIG_INFO);
        MapperContext context = new MapperContext();
        if (!CollectionUtils.isEmpty(ids)) {
            context.putWhereParameter(FieldConstant.IDS, ids);
        } else {
            context.putWhereParameter(FieldConstant.TENANT_ID, tenantTmp);
            if (!StringUtils.isBlank(dataId)) {
                context.putWhereParameter(FieldConstant.DATA_ID, generateLikeArgument(dataId));
            }
            if (StringUtils.isNotBlank(group)) {
                context.putWhereParameter(FieldConstant.GROUP_ID, group);
            }
            if (StringUtils.isNotBlank(appName)) {
                context.putWhereParameter(FieldConstant.APP_NAME, appName);
            }
        }

        MapperResult mapperResult = configInfoMapper.findAllConfigInfo4Export(context);
        return databaseOperate.queryMany(mapperResult.getSql(), mapperResult.getParamList().toArray(),
                CONFIG_ALL_INFO_ROW_MAPPER);
    }

    @Override
    public List<ConfigInfoWrapper> queryConfigInfoByNamespace(String tenantId) {
        if (Objects.isNull(tenantId)) {
            throw new IllegalArgumentException("tenantId can not be null");
        }
        String tenantTmp = StringUtils.isBlank(tenantId) ? StringUtils.EMPTY : tenantId;
        ConfigInfoMapper configInfoMapper = mapperManager.findMapper(dataSourceService.getDataSourceType(),
                TableConstant.CONFIG_INFO);
        final String sql = configInfoMapper.select(
                Arrays.asList("data_id", "group_id", "tenant_id", "app_name", "type", "gmt_modified"),
                Collections.singletonList("tenant_id"));
        return databaseOperate.queryMany(sql, new Object[]{tenantTmp}, CONFIG_INFO_WRAPPER_ROW_MAPPER);
    }

}
