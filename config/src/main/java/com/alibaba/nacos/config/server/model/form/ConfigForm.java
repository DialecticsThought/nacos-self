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

package com.alibaba.nacos.config.server.model.form;

import com.alibaba.nacos.api.exception.api.NacosApiException;
import com.alibaba.nacos.api.model.v2.ErrorCode;
import com.alibaba.nacos.common.utils.StringUtils;
import org.springframework.http.HttpStatus;

import java.io.Serializable;
import java.util.Objects;

/**
 * ConfigForm.
 * <p>
 * ConfigVo{
 * dataId='exampleDataId', group='DEFAULT_GROUP', namespaceId='dev-namespace', content='server.port=8080
 * spring.application.name=myApp', tag='release', appName='myApp', srcUser='admin',
 * configTags='production,critical', desc='This is the configuration for myApp',
 * use='Production', effect='global', type='properties', schema='schema-v1'
 * }
 *
 * @author dongyafei
 * @date 2022/7/24
 */
public class ConfigForm implements Serializable {

    private static final long serialVersionUID = 4124932564086863921L;
    /**
     * 描述: 配置项的唯一标识符，Nacos 中每个配置的核心 ID。
     * 用途: 用于区分不同的配置项，是必须的字段
     */
    private String dataId;
    /**
     * 描述: 配置所属的组，通常用于组织和管理一批相关的配置项。
     * 用途: 同一个 dataId 可以属于不同的 group，用于区分不同的业务场景或环境下的配置
     */
    private String group;
    /**
     * 描述: 配置项所属的命名空间 ID，默认为空字符串（StringUtils.EMPTY）。
     * 用途: 用于隔离不同租户或环境下的配置，提供更细粒度的隔离级别
     */
    private String namespaceId = StringUtils.EMPTY;
    /**
     * 描述: 配置内容，具体的配置数据以字符串形式存储。
     * 用途: 存储实际的配置内容，如 YAML、Properties、JSON 等格式
     */
    private String content;
    /**
     * 描述: 配置的标签，用于进一步分类和标记配置项。
     * 用途: 可以为配置添加自定义标签，便于标识和管理
     */
    private String tag;
    /**
     * 描述: 配置所属的应用名称。
     * 用途: 记录配置是由哪个应用创建或关联，方便配置的归属管理
     */
    private String appName;
    /**
     * 描述: 配置的创建者或最后修改人的用户名。
     * 用途: 用于记录配置的来源或修改记录，方便审计和追踪
     */
    private String srcUser;
    /**
     * 描述: 配置的额外标签，可能有多个标签，以逗号分隔。
     * 用途: 用于为配置项提供额外的分类和检索信息
     */
    private String configTags;
    /**
     * 描述: 配置项的描述信息。
     * 用途: 用于对配置内容进行简单描述，方便理解配置的用途或背景
     */
    private String desc;
    /**
     * 描述: 配置的使用场景或用途。
     * 用途: 记录配置的具体使用情况，例如是用于测试、生产等不同环境
     */
    private String use;
    /**
     * 描述: 配置的生效情况或影响范围。
     * 用途: 说明配置生效的条件或范围，标记配置的影响力
     */
    private String effect;
    /**
     * 描述: 配置的类型，如 text, yaml, json, properties 等。
     * 用途: 用于标识配置的文件格式或类型，决定如何解析配置内容
     */
    private String type;
    /**
     * 描述: 配置的架构信息，通常用来描述配置内容的结构。
     * 用途: 可以为配置提供结构化约束或格式定义，确保配置内容符合预期格式
     */
    private String schema;

    public ConfigForm() {
    }

    public ConfigForm(String dataId, String group, String namespaceId, String content, String tag, String appName,
                      String srcUser, String configTags, String desc, String use, String effect, String type, String schema) {
        this.dataId = dataId;
        this.group = group;
        this.namespaceId = namespaceId;
        this.content = content;
        this.tag = tag;
        this.appName = appName;
        this.srcUser = srcUser;
        this.configTags = configTags;
        this.desc = desc;
        this.use = use;
        this.effect = effect;
        this.type = type;
        this.schema = schema;
    }

    public String getDataId() {
        return dataId;
    }

    public void setDataId(String dataId) {
        this.dataId = dataId;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getNamespaceId() {
        return namespaceId;
    }

    public void setNamespaceId(String namespaceId) {
        this.namespaceId = namespaceId;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getSrcUser() {
        return srcUser;
    }

    public void setSrcUser(String srcUser) {
        this.srcUser = srcUser;
    }

    public String getConfigTags() {
        return configTags;
    }

    public void setConfigTags(String configTags) {
        this.configTags = configTags;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getUse() {
        return use;
    }

    public void setUse(String use) {
        this.use = use;
    }

    public String getEffect() {
        return effect;
    }

    public void setEffect(String effect) {
        this.effect = effect;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConfigForm configForm = (ConfigForm) o;
        return dataId.equals(configForm.dataId) && group.equals(configForm.group) && Objects.equals(namespaceId, configForm.namespaceId)
                && content.equals(configForm.content) && Objects.equals(tag, configForm.tag) && Objects
                .equals(appName, configForm.appName) && Objects.equals(srcUser, configForm.srcUser) && Objects
                .equals(configTags, configForm.configTags) && Objects.equals(desc, configForm.desc) && Objects
                .equals(use, configForm.use) && Objects.equals(effect, configForm.effect) && Objects
                .equals(type, configForm.type) && Objects.equals(schema, configForm.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataId, group, namespaceId, content, tag, appName, srcUser, configTags, desc, use, effect, type,
                schema);
    }

    @Override
    public String toString() {
        return "ConfigVo{" + "dataId='" + dataId + '\'' + ", group='" + group + '\'' + ", namespaceId='" + namespaceId + '\''
                + ", content='" + content + '\'' + ", tag='" + tag + '\'' + ", appName='" + appName + '\''
                + ", srcUser='" + srcUser + '\'' + ", configTags='" + configTags + '\'' + ", desc='" + desc + '\''
                + ", use='" + use + '\'' + ", effect='" + effect + '\'' + ", type='" + type + '\'' + ", schema='"
                + schema + '\'' + '}';
    }

    /**
     * Validate.
     *
     * @throws NacosApiException NacosApiException.
     */
    public void validate() throws NacosApiException {
        if (StringUtils.isBlank(dataId)) {
            throw new NacosApiException(HttpStatus.BAD_REQUEST.value(), ErrorCode.PARAMETER_MISSING,
                    "Required parameter 'dataId' type String is not present");
        } else if (StringUtils.isBlank(group)) {
            throw new NacosApiException(HttpStatus.BAD_REQUEST.value(), ErrorCode.PARAMETER_MISSING,
                    "Required parameter 'group' type String is not present");
        } else if (StringUtils.isBlank(content)) {
            throw new NacosApiException(HttpStatus.BAD_REQUEST.value(), ErrorCode.PARAMETER_MISSING,
                    "Required parameter 'content' type String is not present");
        }
    }
}
