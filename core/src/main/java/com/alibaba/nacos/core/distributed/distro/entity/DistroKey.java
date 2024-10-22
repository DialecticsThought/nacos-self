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

package com.alibaba.nacos.core.distributed.distro.entity;

import java.util.Objects;

/**
 * Distro key.
 *
 * @author xiweng.yy
 *
 * DistroKey distroKey = new DistroKey("client123-session", "SESSION", "192.168.1.10");
 */
public class DistroKey {
    // 保存资源的唯一键
    private String resourceKey;
    // 保存资源的类型
    private String resourceType;
    // 目标服务器，用于表示该分布式键要同步到的服务器
    private String targetServer;

    public DistroKey() {
    }

    public DistroKey(String resourceKey, String resourceType) {
        this.resourceKey = resourceKey;
        this.resourceType = resourceType;
    }

    public DistroKey(String resourceKey, String resourceType, String targetServer) {
        this.resourceKey = resourceKey;
        this.resourceType = resourceType;
        this.targetServer = targetServer;
    }

    public String getResourceKey() {
        return resourceKey;
    }

    public void setResourceKey(String resourceKey) {
        this.resourceKey = resourceKey;
    }

    public String getResourceType() {
        return resourceType;
    }

    public void setResourceType(String resourceType) {
        this.resourceType = resourceType;
    }

    public String getTargetServer() {
        return targetServer;
    }

    public void setTargetServer(String targetServer) {
        this.targetServer = targetServer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        // 如果传入的对象为 null 或者对象类型不同，返回 false
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        // 转换传入的对象为 DistroKey 类型
        DistroKey distroKey = (DistroKey) o;
        // 判断 resourceKey, resourceType, 和 targetServer 是否相等
        return Objects.equals(resourceKey, distroKey.resourceKey) && Objects
                .equals(resourceType, distroKey.resourceType) && Objects.equals(targetServer, distroKey.targetServer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceKey, resourceType, targetServer);
    }

    @Override
    public String toString() {
        return "DistroKey{" + "resourceKey='" + resourceKey + '\'' + ", resourceType='" + resourceType + '\''
                + ", targetServer='" + targetServer + '\'' + '}';
    }
}
