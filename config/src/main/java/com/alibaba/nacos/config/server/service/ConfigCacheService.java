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
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.InternetAddressUtil;
import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.constant.Constants;
import com.alibaba.nacos.config.server.model.CacheItem;
import com.alibaba.nacos.config.server.model.ConfigCache;
import com.alibaba.nacos.config.server.model.event.LocalDataChangeEvent;
import com.alibaba.nacos.config.server.service.dump.disk.ConfigDiskServiceFactory;
import com.alibaba.nacos.config.server.utils.GroupKey2;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.nacos.config.server.constant.Constants.ENCODE_UTF8;
import static com.alibaba.nacos.config.server.constant.Constants.PERSIST_ENCODE;
import static com.alibaba.nacos.config.server.utils.LogUtil.DEFAULT_LOG;
import static com.alibaba.nacos.config.server.utils.LogUtil.DUMP_LOG;
import static com.alibaba.nacos.config.server.utils.LogUtil.FATAL_LOG;

/**
 * Config service.
 *
 * @author Nacos
 *
 * TODO ConfigCacheService 是 Nacos 配置服务中的一个核心类，负责处理配置数据的缓存管理，包括加载、更新、持久化等操作
 */
public class ConfigCacheService {

    private static final String NO_SPACE_CN = "设备上没有空间";

    private static final String NO_SPACE_EN = "No space left on device";

    private static final String DISK_QUOTA_CN = "超出磁盘限额";

    private static final String DISK_QUOTA_EN = "Disk quota exceeded";

    /**
     * groupKey -> cacheItem.
     *
     * 用于存储每个 groupKey 对应的 CacheItem 对象，
     * groupKey 是基于 dataId、group 和 tenant 生成的唯一标识符
     */
    private static final ConcurrentHashMap<String, CacheItem> CACHE = new ConcurrentHashMap<>();

    public static int groupCount() {
        return CACHE.size();
    }

    /**
     * Save config file and update md5 value in cache.
     *
     * @param dataId         dataId string value.
     * @param group          group string value.
     * @param tenant         tenant string value.
     * @param content        content string value.
     * @param md5            md5 of persist.
     * @param lastModifiedTs lastModifiedTs.
     * @param type           file type.
     * @return dumpChange success or not.
     */
    public static boolean dumpWithMd5(String dataId, String group, String tenant, String content, String md5,
            long lastModifiedTs, String type, String encryptedDataKey) {
        // 通过 GroupKey2.getKey 生成唯一的 groupKey，这是一个基于 dataId、group 和 tenant 的标识符
        String groupKey = GroupKey2.getKey(dataId, group, tenant);
        CacheItem ci = makeSure(groupKey, encryptedDataKey);
        ci.setType(type);
        // 获取写锁
        final int lockResult = tryWriteLock(groupKey);
        // 获取锁失败
        if (lockResult < 0) {
            DUMP_LOG.warn("[dump-error] write lock failed. {}", groupKey);
            return false;
        }

        try {

            //check timestamp
            // 校验最后更新时间，如果这个事件滞后了则不处理了
            boolean lastModifiedOutDated = lastModifiedTs < ConfigCacheService.getLastModifiedTs(groupKey);

            // 小于缓存中的最后更新时间，说明滞后了，不处理
            if (lastModifiedOutDated) {
                DUMP_LOG.warn("[dump-ignore] timestamp is outdated,groupKey={}", groupKey);
                return true;
            }

            boolean newLastModified = lastModifiedTs > ConfigCacheService.getLastModifiedTs(groupKey);
            // 计算配置信息的md5值
            if (md5 == null) {
                md5 = MD5Utils.md5Hex(content, PERSIST_ENCODE);
            }

            //check md5 & update local disk cache.
            String localContentMd5 = ConfigCacheService.getContentMd5(groupKey);
            boolean md5Changed = !md5.equals(localContentMd5);

            // 如果配置内容发生变更，需要保存到磁盘
            if (md5Changed) {
                DUMP_LOG.info("[dump] md5 changed, save to disk cache ,groupKey={}, newMd5={},oldMd5={}", groupKey, md5,
                        localContentMd5);
                ConfigDiskServiceFactory.getInstance().saveToDisk(dataId, group, tenant, content);
            } else {
                DUMP_LOG.warn("[dump-ignore] ignore to save to disk cache. md5 consistent,groupKey={}, md5={}",
                        groupKey, md5);
            }

            //check  md5 and timestamp & update local jvm cache.
            if (md5Changed) {
                DUMP_LOG.info(
                        "[dump] md5 changed, update md5 and timestamp in jvm cache ,groupKey={}, newMd5={},oldMd5={},lastModifiedTs={}",
                        groupKey, md5, localContentMd5, lastModifiedTs);

                // 如果配置内容发生变更，需要更新MD5值，更新本地内存中的配置信息，并发布本地配置变更事件
                updateMd5(groupKey, md5, lastModifiedTs, encryptedDataKey);
            } else if (newLastModified) {
                DUMP_LOG.info(
                        "[dump] md5 consistent ,timestamp changed, update timestamp only in jvm cache ,groupKey={},lastModifiedTs={}",
                        groupKey, lastModifiedTs);

                // 设置缓存中配置最后变更时间
                updateTimeStamp(groupKey, lastModifiedTs, encryptedDataKey);
            } else {
                DUMP_LOG.warn(
                        "[dump-ignore] ignore to save to jvm cache. md5 consistent and no new timestamp changed.groupKey={}",
                        groupKey);
            }

            return true;
        } catch (IOException ioe) {
            DUMP_LOG.error("[dump-exception] save disk error. " + groupKey + ", " + ioe);
            if (ioe.getMessage() != null) {
                String errMsg = ioe.getMessage();
                if (NO_SPACE_CN.equals(errMsg) || NO_SPACE_EN.equals(errMsg) || errMsg.contains(DISK_QUOTA_CN)
                        || errMsg.contains(DISK_QUOTA_EN)) {
                    // Protect from disk full.
                    FATAL_LOG.error("Local Disk Full,Exit", ioe);
                    EnvUtil.systemExit();
                }
            }
            return false;
        } finally {

            // 释放写锁
            releaseWriteLock(groupKey);
        }

    }

    /**
     * Save config file and update md5 value in cache.
     *
     * @param dataId           dataId string value.
     * @param group            group string value.
     * @param tenant           tenant string value.
     * @param content          content string value.
     * @param lastModifiedTs   lastModifiedTs.
     * @param type             file type.
     * @param encryptedDataKey encryptedDataKey.
     * @return dumpChange success or not.
     */
    public static boolean dump(String dataId, String group, String tenant, String content, long lastModifiedTs,
            String type, String encryptedDataKey) {
        // TODO 进入
        return dumpWithMd5(dataId, group, tenant, content, null, lastModifiedTs, type, encryptedDataKey);
    }

    /**
     * Save config file and update md5 value in cache.
     *
     * @param dataId         dataId string value.
     * @param group          group string value.
     * @param tenant         tenant string value.
     * @param content        content string value.
     * @param lastModifiedTs lastModifiedTs.
     * @param betaIps        betaIps string value.
     * @return dumpChange success or not.
     */
    public static boolean dumpBeta(String dataId, String group, String tenant, String content, long lastModifiedTs,
            String betaIps, String encryptedDataKey) {
        final String groupKey = GroupKey2.getKey(dataId, group, tenant);

        makeSure(groupKey, null);
        final int lockResult = tryWriteLock(groupKey);

        if (lockResult < 0) {
            DUMP_LOG.warn("[dump-beta-error] write lock failed. {}", groupKey);
            return false;
        }

        try {

            //check timestamp
            boolean timestampOutDated = lastModifiedTs < ConfigCacheService.getBetaLastModifiedTs(groupKey);
            if (timestampOutDated) {
                DUMP_LOG.warn("[dump-beta-ignore] timestamp is outdated,groupKey={}", groupKey);
                return true;
            }

            boolean timestampUpdated = lastModifiedTs > ConfigCacheService.getBetaLastModifiedTs(groupKey);

            String[] betaIpsArr = betaIps.split(",");
            List<String> betaIpList = Lists.newArrayList(betaIpsArr);
            String md5 = MD5Utils.md5Hex(content, ENCODE_UTF8);

            //md5 check & update local disk cache.
            String localContentBetaMd5 = ConfigCacheService.getContentBetaMd5(groupKey);
            boolean md5Changed = !md5.equals(localContentBetaMd5);
            if (md5Changed) {
                DUMP_LOG.info(
                        "[dump-beta] md5 changed, update md5 in local disk cache. groupKey={}, newMd5={}, oldMd5={}",
                        groupKey, md5, localContentBetaMd5);
                ConfigDiskServiceFactory.getInstance().saveBetaToDisk(dataId, group, tenant, content);
            }

            //md5 , ip list  timestamp check  and update local jvm cache.
            boolean ipListChanged = !betaIpList.equals(ConfigCacheService.getBetaIps(groupKey));
            if (md5Changed) {
                DUMP_LOG.info(
                        "[dump-beta] md5 changed, update md5 & ip list & timestamp in jvm cache. groupKey={}, newMd5={}, oldMd5={}，lastModifiedTs={}",
                        groupKey, md5, localContentBetaMd5, lastModifiedTs);
                updateBetaMd5(groupKey, md5, betaIpList, lastModifiedTs, encryptedDataKey);
            } else if (ipListChanged) {
                DUMP_LOG.warn("[dump-beta] ip list changed, update ip list & timestamp in jvm cache. groupKey={},"
                                + " newIpList={}, oldIpList={}，lastModifiedTs={}", groupKey, betaIpList,
                        ConfigCacheService.getBetaIps(groupKey), lastModifiedTs);
                updateBetaIpList(groupKey, betaIpList, lastModifiedTs);
            } else if (timestampUpdated) {
                DUMP_LOG.warn(
                        "[dump-beta] timestamp changed, update timestamp in jvm cache. groupKey={}, newLastModifiedTs={}, oldLastModifiedTs={}",
                        groupKey, lastModifiedTs, ConfigCacheService.getBetaLastModifiedTs(groupKey));
                updateBetaTimeStamp(groupKey, lastModifiedTs);
            } else {
                DUMP_LOG.warn(
                        "[dump-beta-ignore] ignore to save jvm cache, md5 & ip list & timestamp no changed. groupKey={}",
                        groupKey);
            }
            return true;
        } catch (IOException ioe) {
            DUMP_LOG.error("[dump-beta-exception] save disk error. " + groupKey + ", " + ioe.toString(), ioe);
            return false;
        } finally {
            releaseWriteLock(groupKey);
        }
    }

    /**
     * Save config file and update md5 value in cache.
     *
     * @param dataId         dataId string value.
     * @param group          group string value.
     * @param tenant         tenant string value.
     * @param content        content string value.
     * @param lastModifiedTs lastModifiedTs.
     * @param tag            tag string value.
     * @return dumpChange success or not.
     */
    public static boolean dumpTag(String dataId, String group, String tenant, String tag, String content,
            long lastModifiedTs, String encryptedDataKey4Tag) {
        final String groupKey = GroupKey2.getKey(dataId, group, tenant);

        makeSure(groupKey, null);
        final int lockResult = tryWriteLock(groupKey);

        if (lockResult < 0) {
            DUMP_LOG.warn("[dump-tag-error] write lock failed. {}", groupKey);
            return false;
        }

        try {

            //check timestamp
            long localTagLastModifiedTs = ConfigCacheService.getTagLastModifiedTs(groupKey, tag);

            boolean timestampOutdated = lastModifiedTs < localTagLastModifiedTs;
            if (timestampOutdated) {
                DUMP_LOG.warn("[dump-tag-ignore] timestamp is outdated,groupKey={}", groupKey);
                return true;
            }

            boolean timestampChanged = lastModifiedTs > localTagLastModifiedTs;

            final String md5 = MD5Utils.md5Hex(content, ENCODE_UTF8);

            String localContentTagMd5 = ConfigCacheService.getContentTagMd5(groupKey, tag);
            boolean md5Changed = !md5.equals(localContentTagMd5);

            if (md5Changed) {
                ConfigDiskServiceFactory.getInstance().saveTagToDisk(dataId, group, tenant, tag, content);
            }

            if (md5Changed) {
                DUMP_LOG.warn(
                        "[dump-tag] md5 changed, update local jvm cache, groupKey={},tag={}, newMd5={},oldMd5={},lastModifiedTs={}",
                        groupKey, tag, md5, localContentTagMd5, lastModifiedTs);
                updateTagMd5(groupKey, tag, md5, lastModifiedTs, encryptedDataKey4Tag);
            } else if (timestampChanged) {
                DUMP_LOG.warn(
                        "[dump-tag] timestamp changed, update last modified in local jvm cache, groupKey={},tag={},"
                                + "tagLastModifiedTs={},oldTagLastModifiedTs={}", groupKey, tag, lastModifiedTs,
                        localTagLastModifiedTs);
                updateTagTimeStamp(groupKey, tag, lastModifiedTs);

            } else {
                DUMP_LOG.warn("[dump-tag-ignore] md5 & timestamp not changed. groupKey={},tag={}", groupKey, tag);
            }
            return true;
        } catch (IOException ioe) {
            DUMP_LOG.error("[dump-tag-exception] save disk error. " + groupKey + ", " + ioe.toString(), ioe);
            return false;
        } finally {
            releaseWriteLock(groupKey);
        }
    }

    /**
     * Delete config file, and delete cache.
     *
     * @param dataId dataId string value.
     * @param group  group string value.
     * @param tenant tenant string value.
     * @return remove success or not.
     */
    public static boolean remove(String dataId, String group, String tenant) {
        final String groupKey = GroupKey2.getKey(dataId, group, tenant);
        // 获取写锁
        final int lockResult = tryWriteLock(groupKey);

        // If data is non-existent.
        // 如果数据不存在了
        if (0 == lockResult) {
            DUMP_LOG.info("[remove-ok] {} not exist.", groupKey);
            return true;
        }

        // try to lock failed
        // 获取写锁失败了
        if (lockResult < 0) {
            DUMP_LOG.warn("[remove-error] write lock failed. {}", groupKey);
            return false;
        }

        try {
            DUMP_LOG.info("[dump] remove  local disk cache,groupKey={} ", groupKey);
            ConfigDiskServiceFactory.getInstance().removeConfigInfo(dataId, group, tenant);
            // 移除配置缓存
            CACHE.remove(groupKey);
            DUMP_LOG.info("[dump] remove  local jvm cache,groupKey={} ", groupKey);
            // 发布本地配置变动通知
            NotifyCenter.publishEvent(new LocalDataChangeEvent(groupKey));

            return true;
        } finally {
            // 释放写锁
            releaseWriteLock(groupKey);
        }
    }

    /**
     * Delete beta config file, and delete cache.
     * 用于删除 dataId、group 和 tenant 对应的 Beta 配置文件
     * @param dataId dataId string value. 数据 ID
     * @param group  group string value. 配置所属的组
     * @param tenant tenant string value. 租户信息
     * @return remove success or not.
     */
    public static boolean removeBeta(String dataId, String group, String tenant) {
        // 根据 dataId、group 和 tenant 生成唯一的 groupKey，用于标识该配置项
        final String groupKey = GroupKey2.getKey(dataId, group, tenant);
        // 尝试对该 groupKey 获取写锁，确保删除操作的线程安全性
        final int lockResult = tryWriteLock(groupKey);

        // If data is non-existent.
        if (0 == lockResult) { // 如果 lockResult 为 0，表示该 groupKey 对应的数据不存在，直接返回 true，并记录日志
            DUMP_LOG.info("[remove-ok] {} not exist.", groupKey);
            return true;
        }

        // try to lock failed
        if (lockResult < 0) {// 如果 lockResult 小于 0，表示获取写锁失败，记录警告日志并返回 false
            DUMP_LOG.warn("[remove-error] write lock failed. {}", groupKey);
            return false;
        }

        try {
            DUMP_LOG.info("[remove-beta-ok] remove beta in local disk cache,groupKey={} ", groupKey);
            // 从本地磁盘中删除 Beta 配置文件
            ConfigDiskServiceFactory.getInstance().removeConfigInfo4Beta(dataId, group, tenant);
            // 发布本地数据变更事件，通知系统该 groupKey 的 Beta 配置已被移除
            NotifyCenter.publishEvent(new LocalDataChangeEvent(groupKey, true, CACHE.get(groupKey).getIps4Beta()));
            // 从本地 map 缓存中删除 Beta 配置信息
            CACHE.get(groupKey).removeBeta();
            DUMP_LOG.info("[remove-beta-ok] remove beta in local jvm cache,groupKey={} ", groupKey);

            return true;
        } finally {
            releaseWriteLock(groupKey);
        }
    }

    /**
     * Delete tag config file, and delete cache.
     * 于删除 dataId、group、tenant 和 tag 对应的标签配置文件
     * @param dataId dataId string value. 数据 ID
     * @param group  group string value. 配置所属的组
     * @param tenant tenant string value. 租户信息
     * @param tag    tag string value. 标签信息
     * @return remove success or not.
     */
    public static boolean removeTag(String dataId, String group, String tenant, String tag) {
        // 根据 dataId、group 和 tenant 生成唯一的 groupKey，用于标识该配置项
        final String groupKey = GroupKey2.getKey(dataId, group, tenant);
        // 尝试对该 groupKey 获取写锁，确保删除操作的线程安全性
        final int lockResult = tryWriteLock(groupKey);

        // If data is non-existent.
        if (0 == lockResult) {//如果 lockResult 为 0，表示该 groupKey 对应的数据不存在，直接返回 true，并记录日志
            DUMP_LOG.info("[remove-ok] {} not exist.", groupKey);
            return true;
        }

        // try to lock failed
        if (lockResult < 0) {//如果 lockResult 小于 0，表示获取写锁失败，记录警告日志并返回 false
            DUMP_LOG.warn("[remove-error] write lock failed. {}", groupKey);
            return false;
        }

        try {
            DUMP_LOG.info("[remove-tag-ok] remove tag in local disk cache,tag={},groupKey={} ", tag, groupKey);
            //从本地磁盘中删除标签配置文件
            ConfigDiskServiceFactory.getInstance().removeConfigInfo4Tag(dataId, group, tenant, tag);
            // 从本地缓存中获取 groupKey 对应的 CacheItem
            CacheItem ci = CACHE.get(groupKey);
            // 如果该配置项包含标签缓存 (configCacheTags)，则删除该 tag 对应的配置
            if (ci.getConfigCacheTags() != null) {
                ci.getConfigCacheTags().remove(tag);
                // 如果删除后标签缓存为空，清空整个标签缓存
                if (ci.getConfigCacheTags().isEmpty()) {
                    ci.clearConfigTags();
                }
            }

            DUMP_LOG.info("[remove-tag-ok] remove tag in local jvm cache,tag={},groupKey={} ", tag, groupKey);
            // 发布本地数据变更事件，通知系统该 groupKey 和 tag 对应的标签配置已被移除，并返回 true
            NotifyCenter.publishEvent(new LocalDataChangeEvent(groupKey, tag));
            return true;
        } finally {
            releaseWriteLock(groupKey);
        }
    }

    /**
     * Update md5 value.
     * 用于更新 groupKey 对应的普通配置的 MD5 值
     * @param groupKey       groupKey string value. 唯一标识配置项的键
     * @param md5Utf8        md5 string value. UTF-8 编码的 MD5 值
     * @param lastModifiedTs lastModifiedTs long value. 最后修改时间戳
     */
    public static void updateMd5(String groupKey, String md5Utf8, long lastModifiedTs, String encryptedDataKey) {
        // 确保 groupKey 对应的缓存项存在。如果不存在，则创建新的缓存项并将其放入缓存中
        CacheItem cache = makeSure(groupKey, encryptedDataKey);
        // 如果当前的 MD5 值为空，或者与传入的 md5Utf8 不一致，则表示需要更新 MD5 值
        if (cache.getConfigCache().getMd5Utf8() == null || !cache.getConfigCache().getMd5Utf8().equals(md5Utf8)) {
            // 将新的 UTF-8 编码的 MD5 值设置到 configCache 中
            cache.getConfigCache().setMd5Utf8(md5Utf8);
            // 将新的最后修改时间戳设置到 configCache 中
            cache.getConfigCache().setLastModifiedTs(lastModifiedTs);
            // 将新的加密密钥设置到 configCache 中
            cache.getConfigCache().setEncryptedDataKey(encryptedDataKey);
            // 通过 NotifyCenter 发布一个本地数据变更事件，通知系统其他部分该配置已更新
            NotifyCenter.publishEvent(new LocalDataChangeEvent(groupKey));
        }
    }

    /**
     * Update Beta md5 value.
     * 用于更新 groupKey 对应的 Beta 配置的 MD5 值、最后修改时间和加密密钥，并更新 Beta 配置的 IP 列表
     * @param groupKey       groupKey string value. 唯一标识配置项的键
     * @param md5Utf8        md5UTF8 string value. UTF-8 编码的 MD5 值
     * @param ips4Beta       ips4Beta List.  Beta 版本的 IP 列表
     * @param lastModifiedTs lastModifiedTs long value. 最后修改时间戳
     * @param encryptedDataKey4Beta 用于 Beta 配置的加密密钥
     */
    public static void updateBetaMd5(String groupKey, String md5Utf8, List<String> ips4Beta, long lastModifiedTs,
            String encryptedDataKey4Beta) {
        // 确保 groupKey 对应的缓存项存在。如果不存在，则创建新的缓存项。这里不需要传递加密密钥
        CacheItem cache = makeSure(groupKey, null);
        // 如果 configCacheBeta 为空，则初始化 Beta 缓存
        cache.initBetaCacheIfEmpty();
        // 获取 configCacheBeta 中存储的当前 MD5 值
        String betaMd5Utf8 = cache.getConfigCacheBeta().getMd5(ENCODE_UTF8);
        // 如果当前 Beta 配置的 MD5 值为空，或者与传入的 md5Utf8 不一致，或者 IP 列表与缓存中的不相等，则需要更新 Beta 配置
        if (betaMd5Utf8 == null || !betaMd5Utf8.equals(md5Utf8) || !CollectionUtils.isListEqual(ips4Beta,
                cache.ips4Beta)) {
            // 设置 isBeta 为 true，并将传入的 ips4Beta 更新到缓存项中
            cache.isBeta = true;
            cache.ips4Beta = ips4Beta;
            // 更新 Beta 配置的 MD5 值、最后修改时间和加密密钥
            cache.getConfigCacheBeta().setMd5Utf8(md5Utf8);
            cache.getConfigCacheBeta().setLastModifiedTs(lastModifiedTs);
            cache.getConfigCacheBeta().setEncryptedDataKey(encryptedDataKey4Beta);
            // 通过 NotifyCenter 发布一个本地数据变更事件，通知系统其他部分该 groupKey 对应的 Beta 配置已更新
            NotifyCenter.publishEvent(new LocalDataChangeEvent(groupKey, true, ips4Beta));
        }
    }

    /**
     * Update tag md5 value.
     * 该方法用于更新 groupKey 和 tag 对应的标签配置的 MD5 值、最后修改时间和加密密钥
     * @param groupKey       groupKey string value. 唯一标识配置项的键
     * @param tag            tag string value. 配置项的标签
     * @param md5Utf8        md5UTF8 string value. UTF-8 编码的 MD5 值
     * @param lastModifiedTs lastModifiedTs long value. 最后修改时间戳
     * @param encryptedDataKey4Tag 用于标签配置的加密密钥
     */
    public static void updateTagMd5(String groupKey, String tag, String md5Utf8, long lastModifiedTs,
            String encryptedDataKey4Tag) {
        // 确保 groupKey 对应的缓存项存在。如果不存在，则创建新的缓存项
        CacheItem cache = makeSure(groupKey, null);
        // 如果 configCacheTags 为空，初始化标签缓存。确保 tag 对应的 ConfigCache 对象存在
        cache.initConfigTagsIfEmpty(tag);
        //从 configCacheTags 中获取 tag 对应的缓存配置项
        ConfigCache configCache = cache.getConfigCacheTags().get(tag);
        // 更新标签配置的 MD5 值、最后修改时间和加密密钥
        configCache.setMd5Utf8(md5Utf8);
        configCache.setLastModifiedTs(lastModifiedTs);
        configCache.setEncryptedDataKey(encryptedDataKey4Tag);
        // 通过 NotifyCenter 发布一个本地数据变更事件，通知系统其他部分该 groupKey 和 tag 对应的配置已更新
        NotifyCenter.publishEvent(new LocalDataChangeEvent(groupKey, tag));
    }

    /**
     * Get and return content md5 value from cache. Empty string represents no data.
     */
    public static String getContentMd5(String groupKey) {
        return getContentMd5(groupKey, "", "");
    }

    /**
     * 根据 groupKey、ip 和 tag 获取缓存中配置项的 MD5 值
     * @param groupKey
     * @param ip
     * @param tag
     * @return
     */
    public static String getContentMd5(String groupKey, String ip, String tag) {
        // 从缓存中获取 groupKey 对应的 CacheItem 对象。如果该 groupKey 在缓存中存在，则返回对应的 CacheItem，否则返回 null
        CacheItem item = CACHE.get(groupKey);
        // 如果 item 不为 null 且标记为 Beta 配置 (item.isBeta == true) 且 ips4Beta 不为空
        // ，且 ip 在 ips4Beta 列表中，表示此配置是为特定 Beta 版本客户端服务的配置
        if (item != null && item.isBeta && item.ips4Beta != null && item.ips4Beta.contains(ip)
                && item.getConfigCacheBeta() != null) {
            // 返回 Beta 配置的 MD5 值
            return item.getConfigCacheBeta().getMd5(ENCODE_UTF8);
        }
        // 如果 item 不为 null 且 tag 不为空，并且 item.getConfigCacheTags() 存在并包含该 tag，则表示该配置项带有标签
        if (item != null && StringUtils.isNotBlank(tag) && item.getConfigCacheTags() != null
                && item.getConfigCacheTags().containsKey(tag)) {
            // 返回该标签配置的 MD5 值
            return item.getConfigCacheTags().get(tag).getMd5(ENCODE_UTF8);
        }
        // 如果 item 不为 null，并且它被标记为批处理配置 (isBatch == true)，且 delimiter 大于等于客户端 IP 的整数值，
        // 且批处理缓存 (configCacheBatch) 存在，则表示这是一个批处理配置
        if (item != null && item.isBatch && item.delimiter >= InternetAddressUtil.ipToInt(ip)
                && item.getConfigCacheBatch() != null) {
            // 返回批处理配置的 MD5 值
            return item.getConfigCacheBatch().getMd5(ENCODE_UTF8);
        }
        // 如果没有找到匹配的 Beta 配置、标签配置或批处理配置，返回 groupKey 对应的普通配置的 MD5 值。
        // 如果 item 为 null，则返回 Constants.NULL，表示没有找到该配置
        return (null != item) ? item.getConfigCache().getMd5(ENCODE_UTF8) : Constants.NULL;
    }

    /**
     * Get and return beta md5 value from cache. Empty string represents no data.
     *
     * 用于获取指定 groupKey 对应的 Beta 配置的 MD5 值
     */
    public static String getContentBetaMd5(String groupKey) {
        // 从缓存中获取 groupKey 对应的 CacheItem 对象。如果该 groupKey 在缓存中存在，则返回对应的 CacheItem，否则返回 null
        CacheItem item = CACHE.get(groupKey);
        // 如果 item 为 null 或 item.getConfigCacheBeta() 为 null，说明该 groupKey 没有 Beta 配置，返回 Constants.NULL
        if (item == null || item.getConfigCacheBeta() == null) {
            return Constants.NULL;
        }
        // 如果 Beta 配置存在，则返回 configCacheBeta 的 MD5 值
        return item.getConfigCacheBeta().getMd5(ENCODE_UTF8);
    }

    /**
     * Get and return tag md5 value from cache. Empty string represents no data.
     * 用于获取指定 groupKey 和 tag 对应的标签配置的 MD5 值
     * @param groupKey groupKey string value.
     * @param tag      tag string value.
     * @return Content Tag Md5 value.
     */
    public static String getContentTagMd5(String groupKey, String tag) {
        // 从缓存中获取 groupKey 对应的 CacheItem 对象。如果该 groupKey 在缓存中存在，则返回对应的 CacheItem，否则返回 null
        CacheItem item = CACHE.get(groupKey);
        // 如果 item 为 null，或 item.getConfigCacheTags() 为 null，或 configCacheTags 中不包含 tag，
        // 说明该 groupKey 下没有对应的标签配置，返回 Constants.NULL
        if (item == null || item.getConfigCacheTags() == null || !item.getConfigCacheTags().containsKey(tag)) {
            return Constants.NULL;
        }
        // 如果 tag 存在于 configCacheTags 中，则返回该标签配置的 MD5 值
        return item.getConfigCacheTags().get(tag).getMd5(ENCODE_UTF8);
    }

    /**
     * Get and return beta ip list.
     *
     * @param groupKey groupKey string value.
     * @return list beta ips.
     */
    public static List<String> getBetaIps(String groupKey) {
        CacheItem item = CACHE.get(groupKey);
        return (null != item) ? item.getIps4Beta() : Collections.emptyList();
    }

    /**
     * 返回指定 groupKey 对应的 Beta 配置的最后修改时间戳
     * @param groupKey 唯一标识配置项的键，由 dataId、group 和 tenant 组成
     * @return
     */
    public static long getBetaLastModifiedTs(String groupKey) {
        // 从缓存中获取 groupKey 对应的 CacheItem 对象。如果该 groupKey 在缓存中存在，则返回对应的 CacheItem，否则返回 null
        CacheItem item = CACHE.get(groupKey);
        // 如果 item 不为 null 且 item.getConfigCacheBeta() 不为 null，则表示该 groupKey 存在 Beta 配置，返回 Beta 配置的最后修改时间戳。
        // 否则，返回 0L，表示没有找到该 groupKey 对应的 Beta 配置或 Beta 配置的时间戳不存在
        return (null != item && item.getConfigCacheBeta() != null) ? item.getConfigCacheBeta().getLastModifiedTs() : 0L;
    }

    /**
     *
     * @param groupKey  唯一标识配置项的键，由 dataId、group 和 tenant 组成
     * @param tag 用于标识配置项的标签
     * @return
     */
    public static long getTagLastModifiedTs(String groupKey, String tag) {
        // 从缓存中获取 groupKey 对应的 CacheItem 对象。如果该 groupKey 存在于缓存中，则返回对应的 CacheItem，否则返回 null
        CacheItem item = CACHE.get(groupKey);
        // 如果 item.getConfigCacheTags() 为 null，或者 tag 不在 configCacheTags 中，
        // 则表示该 groupKey 下不存在该 tag 对应的标签配置，返回 0
        if (item.getConfigCacheTags() == null || !item.getConfigCacheTags().containsKey(tag)) {
            return 0;
        }
        // ConfigCache：从 configCacheTags 中获取 tag 对应的 ConfigCache 对象，用于存储该标签配置的元数据（如 MD5 和最后修改时间）
        ConfigCache configCacheTag = item.getConfigCacheTags().get(tag);
        // 如果 configCacheTag 不为 null，返回该标签配置的最后修改时间戳。
        // 如果 configCacheTag 为 null，返回 0，表示该 tag 的配置不存在
        return (null != configCacheTag) ? configCacheTag.getLastModifiedTs() : 0;
    }

    /**
     * Get and return content cache.
     *
     * @param groupKey groupKey string value.
     * @return CacheItem.
     */
    public static CacheItem getContentCache(String groupKey) {
        return CACHE.get(groupKey);
    }

    public static long getLastModifiedTs(String groupKey) {
        CacheItem item = CACHE.get(groupKey);
        return (null != item) ? item.getConfigCache().getLastModifiedTs() : 0L;
    }

    /**
     * update tag timestamp.
     * 该方法用于更新指定 groupKey 和 tag 对应的配置标签的最后修改时间
     * @param groupKey       groupKey. 唯一标识配置项的键，由 dataId、group 和 tenant 组成
     * @param tag            tag. 配置项的标签，用于标识带标签的配置
     * @param lastModifiedTs lastModifiedTs. 配置项最后修改时间戳
     */
    public static void updateTagTimeStamp(String groupKey, String tag, long lastModifiedTs) {
        // 确保 groupKey 对应的缓存项存在。如果不存在，则创建一个新的 CacheItem 并将其加入缓存
        CacheItem cache = makeSure(groupKey, null);
        // 初始化标签缓存。如果 configCacheTags 为空，则创建新的 Map，并确保 tag 对应的 ConfigCache 存在
        cache.initConfigTagsIfEmpty(tag);
        // 获取 tag 对应的 ConfigCache 对象，并将其最后修改时间设置为传入的 lastModifiedTs
        cache.getConfigCacheTags().get(tag).setLastModifiedTs(lastModifiedTs);

    }

    /**
     * 该方法用于检查 groupKey 对应的配置内容是否与指定的 MD5 值一致
     * @param groupKey 唯一标识配置项的键
     * @param md5 传入的 MD5 校验值，用于与服务端的 MD5 值进行比较
     * @return
     */
    public static boolean isUptodate(String groupKey, String md5) {
        // 获取 groupKey 对应的配置内容的 MD5 值。如果缓存中有该 groupKey，返回该 groupKey 的 MD5 值
        String serverMd5 = ConfigCacheService.getContentMd5(groupKey);
        // 比较传入的 md5 和从缓存中获取的 serverMd5 是否相等。如果相等，则返回 true，否则返回 false，表示配置内容没有更新
        return StringUtils.equals(md5, serverMd5);
    }

    /**
     * 该方法用于检查带有 groupKey、ip 和 tag 的配置内容是否与传入的 md5 一致
     * @param groupKey  唯一标识配置项的键
     * @param md5  传入的 MD5 校验值
     * @param ip  客户端 IP，用于区分不同客户端的缓存
     * @param tag  配置项的标签
     * @return
     */
    public static boolean isUptodate(String groupKey, String md5, String ip, String tag) {
        // 获取带有 groupKey、ip 和 tag 的配置内容的 MD5 值。如果缓存中有该 groupKey 和 tag，返回该标签配置的 MD5 值
        String serverMd5 = ConfigCacheService.getContentMd5(groupKey, ip, tag);
        // 比较传入的 md5 和从缓存中获取的 serverMd5 是否相等。如果相等，返回 true，表示配置是最新的；否则返回 false
        return StringUtils.equals(md5, serverMd5);
    }

    /**
     * Try to add read lock. If it succeeded, then it can call {@link #releaseWriteLock(String)}.And it won't call if
     * failed.
     *
     * @param groupKey groupKey string value.
     * @return 0 - No data and failed. Positive number - lock succeeded. Negative number - lock failed。
     */
    public static int tryReadLock(String groupKey) {
        CacheItem groupItem = CACHE.get(groupKey);
        int result = (null == groupItem) ? 0 : (groupItem.getRwLock().tryReadLock() ? 1 : -1);
        if (result < 0) {
            DEFAULT_LOG.warn("[read-lock] failed, {}, {}", result, groupKey);
        }
        return result;
    }

    /**
     * Release readLock.
     *
     * @param groupKey groupKey string value.
     */
    public static void releaseReadLock(String groupKey) {
        CacheItem item = CACHE.get(groupKey);
        if (null != item) {
            item.getRwLock().releaseReadLock();
        }
    }

    /**
     * Try to add write lock. If it succeeded, then it can call {@link #releaseWriteLock(String)}.And it won't call if
     * failed.
     *
     * @param groupKey groupKey string value.
     * @return 0 - No data and failed. Positive number 0 - Success. Negative number - lock failed。
     */
    static int tryWriteLock(String groupKey) {
        CacheItem groupItem = CACHE.get(groupKey);
        int result = (null == groupItem) ? 0 : (groupItem.getRwLock().tryWriteLock() ? 1 : -1);
        if (result < 0) {
            DEFAULT_LOG.warn("[write-lock] failed, {}, {}", result, groupKey);
        }
        return result;
    }

    static void releaseWriteLock(String groupKey) {
        CacheItem groupItem = CACHE.get(groupKey);
        if (null != groupItem) {
            groupItem.getRwLock().releaseWriteLock();
        }
    }

    /**
     * 它的主要目的是确保缓存中存在与 groupKey 对应的 CacheItem
     * @param groupKey
     * @param encryptedDataKey
     * @return
     */
    static CacheItem makeSure(final String groupKey, final String encryptedDataKey) {
        // 从 CACHE 中获取与 groupKey 对应的缓存项。如果该 groupKey 对应的缓存项已存在，则返回该缓存项
        CacheItem item = CACHE.get(groupKey);
        // 如果 item 不为 null，说明缓存中已经有与 groupKey 对应的缓存项，直接返回该缓存项
        if (null != item) {
            return item;
        }
        // 如果缓存中没有找到 groupKey 对应的 CacheItem，则创建一个新的 CacheItem 对象
        CacheItem tmp = new CacheItem(groupKey, encryptedDataKey);
        // 将新的 CacheItem (tmp) 放入缓存 CACHE 中，前提是该 groupKey 在缓存中还没有对应的缓存项
        item = CACHE.putIfAbsent(groupKey, tmp);
        return (null == item) ? tmp : item;
    }

    /**
     * update time stamp.
     *
     * @param groupKey         groupKey.
     * @param lastModifiedTs   lastModifiedTs.
     * @param encryptedDataKey encryptedDataKey.
     */
    public static void updateTimeStamp(String groupKey, long lastModifiedTs, String encryptedDataKey) {
        CacheItem cache = makeSure(groupKey, encryptedDataKey);
        cache.getConfigCache().setLastModifiedTs(lastModifiedTs);
    }

    /**
     * update beta ip list.
     * 负责更新指定 groupKey 下的 Beta 配置的 IP 列表（ips4Beta）以及最后修改时间（lastModifiedTs）
     * @param groupKey       groupKey.
     * @param ips4Beta       ips4Beta.
     * @param lastModifiedTs lastModifiedTs.
     */
    private static void updateBetaIpList(String groupKey, List<String> ips4Beta, long lastModifiedTs) {
       // 保 groupKey 对应的缓存项存在。如果不存在则创建一个新的 CacheItem，并将其加入缓存
        CacheItem cache = makeSure(groupKey, null);
        // 初始化 Beta 缓存。如果 configCacheBeta 为空，则创建一个新的 ConfigCache 用于存储 Beta 配置
        cache.initBetaCacheIfEmpty();
        // 将当前 CacheItem 标记为 Beta 版本的配置。isBeta 被设置为 true，表示该缓存项有 Beta 配置
        cache.setBeta(true);
        // 将传入的 ips4Beta（IP 列表）存储到 CacheItem 中，替换之前的 Beta 配置的 IP 列表
        cache.setIps4Beta(ips4Beta);
        // 获取 configCacheBeta 并更新其 lastModifiedTs，即 Beta 配置的最后修改时间
        cache.getConfigCacheBeta().setLastModifiedTs(lastModifiedTs);
        // 发布一个本地数据变更事件，通知系统其他部分该 groupKey 对应的 Beta 配置发生了变化，包括 IP 列表的更新
        NotifyCenter.publishEvent(new LocalDataChangeEvent(groupKey, true, ips4Beta));

    }

    /**
     * update beta lastModifiedTs.
     *
     * @param groupKey       groupKey.
     * @param lastModifiedTs lastModifiedTs.
     */
    private static void updateBetaTimeStamp(String groupKey, long lastModifiedTs) {
        // 确保 groupKey 对应的缓存项存在。如果不存在则创建一个新的 CacheItem，并将其加入缓存
        CacheItem cache = makeSure(groupKey, null);
        // 初始化 Beta 缓存。如果 configCacheBeta 为空，则创建一个新的 ConfigCache 用于存储 Beta 配置
        cache.initBetaCacheIfEmpty();
        // 获取 configCacheBeta 并更新其 lastModifiedTs，即 Beta 配置的最后修改时间
        cache.getConfigCacheBeta().setLastModifiedTs(lastModifiedTs);

    }

    private static final int TRY_GET_LOCK_TIMES = 9;

    /**
     * try config read lock with spin of try get lock times.
     *
     * @param groupKey group key of config.
     * @return
     */
    public static int tryConfigReadLock(String groupKey) {

        // Lock failed by default.
        int lockResult = -1;

        // Try to get lock times, max value: 10;
        for (int i = TRY_GET_LOCK_TIMES; i >= 0; --i) {
            lockResult = ConfigCacheService.tryReadLock(groupKey);

            // The data is non-existent.
            if (0 == lockResult) {
                break;
            }

            // Success
            if (lockResult > 0) {
                break;
            }

            // Retry.
            if (i > 0) {
                try {
                    Thread.sleep(1);
                } catch (Exception e) {
                    LogUtil.PULL_CHECK_LOG.error("An Exception occurred while thread sleep", e);
                }
            }
        }

        return lockResult;
    }
}

