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

package com.alibaba.nacos.config.server.service.dump;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.utils.NetUtils;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.manager.TaskManager;
import com.alibaba.nacos.config.server.model.ConfigInfoChanged;
import com.alibaba.nacos.config.server.model.event.ConfigDataChangeEvent;
import com.alibaba.nacos.config.server.service.dump.disk.ConfigDiskServiceFactory;
import com.alibaba.nacos.config.server.service.dump.processor.DumpAllBetaProcessor;
import com.alibaba.nacos.config.server.service.dump.processor.DumpAllProcessor;
import com.alibaba.nacos.config.server.service.dump.processor.DumpAllTagProcessor;
import com.alibaba.nacos.config.server.service.dump.processor.DumpProcessor;
import com.alibaba.nacos.config.server.service.dump.task.DumpAllBetaTask;
import com.alibaba.nacos.config.server.service.dump.task.DumpAllTagTask;
import com.alibaba.nacos.config.server.service.dump.task.DumpAllTask;
import com.alibaba.nacos.config.server.service.dump.task.DumpTask;
import com.alibaba.nacos.config.server.service.merge.MergeDatumService;
import com.alibaba.nacos.config.server.service.repository.ConfigInfoAggrPersistService;
import com.alibaba.nacos.config.server.service.repository.ConfigInfoBetaPersistService;
import com.alibaba.nacos.config.server.service.repository.ConfigInfoPersistService;
import com.alibaba.nacos.config.server.service.repository.ConfigInfoTagPersistService;
import com.alibaba.nacos.config.server.service.repository.HistoryConfigInfoPersistService;
import com.alibaba.nacos.config.server.utils.ConfigExecutor;
import com.alibaba.nacos.config.server.utils.GroupKey2;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.alibaba.nacos.config.server.utils.PropertyUtil;
import com.alibaba.nacos.config.server.utils.TimeUtils;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.namespace.repository.NamespacePersistService;
import com.alibaba.nacos.persistence.datasource.DynamicDataSource;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.utils.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.config.server.utils.LogUtil.DUMP_LOG;
import static com.alibaba.nacos.config.server.utils.LogUtil.FATAL_LOG;

/**
 * Dump data service.
 *
 * @author Nacos
 * <pre>
 *    Nacos 本地配置的加载无疑肯定是 Nacos Server 启动时候加载的，Nacos 本地配置的加载和
 *    DumpService 有莫大的关系，翻看源码可以看到 DumpService 是一个抽象类，它有两个子类，分别
 *   是 EmbeddedDumpService 和 ExternalDumpService，接下来我们将根据这两个类来展开分析。
 * </pre>
 */
@SuppressWarnings("PMD.AbstractClassShouldStartWithAbstractNamingRule")
public abstract class DumpService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DumpService.class);

    protected DumpProcessor processor;

    protected DumpAllProcessor dumpAllProcessor;

    protected DumpAllBetaProcessor dumpAllBetaProcessor;

    protected DumpAllTagProcessor dumpAllTagProcessor;

    protected ConfigInfoPersistService configInfoPersistService;

    protected NamespacePersistService namespacePersistService;

    protected HistoryConfigInfoPersistService historyConfigInfoPersistService;

    protected ConfigInfoAggrPersistService configInfoAggrPersistService;

    protected ConfigInfoBetaPersistService configInfoBetaPersistService;

    protected ConfigInfoTagPersistService configInfoTagPersistService;

    protected MergeDatumService mergeDatumService;

    protected final ServerMemberManager memberManager;

    /**
     * full dump interval.
     */
    static final int DUMP_ALL_INTERVAL_IN_MINUTE = 6 * 60;

    /**
     * full dump delay.
     */
    static final int INITIAL_DELAY_IN_MINUTE = 6 * 60;

    private TaskManager dumpTaskMgr;

    private TaskManager dumpAllTaskMgr;

    static final int INIT_THREAD_COUNT = 10;

    int total = 0;

    private static final String BETA_TABLE_NAME = "config_info_beta";

    private static final String TAG_TABLE_NAME = "config_info_tag";

    private int retentionDays = 30;

    /**
     * Here you inject the dependent objects constructively, ensuring that some of the dependent functionality is
     * initialized ahead of time.
     *
     * @param memberManager {@link ServerMemberManager}
     */
    public DumpService(ConfigInfoPersistService configInfoPersistService,
            NamespacePersistService namespacePersistService,
            HistoryConfigInfoPersistService historyConfigInfoPersistService,
            ConfigInfoAggrPersistService configInfoAggrPersistService,
            ConfigInfoBetaPersistService configInfoBetaPersistService,
            ConfigInfoTagPersistService configInfoTagPersistService, MergeDatumService mergeDatumService,
            ServerMemberManager memberManager) {
        this.configInfoPersistService = configInfoPersistService;
        this.namespacePersistService = namespacePersistService;
        this.historyConfigInfoPersistService = historyConfigInfoPersistService;
        this.configInfoAggrPersistService = configInfoAggrPersistService;
        this.configInfoBetaPersistService = configInfoBetaPersistService;
        this.configInfoTagPersistService = configInfoTagPersistService;
        this.mergeDatumService = mergeDatumService;
        this.memberManager = memberManager;
        // TODO 查看这个处理类的process方法
        this.processor = new DumpProcessor(this.configInfoPersistService, this.configInfoBetaPersistService,
                this.configInfoTagPersistService);
        this.dumpAllProcessor = new DumpAllProcessor(this.configInfoPersistService);
        this.dumpAllBetaProcessor = new DumpAllBetaProcessor(this.configInfoBetaPersistService);
        this.dumpAllTagProcessor = new DumpAllTagProcessor(this.configInfoTagPersistService);
        // 创建一个TaskManager
        // TODO 查看 TaskManager 他继承NacosDelayTaskExecuteEngine
        this.dumpTaskMgr = new TaskManager("com.alibaba.nacos.server.DumpTaskManager");
        // 设置默认的Processor处理（DumpProcessor）
        this.dumpTaskMgr.setDefaultTaskProcessor(processor);

        this.dumpAllTaskMgr = new TaskManager("com.alibaba.nacos.server.DumpAllTaskManager");
        this.dumpAllTaskMgr.setDefaultTaskProcessor(dumpAllProcessor);

        this.dumpAllTaskMgr.addProcessor(DumpAllTask.TASK_ID, dumpAllProcessor);
        this.dumpAllTaskMgr.addProcessor(DumpAllBetaTask.TASK_ID, dumpAllBetaProcessor);
        this.dumpAllTaskMgr.addProcessor(DumpAllTagTask.TASK_ID, dumpAllTagProcessor);

        DynamicDataSource.getInstance().getDataSource();

        NotifyCenter.registerSubscriber(new Subscriber() {

            @Override
            public void onEvent(Event event) {
                handleConfigDataChange(event);
            }

            @Override
            public Class<? extends Event> subscribeType() {
                return ConfigDataChangeEvent.class;
            }
        });
    }

    void handleConfigDataChange(Event event) {
        // Generate ConfigDataChangeEvent concurrently
        if (event instanceof ConfigDataChangeEvent) {
            ConfigDataChangeEvent evt = (ConfigDataChangeEvent) event;

            DumpRequest dumpRequest = DumpRequest.create(evt.dataId, evt.group, evt.tenant, evt.lastModifiedTs,
                    NetUtils.localIP());
            dumpRequest.setBeta(evt.isBeta);
            dumpRequest.setBatch(evt.isBatch);
            dumpRequest.setTag(evt.tag);
            DumpService.this.dump(dumpRequest);
        }
    }

    /**
     * initialize.
     *
     * @throws Throwable throws Exception when actually operate.
     */
    protected abstract void init() throws Throwable;

    void clearConfigHistory() {
        LOGGER.warn("clearConfigHistory start");
        if (canExecute()) {
            try {
                Timestamp startTime = getBeforeStamp(TimeUtils.getCurrentTime(), 24 * getRetentionDays());
                int pageSize = 1000;
                LOGGER.warn("clearConfigHistory, getBeforeStamp:{}, pageSize:{}", startTime, pageSize);
                historyConfigInfoPersistService.removeConfigHistory(startTime, pageSize);
            } catch (Throwable e) {
                LOGGER.error("clearConfigHistory error : {}", e.toString());
            }
        }

    }

    /**
     * config history clear.
     */
    class ConfigHistoryClear implements Runnable {

        @Override
        public void run() {
            clearConfigHistory();
        }
    }

    /**
     * config history clear.
     */
    class DumpAllProcessorRunner implements Runnable {

        @Override
        public void run() {
            dumpAllTaskMgr.addTask(DumpAllTask.TASK_ID, new DumpAllTask());
        }
    }

    /**
     * dump all beta processor runner.
     */
    class DumpAllBetaProcessorRunner implements Runnable {

        @Override
        public void run() {
            dumpAllTaskMgr.addTask(DumpAllBetaTask.TASK_ID, new DumpAllBetaTask());
        }
    }

    /**
     * dump all tag processor runner.
     */
    class DumpAllTagProcessorRunner implements Runnable {

        @Override
        public void run() {
            dumpAllTaskMgr.addTask(DumpAllTagTask.TASK_ID, new DumpAllTagTask());
        }
    }

    /**
     * dumpOperate 方法是 Nacos 配置服务器在启动时执行的一项操作，
     * 它会将配置信息转储到磁盘，并设置相关的周期性任务，
     * 如清理历史数据、更新 Beta 配置、标签配置等
     * @throws NacosException
     */
    protected void dumpOperate() throws NacosException {
        // 定义了一个转储操作的上下文名称 dumpFileContext
        String dumpFileContext = "CONFIG_DUMP_TO_FILE";
        // 开始记录该转储操作的计时。这用于性能监控或调试
        TimerContext.start(dumpFileContext);
        try {
            LogUtil.DEFAULT_LOG.warn("DumpService start");
            // 获取当前时间，通常用于记录操作的时间戳或历史记录操作
            Timestamp currentTime = new Timestamp(System.currentTimeMillis());

            try {
                // TODO 进入
                dumpAllConfigInfoOnStartup(dumpAllProcessor);

                // update Beta cache
                LogUtil.DEFAULT_LOG.info("start clear all config-info-beta.");
                ConfigDiskServiceFactory.getInstance().clearAllBeta();
                // 清除所有 Beta 配置信息的缓存，确保新的 Beta 配置信息被加载
                // 如果 Beta 表存在（通过 isExistTable 方法检查），则调用 dumpAllBetaProcessor 处理所有 Beta 配置
                if (namespacePersistService.isExistTable(BETA_TABLE_NAME)) {
                    dumpAllBetaProcessor.process(new DumpAllBetaTask());
                }
                // update Tag cache
                LogUtil.DEFAULT_LOG.info("start clear all config-info-tag.");
                // 清除所有标签配置信息的缓存
                ConfigDiskServiceFactory.getInstance().clearAllTag();
                if (namespacePersistService.isExistTable(TAG_TABLE_NAME)) {
                    // 如果标签表存在，则通过 dumpAllTagProcessor 处理所有标签配置
                    dumpAllTagProcessor.process(new DumpAllTagTask());
                }

                // add to dump aggr
                // 获取所有变更的聚合的配置信息并进行转储
                // 每个 ConfigInfoChanged 对象代表一个变更的配置信息。这可能包括多租户、多组别和多数据项的配置信息
                List<ConfigInfoChanged> configList = configInfoAggrPersistService.findAllAggrGroup();
                if (configList != null && !configList.isEmpty()) {
                    // 记录获取到的配置变更信息的总数，用于统计和监控
                    // total 是一个统计变量，可能用于后续日志记录或者处理调度
                    total = configList.size();
                    // mergeDatumService.splitList()：这个方法将原始的配置列表分成多个小列表，便于并行处理
                    // configList：需要拆分的原始配置列表
                    // INIT_THREAD_COUNT：指定的线程数量，通常这个值用于确定如何将列表划分为若干小部分。拆分后的每个子列表将由一个线程处理
                    List<List<ConfigInfoChanged>> splitList = mergeDatumService.splitList(configList,
                            INIT_THREAD_COUNT);
                    // 遍历 splitList 中的每个子列表，并对每个子列表中的配置信息执行合并操作
                    for (List<ConfigInfoChanged> list : splitList) {
                        // 合并处理的核心方法，它将子列表中的配置信息进行合并。合并的具体逻辑可能包括去重、聚合、优化数据存储等操作
                        mergeDatumService.executeConfigsMerge(list);
                    }
                    LOGGER.info("server start, schedule merge end.");
                }
            } catch (Exception e) {
                LogUtil.FATAL_LOG.error(
                        "Nacos Server did not start because dumpservice bean construction failure :\n" + e);
                throw new NacosException(NacosException.SERVER_ERROR,
                        "Nacos Server did not start because dumpservice bean construction failure :\n" + e.getMessage(),
                        e);
            }
            if (!EnvUtil.getStandaloneMode()) {
                // 使用随机数设置转储任务的初始延迟时间，防止所有节点在同一时刻同时开始转储任务
                Random random = new Random();
                long initialDelay = random.nextInt(INITIAL_DELAY_IN_MINUTE) + 10;
                LogUtil.DEFAULT_LOG.warn("initialDelay:{}", initialDelay);
                // 调度三个定时任务，分别用于全量配置、Beta 配置、和标签配置的定时转储。任务会在初始延迟后定期执行
                ConfigExecutor.scheduleConfigTask(new DumpAllProcessorRunner(), initialDelay,
                        DUMP_ALL_INTERVAL_IN_MINUTE, TimeUnit.MINUTES);
                ConfigExecutor.scheduleConfigTask(new DumpAllBetaProcessorRunner(), initialDelay,
                        DUMP_ALL_INTERVAL_IN_MINUTE, TimeUnit.MINUTES);
                ConfigExecutor.scheduleConfigTask(new DumpAllTagProcessorRunner(), initialDelay,
                        DUMP_ALL_INTERVAL_IN_MINUTE, TimeUnit.MINUTES);

                // 调度另一个任务，专门处理配置变更的转储操作，确保新的配置信息被正确转储到磁盘或其他存储中
                ConfigExecutor.scheduleConfigChangeTask(
                        new DumpChangeConfigWorker(this.configInfoPersistService, this.historyConfigInfoPersistService,
                                currentTime), random.nextInt((int) PropertyUtil.getDumpChangeWorkerInterval()),
                        TimeUnit.MILLISECONDS);

            }
            // 每隔 10 分钟调度一次任务，清理历史配置信息，防止存储过多历史版本占用大量空间
            ConfigExecutor.scheduleConfigTask(new ConfigHistoryClear(), 10, 10, TimeUnit.MINUTES);
        } finally {
            TimerContext.end(dumpFileContext, LogUtil.DUMP_LOG);
        }

    }

    private void dumpAllConfigInfoOnStartup(DumpAllProcessor dumpAllProcessor) {

        try {
            LogUtil.DEFAULT_LOG.info("start clear all config-info.");
            // TODO 进入
            ConfigDiskServiceFactory.getInstance().clearAll();
            // 查看任务执行的内容
            // TODO 进入 DumpAllTask
            // TODO 查看 dumpAllProcessor.process
            dumpAllProcessor.process(new DumpAllTask(true));
        } catch (Exception e) {
            LogUtil.FATAL_LOG.error("dump config fail" + e.getMessage());
            throw e;
        }
    }

    private Timestamp getBeforeStamp(Timestamp date, int step) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        // before 6 hour
        cal.add(Calendar.HOUR_OF_DAY, -step);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return Timestamp.valueOf(format.format(cal.getTime()));
    }

    private int getRetentionDays() {
        String val = EnvUtil.getProperty("nacos.config.retention.days");
        if (null == val) {
            return retentionDays;
        }

        int tmp = 0;
        try {
            tmp = Integer.parseInt(val);
            if (tmp > 0) {
                retentionDays = tmp;
            }
        } catch (NumberFormatException nfe) {
            FATAL_LOG.error("read nacos.config.retention.days wrong", nfe);
        }

        return retentionDays;
    }

    /**
     * dump operation.
     *
     * @param dumpRequest dumpRequest.
     */
    public void dump(DumpRequest dumpRequest) {
        if (dumpRequest.isBeta()) {// 处理 Beta 配置的转储，将 Beta 配置同步到需要的节点或存储中
            // 用于灰度发布、测试等场景，允许特定用户或节点使用新的配置，而不影响其他用户
            dumpBeta(dumpRequest.getDataId(), dumpRequest.getGroup(), dumpRequest.getTenant(),
                    dumpRequest.getLastModifiedTs(), dumpRequest.getSourceIp());
        } else if (dumpRequest.isBatch()) {// 处理批量配置的转储，一次性处理多个配置项，提升效率
            // 用于同时更新或发布一组配置，提高运维效率
            dumpBatch(dumpRequest.getDataId(), dumpRequest.getGroup(), dumpRequest.getTenant(),
                    dumpRequest.getLastModifiedTs(), dumpRequest.getSourceIp());
        } else if (StringUtils.isNotBlank(dumpRequest.getTag())) {// 处理带有特定标签的配置的转储，支持按标签分类的配置管理
            // 通过标签对配置进行分类，如按地域、版本、业务线等
            dumpTag(dumpRequest.getDataId(), dumpRequest.getGroup(), dumpRequest.getTenant(), dumpRequest.getTag(),
                    dumpRequest.getLastModifiedTs(), dumpRequest.getSourceIp());
        } else {// 处理正式配置的转储，按照标准流程同步配置
            // 默认的配置类型，适用于大多数生产环境
            dumpFormal(dumpRequest.getDataId(), dumpRequest.getGroup(), dumpRequest.getTenant(),
                    dumpRequest.getLastModifiedTs(), dumpRequest.getSourceIp());
        }
    }

    /**
     * dump formal config.
     *
     * @param dataId       dataId.  配置项的 dataId，用于标识具体的配置信息
     * @param group        group.  配置项所属的组
     * @param tenant       tenant.  租户 ID，用于多租户场景下的配置隔离
     * @param lastModified lastModified.   配置信息的最后修改时间，用于数据同步时判断是否需要更新
     * @param handleIp     handleIp.    处理该任务的 IP 地址
     */
    private void dumpFormal(String dataId, String group, String tenant, long lastModified, String handleIp) {
        // 将 dataId、group 和 tenant 组合生成一个唯一的 groupKey
        String groupKey = GroupKey2.getKey(dataId, group, tenant);
        // 任务的 taskKey 直接设置为 groupKey，意味着该任务是与具体的配置项绑定的，使用 groupKey 作为任务标识
        String taskKey = groupKey;
        // 将DumpTask添加到TaskManager任务管理器，它将异步执行
        dumpTaskMgr.addTask(taskKey, new DumpTask(groupKey, false, false, false, null, lastModified, handleIp));
        DUMP_LOG.info("[dump] add formal task. groupKey={}", groupKey);

    }

    /**
     * dump beta.
     *
     * @param dataId       dataId.
     * @param group        group.
     * @param tenant       tenant.
     * @param lastModified lastModified.
     * @param handleIp     handleIp.
     */
    private void dumpBeta(String dataId, String group, String tenant, long lastModified, String handleIp) {
        // 将 dataId、group 和 tenant 组合生成一个唯一的 groupKey
        String groupKey = GroupKey2.getKey(dataId, group, tenant);
        // 任务的 taskKey 直接设置为 groupKey，意味着该任务是与具体的配置项绑定的，使用 groupKey 作为任务标识 + "+beta"
        String taskKey = groupKey + "+beta";
        // 将DumpTask添加到TaskManager任务管理器，它将异步执行
        dumpTaskMgr.addTask(taskKey, new DumpTask(groupKey, true, false, false, null, lastModified, handleIp));
        DUMP_LOG.info("[dump] add beta task. groupKey={}", groupKey);

    }

    /**
     * dump batch.
     *
     * @param dataId       dataId.
     * @param group        group.
     * @param tenant       tenant.
     * @param lastModified lastModified.
     * @param handleIp     handleIp.
     */
    private void dumpBatch(String dataId, String group, String tenant, long lastModified, String handleIp) {
        // 将 dataId、group 和 tenant 组合生成一个唯一的 groupKey
        String groupKey = GroupKey2.getKey(dataId, group, tenant);
        // 任务的 taskKey 直接设置为 groupKey，意味着该任务是与具体的配置项绑定的，使用 groupKey 作为任务标识 + "+batch"
        String taskKey = groupKey + "+batch";
        // 将DumpTask添加到TaskManager任务管理器，它将异步执行
        dumpTaskMgr.addTask(taskKey, new DumpTask(groupKey, false, true, false, null, lastModified, handleIp));
        DUMP_LOG.info("[dump] add batch task. groupKey={}", dataId + "+" + group);
    }

    /**
     * dump tag.
     *
     * @param dataId       dataId.
     * @param group        group.
     * @param tenant       tenant.
     * @param tag          tag.
     * @param lastModified lastModified.
     * @param handleIp     handleIp.
     */
    private void dumpTag(String dataId, String group, String tenant, String tag, long lastModified, String handleIp) {
        // 将 dataId、group 和 tenant 组合生成一个唯一的 groupKey
        String groupKey = GroupKey2.getKey(dataId, group, tenant);
        // 任务的 taskKey 直接设置为 groupKey，意味着该任务是与具体的配置项绑定的，使用 groupKey 作为任务标识 + "+tag+" + tag
        String taskKey = groupKey + "+tag+" + tag;
        // 将DumpTask添加到TaskManager任务管理器，它将异步执行
        dumpTaskMgr.addTask(taskKey, new DumpTask(groupKey, false, false, true, tag, lastModified, handleIp));
        DUMP_LOG.info("[dump] add tag task. groupKey={},tag={}", groupKey, tag);

    }

    public void dumpAll() {
        dumpAllTaskMgr.addTask(DumpAllTask.TASK_ID, new DumpAllTask());
    }

    /**
     * Used to determine whether the aggregation task, configuration history cleanup task can be performed.
     *
     * @return {@link Boolean}
     */
    protected abstract boolean canExecute();
}
