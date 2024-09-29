/*
 * Copyright 1999-2023 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.core.persistence;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.exception.runtime.NacosRuntimeException;
import com.alibaba.nacos.common.JustForTest;
import com.alibaba.nacos.common.model.RestResult;
import com.alibaba.nacos.common.model.RestResultUtils;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.utils.ExceptionUtil;
import com.alibaba.nacos.common.utils.LoggerUtils;
import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nacos.common.utils.Preconditions;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.consistency.SerializeFactory;
import com.alibaba.nacos.consistency.Serializer;
import com.alibaba.nacos.consistency.cp.CPProtocol;
import com.alibaba.nacos.consistency.cp.RequestProcessor4CP;
import com.alibaba.nacos.consistency.entity.ReadRequest;
import com.alibaba.nacos.consistency.entity.Response;
import com.alibaba.nacos.consistency.entity.WriteRequest;
import com.alibaba.nacos.consistency.exception.ConsistencyException;
import com.alibaba.nacos.consistency.snapshot.SnapshotOperation;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.distributed.ProtocolManager;
import com.alibaba.nacos.core.utils.ClassUtils;
import com.alibaba.nacos.persistence.configuration.condition.ConditionDistributedEmbedStorage;
import com.alibaba.nacos.persistence.constants.PersistenceConstant;
import com.alibaba.nacos.persistence.datasource.DynamicDataSource;
import com.alibaba.nacos.persistence.datasource.LocalDataSourceServiceImpl;
import com.alibaba.nacos.persistence.exception.NJdbcException;
import com.alibaba.nacos.persistence.model.event.DerbyLoadEvent;
import com.alibaba.nacos.persistence.model.event.RaftDbErrorEvent;
import com.alibaba.nacos.persistence.repository.RowMapperManager;
import com.alibaba.nacos.persistence.repository.embedded.EmbeddedStorageContextHolder;
import com.alibaba.nacos.persistence.repository.embedded.hook.EmbeddedApplyHook;
import com.alibaba.nacos.persistence.repository.embedded.hook.EmbeddedApplyHookHolder;
import com.alibaba.nacos.persistence.repository.embedded.operate.BaseDatabaseOperate;
import com.alibaba.nacos.persistence.repository.embedded.sql.ModifyRequest;
import com.alibaba.nacos.persistence.repository.embedded.sql.QueryType;
import com.alibaba.nacos.persistence.repository.embedded.sql.SelectRequest;
import com.alibaba.nacos.persistence.utils.PersistenceExecutor;
import com.alibaba.nacos.sys.utils.DiskUtils;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Conditional;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Distributed Database Operate.
 *
 * <pre>
 *                   ┌────────────────────┐
 *               ┌──▶│   PersistService   │
 *               │   └────────────────────┘ ┌─────────────────┐
 *               │              │           │                 │
 *               │              │           │                 ▼
 *               │              │           │    ┌────────────────────────┐
 *               │              │           │    │  acquireSnowFlowerId   │
 *               │              │           │    └────────────────────────┘
 *               │              │           │                 │
 *               │              │           │                 │
 *               │              │           │                 ▼
 *               │              │           │      ┌────────────────────┐          save sql
 *               │              ▼           │      │     saveConfig     │──────────context─────────────┐
 *               │     ┌────────────────┐   │      └────────────────────┘                              │
 *               │     │ publishConfig  │───┘                 │                                        │
 *               │     └────────────────┘                     │                                        │
 *               │                                            ▼                                        ▼
 *               │                               ┌─────────────────────────┐    save sql    ┌────────────────────┐
 *               │                               │ saveConfigTagRelations  │────context────▶│  SqlContextUtils   │◀─┐
 *        publish config                         └─────────────────────────┘                └────────────────────┘  │
 *               │                                            │                                        ▲            │
 *               │                                            │                                        │            │
 *               │                                            ▼                                        │            │
 *               │                                ┌───────────────────────┐         save sql           │            │
 *            ┌────┐                              │   saveConfigHistory   │─────────context────────────┘            │
 *            │user│                              └───────────────────────┘                                         │
 *            └────┘                                                                                                │
 *               ▲                                                                                                  │
 *               │                                           ┌1:getCurrentSqlContexts───────────────────────────────┘
 *               │                                           │
 *               │                                           │
 *               │                                           │
 *               │           ┌───────────────┐    ┌─────────────────────┐
 *               │           │ JdbcTemplate  │◀───│   DatabaseOperate   │───┐
 *       4:execute result    └───────────────┘    └─────────────────────┘   │
 *               │                   │                       ▲              │
 *               │                   │                       │              │
 *               │                   │                  3:onApply         2:submit(List&lt;ModifyRequest&gt;)
 *               │                   │                       │              │
 *               │                   ▼                       │              │
 *               │           ┌──────────────┐                │              │
 *               │           │ Apache Derby │    ┌───────────────────────┐  │
 *               │           └──────────────┘    │     JRaftProtocol     │◀─┘
 *               │                               └───────────────────────┘
 *               │                                           │
 *               │                                           │
 *               └───────────────────────────────────────────┘
 * </pre>
 *
 * <p>TODO depend on Member and Cp protocol strongly, Waiting for addition split.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
@Conditional(ConditionDistributedEmbedStorage.class)
@Component
@SuppressWarnings({"unchecked"})
public class DistributedDatabaseOperateImpl extends RequestProcessor4CP implements BaseDatabaseOperate {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedDatabaseOperateImpl.class);

    /**
     * The data import operation is dedicated key, which ACTS as an identifier.
     */
    private static final String DATA_IMPORT_KEY = "00--0-data_import-0--00";

    private final ServerMemberManager memberManager;

    private CPProtocol protocol;

    private LocalDataSourceServiceImpl dataSourceService;

    private JdbcTemplate jdbcTemplate;

    private TransactionTemplate transactionTemplate;

    private final Serializer serializer = SerializeFactory.getDefault();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    public DistributedDatabaseOperateImpl(ServerMemberManager memberManager, ProtocolManager protocolManager)
            throws Exception {
        this.memberManager = memberManager;
        this.protocol = protocolManager.getCpProtocol();
        init();
    }

    protected void init() throws Exception {

        this.dataSourceService = (LocalDataSourceServiceImpl) DynamicDataSource.getInstance().getDataSource();

        // Because in Raft + Derby mode, ensuring data consistency depends on the Raft's
        // log playback and snapshot recovery capabilities, and the last data must be cleared
        this.dataSourceService.cleanAndReopenDerby();

        this.jdbcTemplate = dataSourceService.getJdbcTemplate();
        this.transactionTemplate = dataSourceService.getTransactionTemplate();

        // Registers a Derby Raft state machine failure event for node degradation processing
        NotifyCenter.registerToSharePublisher(RaftDbErrorEvent.class);
        // Register the snapshot load event
        NotifyCenter.registerToSharePublisher(DerbyLoadEvent.class);

        NotifyCenter.registerSubscriber(new Subscriber<RaftDbErrorEvent>() {
            @Override
            public void onEvent(RaftDbErrorEvent event) {
                dataSourceService.setHealthStatus("DOWN");
            }

            @Override
            public Class<? extends Event> subscribeType() {
                return RaftDbErrorEvent.class;
            }
        });

        this.protocol.addRequestProcessors(Collections.singletonList(this));
        LOGGER.info("use DistributedTransactionServicesImpl");
    }

    @JustForTest
    public void mockConsistencyProtocol(CPProtocol protocol) {
        this.protocol = protocol;
    }

    @Override
    public <R> R queryOne(String sql, Class<R> cls) {
        try {
            LoggerUtils.printIfDebugEnabled(LOGGER, "queryOne info : sql : {}", sql);

            byte[] data = serializer.serialize(
                    SelectRequest.builder().queryType(QueryType.QUERY_ONE_NO_MAPPER_NO_ARGS).sql(sql)
                            .className(cls.getCanonicalName()).build());

            final boolean blockRead = EmbeddedStorageContextHolder
                    .containsExtendInfo(PersistenceConstant.EXTEND_NEED_READ_UNTIL_HAVE_DATA);

            Response response = innerRead(
                    ReadRequest.newBuilder().setGroup(group()).setData(ByteString.copyFrom(data)).build(), blockRead);
            if (response.getSuccess()) {
                return serializer.deserialize(response.getData().toByteArray(), cls);
            }
            throw new NJdbcException(response.getErrMsg(), response.getErrMsg());
        } catch (Exception e) {
            LOGGER.error("An exception occurred during the query operation : {}", e.toString());
            throw new NacosRuntimeException(NacosException.SERVER_ERROR, e.toString());
        }
    }

    @Override
    public <R> R queryOne(String sql, Object[] args, Class<R> cls) {
        try {
            LoggerUtils.printIfDebugEnabled(LOGGER, "queryOne info : sql : {}, args : {}", sql, args);

            byte[] data = serializer.serialize(
                    SelectRequest.builder().queryType(QueryType.QUERY_ONE_NO_MAPPER_WITH_ARGS).sql(sql).args(args)
                            .className(cls.getCanonicalName()).build());

            final boolean blockRead = EmbeddedStorageContextHolder
                    .containsExtendInfo(PersistenceConstant.EXTEND_NEED_READ_UNTIL_HAVE_DATA);

            Response response = innerRead(
                    ReadRequest.newBuilder().setGroup(group()).setData(ByteString.copyFrom(data)).build(), blockRead);
            if (response.getSuccess()) {
                return serializer.deserialize(response.getData().toByteArray(), cls);
            }
            throw new NJdbcException(response.getErrMsg(), response.getErrMsg());
        } catch (Exception e) {
            LOGGER.error("An exception occurred during the query operation : {}", e.toString());
            throw new NacosRuntimeException(NacosException.SERVER_ERROR, e.toString());
        }
    }

    @Override
    public <R> R queryOne(String sql, Object[] args, RowMapper<R> mapper) {
        try {
            LoggerUtils.printIfDebugEnabled(LOGGER, "queryOne info : sql : {}, args : {}", sql, args);
            /**
             * 通过 SelectRequest.builder() 构建一个 SelectRequest 对象，包含以下信息：
             *      queryType(QueryType.QUERY_ONE_WITH_MAPPER_WITH_ARGS)：查询类型，表示这是一个带映射器和参数的单条记录查询。
             *      sql(sql)：传入的 SQL 查询语句。
             *      args(args)：查询的参数数组。
             *      className(mapper.getClass().getCanonicalName())：行映射器 mapper 的类名，用于在后续反序列化过程中识别映射器。
             *
             * 构建完成后，调用 serializer.serialize() 方法，将这个 SelectRequest 对象序列化为字节数组 data，以便后续发送给分布式存储组件。
             */
            byte[] data = serializer.serialize(
                    SelectRequest.builder().queryType(QueryType.QUERY_ONE_WITH_MAPPER_WITH_ARGS).sql(sql).args(args)
                            .className(mapper.getClass().getCanonicalName()).build());
            // 检查当前的存储上下文是否需要阻塞式读取数据
            // EmbeddedStorageContextHolder.containsExtendInfo() 方法用于检查当前是否存在 EXTEND_NEED_READ_UNTIL_HAVE_DATA 的扩展信息
            // 如果存在这个信息，表示在执行读取时需要阻塞，直到有数据返回为止（blockRead 为 true），否则是非阻塞读取
            final boolean blockRead = EmbeddedStorageContextHolder
                    .containsExtendInfo(PersistenceConstant.EXTEND_NEED_READ_UNTIL_HAVE_DATA);
            // ReadRequest.newBuilder() 用于构建一个 ReadRequest 请求，包含以下信息：
            // setGroup(group())：设置请求所属的组（可能是数据库集群的某个分组）。
            // setData(ByteString.copyFrom(data))：将序列化后的查询数据（data）封装为 ByteString，并附加到请求中
            // 调用 innerRead 方法执行实际的读取操作，blockRead 决定读取是否为阻塞模式
            Response response = innerRead(
                    ReadRequest.newBuilder().setGroup(group()).setData(ByteString.copyFrom(data)).build(), blockRead);

            // response.getSuccess() 返回 true 时，表示读取成功。
            if (response.getSuccess()) {
                //成功后，调用 serializer.deserialize() 方法，将返回的 response.getData()（字节数组）反序列化为对应类型的对象。
                // ClassUtils.resolveGenericTypeByInterface(mapper.getClass()) 用于确定映射器 mapper 所映射的目标类型，确保反序列化的结果与预期类型一致。
                return serializer.deserialize(response.getData().toByteArray(),
                        ClassUtils.resolveGenericTypeByInterface(mapper.getClass()));
            }
            throw new NJdbcException(response.getErrMsg(), response.getErrMsg());
        } catch (Exception e) {
            LOGGER.error("An exception occurred during the query operation : {}", e.toString());
            throw new NacosRuntimeException(NacosException.SERVER_ERROR, e.toString());
        }
    }

    /**
     *
     * @param sql    sqk text  要执行的 SQL 查询语句
     * @param args   sql parameters  SQL 查询的参数数组，用于参数化查询
     * @param mapper Database query result converter   用于将查询结果映射为 Java 对象的实例
     * @return
     * @param <R>  返回一个 List，其中每个元素是通过 RowMapper 映射的结果
     */
    @Override
    public <R> List<R> queryMany(String sql, Object[] args, RowMapper<R> mapper) {
        try {
            LoggerUtils.printIfDebugEnabled(LOGGER, "queryMany info : sql : {}, args : {}", sql, args);
            // 创建一个 SelectRequest 对象，通过 builder 方法构建，
            // 设置 queryType 为 QUERY_MANY_WITH_MAPPER_WITH_ARGS，表示这是一个带有参数的多条记录查询。
            // 将 SQL 查询语句和参数作为请求的一部分，序列化成字节数组 data，便于后续网络传输。
            // mapper.getClass().getCanonicalName()：用于获取 RowMapper 类的完整类名，并添加到请求中，确保服务端可以理解如何处理映射
            byte[] data = serializer.serialize(
                    SelectRequest.builder().queryType(QueryType.QUERY_MANY_WITH_MAPPER_WITH_ARGS).sql(sql).args(args)
                            .className(mapper.getClass().getCanonicalName()).build());
            // 通过 EmbeddedStorageContextHolder.containsExtendInfo 检查是否需要阻塞读取。
            // 如果存储上下文中包含 EXTEND_NEED_READ_UNTIL_HAVE_DATA，则设置 blockRead 为 true，表示在数据可用之前进行阻塞读取。
            //这通常用于需要等待数据返回的场景
            final boolean blockRead = EmbeddedStorageContextHolder
                    .containsExtendInfo(PersistenceConstant.EXTEND_NEED_READ_UNTIL_HAVE_DATA);
            // 使用 innerRead 方法发送读取请求：
            // 调用 ReadRequest.newBuilder() 构建读取请求，设置 group（通常用于标识数据库分组）和序列化后的请求数据 data。
            // 如果 blockRead 为 true，请求会阻塞直到有数据返回。
            // 请求完成后，返回的结果是一个 Response 对象，包含了查询的结果数据或错误信息。
            Response response = innerRead(
                    ReadRequest.newBuilder().setGroup(group()).setData(ByteString.copyFrom(data)).build(), blockRead);
            if (response.getSuccess()) {
                // 如果查询成功（即 response.getSuccess() 返回 true），则将返回的数据通过 serializer.deserialize 反序列化为 List 对象
                // 将返回的字节数组反序列化为一个 List，该列表中的每个元素是通过 RowMapper 映射的查询结果
                return serializer.deserialize(response.getData().toByteArray(), List.class);
            }
            throw new NJdbcException(response.getErrMsg());
        } catch (Exception e) {
            LOGGER.error("An exception occurred during the query operation : {}", e.toString());
            throw new NacosRuntimeException(NacosException.SERVER_ERROR, e.toString());
        }
    }

    @Override
    public <R> List<R> queryMany(String sql, Object[] args, Class<R> rClass) {
        try {
            LoggerUtils.printIfDebugEnabled(LOGGER, "queryMany info : sql : {}, args : {}", sql, args);

            byte[] data = serializer.serialize(
                    SelectRequest.builder().queryType(QueryType.QUERY_MANY_NO_MAPPER_WITH_ARGS).sql(sql).args(args)
                            .className(rClass.getCanonicalName()).build());

            final boolean blockRead = EmbeddedStorageContextHolder
                    .containsExtendInfo(PersistenceConstant.EXTEND_NEED_READ_UNTIL_HAVE_DATA);

            Response response = innerRead(
                    ReadRequest.newBuilder().setGroup(group()).setData(ByteString.copyFrom(data)).build(), blockRead);
            if (response.getSuccess()) {
                return serializer.deserialize(response.getData().toByteArray(), List.class);
            }
            throw new NJdbcException(response.getErrMsg());
        } catch (Exception e) {
            LOGGER.error("An exception occurred during the query operation : {}", e.toString());
            throw new NacosRuntimeException(NacosException.SERVER_ERROR, e.toString());
        }
    }

    @Override
    public List<Map<String, Object>> queryMany(String sql, Object[] args) {
        try {
            LoggerUtils.printIfDebugEnabled(LOGGER, "queryMany info : sql : {}, args : {}", sql, args);

            byte[] data = serializer.serialize(
                    SelectRequest.builder().queryType(QueryType.QUERY_MANY_WITH_LIST_WITH_ARGS).sql(sql).args(args)
                            .build());

            final boolean blockRead = EmbeddedStorageContextHolder
                    .containsExtendInfo(PersistenceConstant.EXTEND_NEED_READ_UNTIL_HAVE_DATA);

            Response response = innerRead(
                    ReadRequest.newBuilder().setGroup(group()).setData(ByteString.copyFrom(data)).build(), blockRead);
            if (response.getSuccess()) {
                return serializer.deserialize(response.getData().toByteArray(), List.class);
            }
            throw new NJdbcException(response.getErrMsg());
        } catch (Exception e) {
            LOGGER.error("An exception occurred during the query operation : {}", e.toString());
            throw new NacosRuntimeException(NacosException.SERVER_ERROR, e.toString());
        }
    }

    /**
     * In some business situations, you need to avoid the timeout issue, so blockRead is used to determine this.
     *
     * @param request   {@link ReadRequest}
     * @param blockRead is async read operation
     * @return {@link Response}
     * @throws Exception Exception
     */
    private Response innerRead(ReadRequest request, boolean blockRead) throws Exception {
        if (blockRead) {
            return (Response) protocol.aGetData(request).join();
        }
        return protocol.getData(request);
    }

    @Override
    public CompletableFuture<RestResult<String>> dataImport(File file) {
        return CompletableFuture.supplyAsync(() -> {
            try (DiskUtils.LineIterator iterator = DiskUtils.lineIterator(file)) {
                int batchSize = 1000;
                List<String> batchUpdate = new ArrayList<>(batchSize);
                List<CompletableFuture<Response>> futures = new ArrayList<>();
                while (iterator.hasNext()) {
                    String sql = iterator.next();
                    if (StringUtils.isNotBlank(sql)) {
                        batchUpdate.add(sql);
                    }
                    boolean submit = batchUpdate.size() == batchSize || !iterator.hasNext();
                    if (submit) {
                        List<ModifyRequest> requests = batchUpdate.stream().map(ModifyRequest::new)
                                .collect(Collectors.toList());
                        CompletableFuture<Response> future = protocol.writeAsync(
                                WriteRequest.newBuilder().setGroup(group())
                                        .setData(ByteString.copyFrom(serializer.serialize(requests)))
                                        .putExtendInfo(DATA_IMPORT_KEY, Boolean.TRUE.toString()).build());
                        futures.add(future);
                        batchUpdate.clear();
                    }
                }
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                for (CompletableFuture<Response> future : futures) {
                    Response response = future.get();
                    if (!response.getSuccess()) {
                        return RestResultUtils.failed(response.getErrMsg());
                    }
                }
                return RestResultUtils.success();
            } catch (Throwable ex) {
                LOGGER.error("data import has error :", ex);
                return RestResultUtils.failed(ex.getMessage());
            }
        });
    }

    @Override
    public Boolean update(List<ModifyRequest> sqlContext, BiConsumer<Boolean, Throwable> consumer) {
        try {

            // Since the SQL parameter is Object[], in order to ensure that the types of
            // array elements are not lost, the serialization here is done using the java-specific
            // serialization framework, rather than continuing with the protobuff

            LoggerUtils.printIfDebugEnabled(LOGGER, "modifyRequests info : {}", sqlContext);

            // {timestamp}-{group}-{ip:port}-{signature}

            final String key =
                    System.currentTimeMillis() + "-" + group() + "-" + memberManager.getSelf().getAddress() + "-"
                            + MD5Utils.md5Hex(sqlContext.toString(), PersistenceConstant.DEFAULT_ENCODE);
            WriteRequest request = WriteRequest.newBuilder().setGroup(group()).setKey(key)
                    .setData(ByteString.copyFrom(serializer.serialize(sqlContext)))
                    .putAllExtendInfo(EmbeddedStorageContextHolder.getCurrentExtendInfo())
                    .setType(sqlContext.getClass().getCanonicalName()).build();
            if (Objects.isNull(consumer)) {
                Response response = this.protocol.write(request);
                if (response.getSuccess()) {
                    return true;
                }
                LOGGER.error("execute sql modify operation failed : {}", response.getErrMsg());
                return false;
            } else {
                this.protocol.writeAsync(request).whenComplete((BiConsumer<Response, Throwable>) (response, ex) -> {
                    String errMsg = Objects.isNull(ex) ? response.getErrMsg() : ExceptionUtil.getCause(ex).getMessage();
                    consumer.accept(response.getSuccess(),
                            StringUtils.isBlank(errMsg) ? null : new NJdbcException(errMsg));
                });
            }
            return true;
        } catch (TimeoutException e) {
            LOGGER.error("An timeout exception occurred during the update operation");
            throw new NacosRuntimeException(NacosException.SERVER_ERROR, e.toString());
        } catch (Throwable e) {
            LOGGER.error("An exception occurred during the update operation : {}", e);
            throw new NacosRuntimeException(NacosException.SERVER_ERROR, e.toString());
        }
    }

    @Override
    public List<SnapshotOperation> loadSnapshotOperate() {
        return Collections.singletonList(new DerbySnapshotOperation(writeLock));
    }

    @SuppressWarnings("all")
    @Override
    public Response onRequest(final ReadRequest request) {
        SelectRequest selectRequest = null;
        readLock.lock();
        Object data;
        try {
            selectRequest = serializer.deserialize(request.getData().toByteArray(), SelectRequest.class);
            LoggerUtils.printIfDebugEnabled(LOGGER, "getData info : selectRequest : {}", selectRequest);
            final RowMapper<Object> mapper = RowMapperManager.getRowMapper(selectRequest.getClassName());
            final byte type = selectRequest.getQueryType();
            switch (type) {
                case QueryType.QUERY_ONE_WITH_MAPPER_WITH_ARGS:
                    data = queryOne(jdbcTemplate, selectRequest.getSql(), selectRequest.getArgs(), mapper);
                    break;
                case QueryType.QUERY_ONE_NO_MAPPER_NO_ARGS:
                    data = queryOne(jdbcTemplate, selectRequest.getSql(),
                            ClassUtils.findClassByName(selectRequest.getClassName()));
                    break;
                case QueryType.QUERY_ONE_NO_MAPPER_WITH_ARGS:
                    data = queryOne(jdbcTemplate, selectRequest.getSql(), selectRequest.getArgs(),
                            ClassUtils.findClassByName(selectRequest.getClassName()));
                    break;
                case QueryType.QUERY_MANY_WITH_MAPPER_WITH_ARGS:
                    data = queryMany(jdbcTemplate, selectRequest.getSql(), selectRequest.getArgs(), mapper);
                    break;
                case QueryType.QUERY_MANY_WITH_LIST_WITH_ARGS:
                    data = queryMany(jdbcTemplate, selectRequest.getSql(), selectRequest.getArgs());
                    break;
                case QueryType.QUERY_MANY_NO_MAPPER_WITH_ARGS:
                    data = queryMany(jdbcTemplate, selectRequest.getSql(), selectRequest.getArgs(),
                            ClassUtils.findClassByName(selectRequest.getClassName()));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported data query categories");
            }
            ByteString bytes = data == null ? ByteString.EMPTY : ByteString.copyFrom(serializer.serialize(data));
            return Response.newBuilder().setSuccess(true).setData(bytes).build();
        } catch (Exception e) {
            LOGGER.error("There was an error querying the data, request : {}, error : {}", selectRequest, e.toString());
            return Response.newBuilder().setSuccess(false)
                    .setErrMsg(ClassUtils.getSimplaName(e) + ":" + ExceptionUtil.getCause(e).getMessage()).build();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Response onApply(WriteRequest log) {
        LoggerUtils.printIfDebugEnabled(LOGGER, "onApply info : log : {}", log);
        final ByteString byteString = log.getData();
        Preconditions.checkArgument(byteString != null, "Log.getData() must not null");
        final Lock lock = readLock;
        lock.lock();
        try {
            List<ModifyRequest> sqlContext = serializer.deserialize(byteString.toByteArray(), List.class);
            boolean isOk = false;
            if (log.containsExtendInfo(DATA_IMPORT_KEY)) {
                isOk = doDataImport(jdbcTemplate, sqlContext);
            } else {
                sqlContext.sort(Comparator.comparingInt(ModifyRequest::getExecuteNo));
                isOk = update(transactionTemplate, jdbcTemplate, sqlContext);
                // If there is additional information, post processing
                // Put into the asynchronous thread pool for processing to avoid blocking the
                // normal execution of the state machine
                PersistenceExecutor.executeEmbeddedDump(() -> {
                    for (EmbeddedApplyHook each : EmbeddedApplyHookHolder.getInstance().getAllHooks()) {
                        each.afterApply(log);
                    }
                });
            }

            return Response.newBuilder().setSuccess(isOk).build();

            // We do not believe that an error caused by a problem with an SQL error
            // should trigger the stop operation of the raft state machine
        } catch (BadSqlGrammarException | DataIntegrityViolationException e) {
            return Response.newBuilder().setSuccess(false).setErrMsg(e.toString()).build();
        } catch (DataAccessException e) {
            throw new ConsistencyException(e.toString());
        } catch (Exception e) {
            LoggerUtils.printIfWarnEnabled(LOGGER, "onApply warn : log : {}", log, e);
            return Response.newBuilder().setSuccess(false).setErrMsg(e.toString()).build();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onError(Throwable throwable) {
        // Trigger reversion strategy
        NotifyCenter.publishEvent(new RaftDbErrorEvent(throwable));
    }

    @Override
    public String group() {
        return PersistenceConstant.CONFIG_MODEL_RAFT_GROUP;
    }
}
