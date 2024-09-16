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

package com.alibaba.nacos.naming.remote.rpc.handler;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.remote.request.ServiceListRequest;
import com.alibaba.nacos.api.naming.remote.response.ServiceListResponse;
import com.alibaba.nacos.api.remote.request.RequestMeta;
import com.alibaba.nacos.auth.annotation.Secured;
import com.alibaba.nacos.core.control.TpsControl;
import com.alibaba.nacos.core.paramcheck.ExtractorManager;
import com.alibaba.nacos.core.paramcheck.impl.ServiceListRequestParamExtractor;
import com.alibaba.nacos.core.remote.RequestHandler;
import com.alibaba.nacos.naming.core.v2.ServiceManager;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.utils.ServiceUtil;
import com.alibaba.nacos.plugin.auth.constant.ActionTypes;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * Service list request handler.
 *
 * @author xiweng.yy
 *
 * TODO 处理客户端提交的服务列表请求的handler
 */
@Component
public class ServiceListRequestHandler extends RequestHandler<ServiceListRequest, ServiceListResponse> {

    @Override
    @TpsControl(pointName = "RemoteNamingServiceListQuery", name = "RemoteNamingServiceListQuery")
    @Secured(action = ActionTypes.READ)
    @ExtractorManager.Extractor(rpcExtractor = ServiceListRequestParamExtractor.class)
    public ServiceListResponse handle(ServiceListRequest request, RequestMeta meta) throws NacosException {
        // ServiceManager.getInstance()通过单例返回一个ServiceManager对象
        /**
         * 获取指定命令空间下的所有服务，在ServiceManager中存在一个map保存着每个命名空间中的所有服务。
         * ConcurrentHashMap<String, Set<Service>> namespaceSingletonMaps = new ConcurrentHashMap<>(1 << 2)
         * key: namespaceId
         * value: Set<Service>
         * 注册实例的时候，就往这个map写入了数据
         *
         * ServiceManager.getInstance().getSingletons(request.getNamespace())相当于执行：
         * namespaceSingletonMaps.getOrDefault(namespace, new HashSet<>(1))
         */
        Collection<Service> serviceSet = ServiceManager.getInstance().getSingletons(request.getNamespace());
        // 构建响应结果
        ServiceListResponse result = ServiceListResponse.buildSuccessResponse(0, new LinkedList<>());
        if (!serviceSet.isEmpty()) {
            // 过滤指定分组的Service，添加groupServiceName，格式如：groupA@@serviceA
            Collection<String> serviceNameSet = selectServiceWithGroupName(serviceSet, request.getGroupName());
            // TODO select service by selector
            // 按分页裁剪serviceNameSet
            List<String> serviceNameList = ServiceUtil
                    .pageServiceName(request.getPageNo(), request.getPageSize(), serviceNameSet);
            result.setCount(serviceNameSet.size());
            result.setServiceNames(serviceNameList);
        }
        return result;
    }

    private Collection<String> selectServiceWithGroupName(Collection<Service> serviceSet, String groupName) {
        Collection<String> result = new HashSet<>(serviceSet.size());
        for (Service each : serviceSet) {
            if (Objects.equals(groupName, each.getGroup())) {
                result.add(each.getGroupedServiceName());
            }
        }
        return result;
    }

}
