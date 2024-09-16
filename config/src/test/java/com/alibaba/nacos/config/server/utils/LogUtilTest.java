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

package com.alibaba.nacos.config.server.utils;

import ch.qos.logback.classic.Logger;
import org.junit.Assert;
import org.junit.Test;

public class LogUtilTest {

    @Test
    public void testSetLogLevel() {

        LogUtil.setLogLevel("config-server", "INFO");
        Logger defaultLog = (Logger) LogUtil.DEFAULT_LOG;
        Assert.assertEquals("INFO", defaultLog.getLevel().levelStr);

        LogUtil.setLogLevel("config-fatal", "INFO");
        Logger fatalLog = (Logger) LogUtil.FATAL_LOG;
        Assert.assertEquals("INFO", fatalLog.getLevel().levelStr);

        LogUtil.setLogLevel("config-pull", "INFO");
        Logger pullLog = (Logger) LogUtil.PULL_LOG;
        Assert.assertEquals("INFO", pullLog.getLevel().levelStr);

        LogUtil.setLogLevel("config-pull-check", "INFO");
        Logger pullCheckLog = (Logger) LogUtil.PULL_CHECK_LOG;
        Assert.assertEquals("INFO", pullCheckLog.getLevel().levelStr);

        LogUtil.setLogLevel("config-dump", "INFO");
        Logger dumpLog = (Logger) LogUtil.DUMP_LOG;
        Assert.assertEquals("INFO", dumpLog.getLevel().levelStr);

        LogUtil.setLogLevel("config-memory", "INFO");
        Logger memoryLog = (Logger) LogUtil.MEMORY_LOG;
        Assert.assertEquals("INFO", memoryLog.getLevel().levelStr);

        LogUtil.setLogLevel("config-client-request", "INFO");
        Logger clientRequestLog = (Logger) LogUtil.CLIENT_LOG;
        Assert.assertEquals("INFO", clientRequestLog.getLevel().levelStr);

        LogUtil.setLogLevel("config-trace", "INFO");
        Logger traceLog = (Logger) LogUtil.TRACE_LOG;
        Assert.assertEquals("INFO", traceLog.getLevel().levelStr);

        LogUtil.setLogLevel("config-notify", "INFO");
        Logger notifyLog = (Logger) LogUtil.NOTIFY_LOG;
        Assert.assertEquals("INFO", notifyLog.getLevel().levelStr);

    }
}
