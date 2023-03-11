/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.sf.enumerator;

import com.ververica.cdc.connectors.sf.request.bean.CallBackTableChangeBean;
import com.ververica.cdc.connectors.sf.request.bean.TableChangeBean;

import java.util.List;
import java.util.function.Consumer;

/** 动态的获取表变更，汇报表变更. */
public interface DynamicTableChange {

    /** 获取到表表更列表. */
    void tableChangeCapture(Consumer<List<TableChangeBean>> tableChangeConsumer);

    /** 回调表变更成功. */
    void tableChangeCallback(List<CallBackTableChangeBean> callBackTableChangeBeans);
}
