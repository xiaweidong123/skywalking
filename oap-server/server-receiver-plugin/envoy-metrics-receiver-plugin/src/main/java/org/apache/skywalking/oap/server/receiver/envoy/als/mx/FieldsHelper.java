/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.receiver.envoy.als.mx;

import com.google.common.base.Splitter;
import com.google.common.reflect.Invokable;
import com.google.common.reflect.TypeToken;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.skywalking.oap.server.library.module.ModuleStartException;
import org.apache.skywalking.oap.server.library.util.ResourceUtils;
import org.apache.skywalking.oap.server.receiver.envoy.als.ServiceMetaInfo;
import org.yaml.snakeyaml.Yaml;

@Slf4j
@SuppressWarnings("UnstableApiUsage")
enum FieldsHelper {
    SINGLETON;

    private boolean initialized = false;

    /**
     * The mappings from the field name of {@link ServiceMetaInfo} to the field name of {@code flatbuffers}.
     */
    private Map<String, List<String>> fieldNameMapping;

    /**
     * The mappings from the field name of {@link ServiceMetaInfo} to its {@code setter}.
     */
    private Map<String, Invokable<ServiceMetaInfo, ?>> fieldSetterMapping;

    @SuppressWarnings("unchecked")
    public void init(final String file) throws Exception {
        if (initialized) {
            return;
        }
        final Yaml yaml = new Yaml();
        final InputStream inputStream = ResourceUtils.readToStream(file);
        final Map<String, String> config = (Map<String, String>) yaml.load(inputStream);

        fieldNameMapping = new HashMap<>(config.size());
        fieldSetterMapping = new HashMap<>(config.size());

        for (final Map.Entry<String, String> entry : config.entrySet()) {
            final String serviceMetaInfoFieldName = entry.getKey();
            final String flatBuffersFieldName = entry.getValue();
            final List<String> flatBuffersFieldNames = Splitter.on('.').omitEmptyStrings().splitToList(flatBuffersFieldName);
            fieldNameMapping.put(serviceMetaInfoFieldName, flatBuffersFieldNames);

            try {
                final Method setterMethod = ServiceMetaInfo.class.getMethod("set" + StringUtils.capitalize(serviceMetaInfoFieldName), String.class);
                final Invokable<ServiceMetaInfo, ?> setter = new TypeToken<ServiceMetaInfo>() {
                }.method(setterMethod);
                setter.setAccessible(true);
                fieldSetterMapping.put(serviceMetaInfoFieldName, setter);
            } catch (final NoSuchMethodException e) {
                throw new ModuleStartException("Initialize method error", e);
            }
        }
        initialized = true;
    }

    /**
     * Inflates the {@code serviceMetaInfo} with the given {@link Struct struct}.
     *
     * @param metadata        the {@link Struct} metadata from where to retrieve and inflate the {@code serviceMetaInfo}.
     * @param serviceMetaInfo the {@code serviceMetaInfo} to be inflated.
     * @throws Exception if failed to inflate the {@code serviceMetaInfo}
     */
    public void inflate(final Struct metadata, final ServiceMetaInfo serviceMetaInfo) throws Exception {
        final Value root = Value.newBuilder().setStructValue(metadata).build();
        for (Map.Entry<String, List<String>> entry : fieldNameMapping.entrySet()) {
            Value value = root;
            for (final String property : entry.getValue()) {
                value = value.getStructValue().getFieldsOrThrow(property);
            }
            setValue(entry.getKey(), value, serviceMetaInfo);
        }
    }

    public void setValue(final String key, final Value value, final ServiceMetaInfo result) throws Exception {
        fieldSetterMapping.get(key).invoke(result, value.getStringValue());
    }

}
