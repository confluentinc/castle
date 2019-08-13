/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.castle.role;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Schema {
    private final String subject;
    private final String schema;

    @JsonCreator
    public Schema(@JsonProperty("subject") String subject,
                  @JsonProperty("schema") String schema) {
        this.subject = subject == null ? "" : subject;
        this.schema = schema == null ? "" : schema;
    }

    @JsonProperty
    public String subject() {
        return subject;
    }

    @JsonProperty
    public String schema() {
        return schema;
    }
};
