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

package com.yuanfudao.multi.jdbc.connector.table;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** multi jdbc split serializer. */
public class MultiJdbcSplitSerializer
        implements SimpleVersionedSerializer<MultiJdbcPartitionSplit> {

    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(MultiJdbcPartitionSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(split.getQuery());
            out.writeUTF(split.getJdbcUrl());
            if (split.getUsername() != null && split.getPassword() != null) {
                out.writeUTF("password");
                out.writeUTF(split.getUsername());
                out.writeUTF(split.getPassword());
            } else {
                out.writeUTF("none");
            }
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public MultiJdbcPartitionSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            String query = in.readUTF();
            String jdbcUrl = in.readUTF();
            String security = in.readUTF();
            String username = null;
            String password = null;
            if (security.equals("password")) {
                username = in.readUTF();
                password = in.readUTF();
            }
            return new MultiJdbcPartitionSplit(query, jdbcUrl, username, password);
        }
    }
}
