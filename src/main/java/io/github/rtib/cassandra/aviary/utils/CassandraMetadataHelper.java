/*
 * Copyright 2023 T. Répási.
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
package io.github.rtib.cassandra.aviary.utils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import io.github.rtib.cassandra.aviary.model.IOrigin;
import java.util.List;

/**
 *
 * @author repasi
 */
public class CassandraMetadataHelper {

    private final CqlSession cqlSession;

    public CassandraMetadataHelper(CqlSession cqlSession) {
        this.cqlSession = cqlSession;
    }

    /**
     * Helper function to get the metadata of a given table.
     * @param origin identify the keyspace and table
     * @return
     */
    protected TableMetadata getTableMetadata(IOrigin origin) {
        return cqlSession.getMetadata().getKeyspace(origin.getKeyspace()).orElseThrow().getTable(origin.getTable()).orElseThrow();
    }

    /**
     * List all fields of partition key of a given table.
     * @param origin identify the keyspace and table
     * @return List of field names contained in primary key
     */
    public List<String> getPrimaryKey(IOrigin origin) {
        return getTableMetadata(origin).getPrimaryKey().stream()
                .map(ColumnMetadata::getName)
                .map(CqlIdentifier::toString)
                .toList();
    }

    /**
     * List all fields of partition key of a given table.
     * @param origin identify the keyspace and table
     * @return List of field names contained in partition key
     */
    public List<CqlIdentifier> getPartitionKey(IOrigin origin) {
        return getTableMetadata(origin).getPartitionKey().stream()
                .map(ColumnMetadata::getName)
                .toList();
    }
    
}
