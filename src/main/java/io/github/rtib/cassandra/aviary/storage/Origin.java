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
package io.github.rtib.cassandra.aviary.storage;

import io.github.rtib.cassandra.aviary.model.IOrigin;
import java.util.Objects;

/**
 * Class representing the origin of a canary. An object is holding keyspace and
 * table names as represented in CQL.
 * 
 * @author repasi
 */
public final class Origin implements IOrigin {
    private final String keyspace;
    private final String table;

    /**
     * Create a new instance.
     * 
     * @param keyspace name of the keyspace as represented in CQL
     * @param table name of the table as represented in CQL
     */
    public Origin(final String keyspace, final String table) {
        this.keyspace = keyspace;
        this.table = table;
    }

    protected Origin() {
        this.keyspace = null;
        this.table = null;
    }
    
    @Override
    public String getKeyspace() {
        return keyspace;
    }

    @Override
    public String getTable() {
        return table;
    }
    
    @Override
    public String toString() {
        return keyspace + "." + table;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash + Objects.hashCode(this.keyspace);
        hash = 37 * hash + Objects.hashCode(this.table);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Origin other = (Origin) obj;
        if (!Objects.equals(this.keyspace, other.keyspace)) {
            return false;
        }
        return Objects.equals(this.table, other.table);
    }
}
