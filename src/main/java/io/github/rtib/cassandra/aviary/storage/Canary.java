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

import io.github.rtib.cassandra.aviary.model.ICanary;
import io.github.rtib.cassandra.aviary.model.IOrigin;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Canary implementation.
 * 
 * @author repasi
 */
public final class Canary implements ICanary {
    private final Origin origin;
    private final Map<String,Object> identifier;

    public Canary(final String keyspace, final String table, final Map<String,Object> identifier) {
        this.origin = new Origin(keyspace, table);
        this.identifier = Map.copyOf(identifier);
    }

    @Override
    public IOrigin getOrigin() {
        return origin;
    }

    @Override
    public Map<String, Object> getIdentifier() {
        return Map.copyOf(identifier);
    }

    @Override
    public String toString() {
        return "Canary{" + "Origin=" + origin + ", PrimaryKey=" + identifier + "}";
    }
    
    protected Canary() {
        this.origin = null;
        this.identifier = null;
    }

    /**
     * Get a builder to build a canary step-by-step.
     * 
     * @return CanaryBuilder instance
     */
    public static CanaryBuilder builder() {
        return new CanaryBuilder();
    }
    
    /**
     * Builder class of Canary instances.
     */
    public static final class CanaryBuilder {
        private String keyspace;
        private String table;
        private final Set<String> pkFields;
        private Map<String,Object> pkValues;

        public CanaryBuilder() {
            keyspace = null;
            table = null;
            pkFields = new HashSet<>();
            pkValues = new HashMap<>();
        }

        /**
         * Reset all previously stored field values.
         * 
         * @return this builder instance
         */
        public CanaryBuilder resetFieldValues() {
            pkValues = new HashMap<>();
            return this;
        }

        /**
         * Set the keyspace of origin.
         * 
         * @param keyspace the origin keyspace
         * @return this builder instance
         */
        public CanaryBuilder withKeyspace(final String keyspace) {
            this.keyspace = keyspace;
            return this;
        }

        /**
         * Set the table of origin.
         * 
         * @param table the origin table
         * @return this builder instance
         */
        public CanaryBuilder withTable(final String table) {
            this.table = table;
            return this;
        }
        
        /**
         * Set the origin of the canary.
         * 
         * @param origin IOrigin object
         * @return this builder instance
         */
        public CanaryBuilder withOrigin(final IOrigin origin) {
            this.keyspace = origin.getKeyspace();
            this.table = origin.getTable();
            return this;
        }

        /**
         * Set a field value.
         * 
         * @param column name of the field as represented in CQL
         * @param value object representing the value
         * @return this builder instance
         */
        public CanaryBuilder withField(String column, Object value) {
            if (pkFields.contains(column))
                pkValues.put(column, value);
            return this;
        }

        public CanaryBuilder withPrimaryKeyField(final String pkField) {
            pkFields.add(pkField);
            return this;
        }

        /**
         * Set the fields of the primary key. The valueset of these fields will
         * be considered the key to the canary.
         * 
         * @param primaryKeys list of field name as represented in CQL
         * @return this builder instance
         */
        public CanaryBuilder withPrimaryKeyFields(List<String> primaryKeys) {
            for (String pkField : primaryKeys)
                pkFields.add(pkField);
            return this;
        }

        /**
         * Build the Canary instance.
         * 
         * @return an object implementing ICanary as built from the values set
         * @throws IncompletePrimaryKeyException if fields has been declared key but no value was associated with
         */
        public ICanary build() throws IncompletePrimaryKeyException {        
            if (!pkValues.keySet().equals(pkFields))
                throw new IncompletePrimaryKeyException();

            return new Canary(keyspace, table, pkValues);
        }
    }

    public static final class IncompletePrimaryKeyException extends Exception {
        
    }
}
