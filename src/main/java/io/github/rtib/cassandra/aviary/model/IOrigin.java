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
package io.github.rtib.cassandra.aviary.model;

/**
 * An origin of a canary are the keyspace and table names the canary is from.
 * 
 * @author repasi
 */
public interface IOrigin {
    
    /**
     * Name of the originating keyspace.
     * 
     * @return keyspace name as represented in CQL
     */
    String getKeyspace();
    
    /**
     * Name of the originating table.
     * 
     * @return table name as represented in CQL
     */
    String getTable();
    
}
