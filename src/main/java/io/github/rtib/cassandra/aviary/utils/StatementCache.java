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

import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Caching CQL prepared statements on the fly. For any type of object, a 
 * statement preparation method can be implemented. This cache will give you the
 * statement for a specified key from cache or by preparing it.
 * @author repasi
 * @param <K> Type of the statement identifier
 */
public abstract class StatementCache<K> {
    
    private final ConcurrentHashMap<K, PreparedStatement> cache;
    
    public StatementCache() {
        cache = new ConcurrentHashMap<>();
    }
    
    /**
     * Get the prepared statement for an identifier. If the statement was
     * already prepared for the given key, then this method will get it from
     * the cache, otherwise it will call the prepareStatementFor method to
     * prepare the statement and store it in the cache.
     * @param key statement identifier
     * @return the prepared statement for the given key
     */
    public synchronized PreparedStatement getStatement(final K key) {
        if (!cache.containsKey(key))
            cache.put(key, prepareStatementFor(key));
        return cache.get(key);
    }

    /**
     * Implements the statement preparation for a given key.
     * @param key statement identifier
     * @return the prepared statement for the given identifier
     */
    public abstract PreparedStatement prepareStatementFor(final K key);
    
}
