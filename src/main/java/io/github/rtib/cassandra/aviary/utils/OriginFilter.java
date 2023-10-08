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

import io.github.rtib.cassandra.aviary.model.IOrigin;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Implementation of a predicate that can be applied as filter to
 * a stream of IOrigin instances. This holds two regex patterns to be applied
 * to keyspace and table names. An origin matches the filter if both, the 
 * keyspace and the table matches its pattern. Patterns are considered case-
 * insensitive as CQL handles keyspace and table name case-insensitive as well.
 * @author repasi
 */
public class OriginFilter implements Predicate<IOrigin> {

    private final Pattern keyspacePattern;
    private final Pattern tablePattern;

    /**
     * Parsing a filter string 
     * @param filterString 
     */
    public OriginFilter(final String filterString) {
        String[] parts = filterString.split(":", 2);
        this.keyspacePattern = Pattern.compile(parts[0], Pattern.CASE_INSENSITIVE);
        this.tablePattern = Pattern.compile(parts[1], Pattern.CASE_INSENSITIVE);
    }
    
    @Override
    public boolean test(IOrigin t) {
        return this.keyspacePattern.matcher(t.getKeyspace()).matches() &&
                this.tablePattern.matcher(t.getTable()).matches();
    }
    
    @Override
    public String toString() {
        return keyspacePattern + ":" + tablePattern;
    }
    
}
