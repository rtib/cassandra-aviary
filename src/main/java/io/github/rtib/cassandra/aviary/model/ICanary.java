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

import java.util.Map;

/**
 * Interface of Canary representations.
 *
 * @author repasi
 */
public interface ICanary {
    
    /**
     * The origin of the Canary is a pair of strings containing keyspace and
     * table names.
     * 
     * @return the canary origin
     */
    IOrigin getOrigin();
    
    /**
     * The identifier of the canary is a set of its primary key fields and
     * values.
     * 
     * @return map of primary key fields and their values
     */
    // TODO: SequencedMap could be better here, but lacks support with Jackson < 2.16.
    Map<String,Object> getIdentifier();
}
