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
package io.github.rtib.cassandra.aviary.selector;

import io.github.rtib.cassandra.aviary.model.IOrigin;
import java.util.function.Predicate;

/**
 * Interface of canary selectors.
 * 
 * @author repasi
 */
public interface ICanarySelector {
    
    /**
     * Start selecting canaries.
     */
    void selectCanaries();

    /**
     * Set a filter predicate the list of origins need to pass through. Canaries
     * will only be selected from origins passing the filter.
     * @param filter a stream predicate that apply to IOrigin entries
     */
    default void setOriginFilter(Predicate<IOrigin> filter) {};
}
