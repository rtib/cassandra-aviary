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

/**
 * Interface to implement by any writer class that will be able to store
 * canaries.
 * @author repasi
 */
public interface ICanaryWriter {
    
    /**
     * Store one Canary instance.
     * @param canary the instance to store
     */
    void write(ICanary canary);
    
}
