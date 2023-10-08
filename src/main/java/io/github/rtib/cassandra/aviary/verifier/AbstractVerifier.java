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
package io.github.rtib.cassandra.aviary.verifier;

import com.datastax.oss.driver.api.core.CqlSession;
import io.github.rtib.cassandra.aviary.model.ICanary;
import io.github.rtib.cassandra.aviary.model.IOrigin;
import io.github.rtib.cassandra.aviary.utils.CassandraMetadataHelper;
import io.github.rtib.cassandra.aviary.utils.StatementCache;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import org.apache.commons.lang3.ClassUtils;

/**
 *
 * @author repasi
 */
public abstract class AbstractVerifier extends StatementCache<IOrigin> implements ICanaryVerifier {
    
    protected final CqlSession cqlSession;
    protected final Iterable<ICanary> reader;
    protected final ExecutorService executor;
    protected final CassandraMetadataHelper helper;
    
    public AbstractVerifier(CqlSession session, Iterable<ICanary> reader, ExecutorService executor) {
        this.cqlSession = session;
        this.reader = reader;
        this.executor = executor;
        this.helper = new CassandraMetadataHelper(session);
    }
    
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Class<?> selectorClass;
        private CqlSession session;
        private ExecutorService executor;
        private Iterable<ICanary> reader;
        private List<Predicate<IOrigin>> filters = Collections.EMPTY_LIST;

        public Builder() {
            this.executor = Executors.newVirtualThreadPerTaskExecutor();
        }
        
        public Builder forName(final String className) throws VerifierBuilderException {
            Class<?> c;
            try {
                c = Class.forName(className);
            } catch (ClassNotFoundException ex) {
                throw new VerifierBuilderException("Cannot find selector class for name.", ex);
            }
            if (!ClassUtils.getAllInterfaces(c).stream()
                    .anyMatch(x -> x == ICanaryVerifier.class))
                throw new VerifierBuilderException("Loaded class is not implementing ICanarySelector.");
            this.selectorClass = c;
            return this;
        }
        
        public Builder withCqlSession(final CqlSession session) {
            this.session = session;
            return this;
        }
        
        public Builder withAviaryReader(final Iterable<ICanary> reader) {
            this.reader = reader;
            return this;
        }
        
        public Builder withExecutorService(final ExecutorService executor) {
            this.executor = executor;
            return this;
        }
        
        public Builder withOriginFilters(final Predicate<IOrigin>... filters) {
            if (filters == null || filters.length == 0)
                this.filters = Collections.EMPTY_LIST;
            else
                this.filters = Arrays.asList(filters);
            return this;
        }
        
        public ICanaryVerifier build() throws VerifierBuilderException {
            ICanaryVerifier inst;
            try {
                inst = (ICanaryVerifier) selectorClass.getConstructor(CqlSession.class, Iterable.class, ExecutorService.class)
                        .newInstance(session, reader, executor);
                inst.setOriginFilter(filters.stream().reduce(x->true, Predicate::and));
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException ex) {
                throw new VerifierBuilderException("Failed to build verifier instance.", ex);
            }
            return inst;
        }
    }

    public static class VerifierBuilderException extends Exception {

        public VerifierBuilderException(String message, Throwable cause) {
            super(message, cause);
        }

        private VerifierBuilderException(String message) {
            super(message);
        }
    }
}
