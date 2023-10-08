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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import io.github.rtib.cassandra.aviary.model.IOrigin;
import io.github.rtib.cassandra.aviary.storage.ICanaryWriter;
import io.github.rtib.cassandra.aviary.storage.Origin;
import io.github.rtib.cassandra.aviary.utils.CassandraMetadataHelper;
import io.github.rtib.cassandra.aviary.utils.StatementCache;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ClassUtils;

/**
 * Abstract selector and factory.
 * 
 * @author repasi
 */
public abstract class AbstractSelector extends StatementCache<IOrigin> implements ICanarySelector {
    private static final Logger LOG = Logger.getLogger(AbstractSelector.class.getName());

    protected final CqlSession cqlSession;
    protected final ICanaryWriter canaryWriter;
    protected final ExecutorService executor;
    protected final CassandraMetadataHelper helper;
    private Predicate<IOrigin> originFilter;

    /**
     * Constructor of all ICanarySelector implementations extending this class.
     * @param cqlSession the connected CqlSession
     * @param writer
     */
    public AbstractSelector(CqlSession cqlSession, ICanaryWriter writer) {
        this.cqlSession = cqlSession;
        this.canaryWriter = writer;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.helper = new CassandraMetadataHelper(cqlSession);
    }

    /**
     * Get a builder instance to load and build an actual selector.
     * @return a new instance of AbstractSelector.Builder
     */    
    public static Builder builder() {
        return new Builder();
    }
    
    @Override
    public void selectCanaries() {
        executeSelectCanaries();
    }

    @Override
    public void setOriginFilter(Predicate<IOrigin> filter) {
        this.originFilter = filter;
    }
    
    /**
     * Here the actual canary selection needs to be implemented.
     */
    public abstract void executeSelectCanaries();
    
    /**
     * Get all origins, i.e. all tables from all non-system keyspaces of the
     * Cassandra cluster.
     * @return set of origins, i.e. keyspace-table-pairs
     */
    protected Set<IOrigin> getOrigins() {
        // TODO: a more versatile filter infrastructure
        return cqlSession.getMetadata().getKeyspaces().values().stream()
                .parallel()
                .<IOrigin> mapMulti((keyspace, consumer) -> {
                    for (CqlIdentifier table : keyspace.getTables().keySet())
                        consumer.accept(new Origin(keyspace.getName().asCql(true), table.asCql(true)));
                })
                .filter(originFilter)
                .collect(Collectors.toSet());
    }

    /**
     * Acquire the token map of the connected Cassandra cluster.
     * @return token map object
     */
    protected TokenMap getTokenMap() {
        return cqlSession.getMetadata().getTokenMap().orElseThrow();
    }

    /**
     * Builder to load, setup and instantiate a selector class.
     */
    public static class Builder {

        private Class<?> selectorClass;
        private CqlSession session;
        private ICanaryWriter writer;
        private List<Predicate<IOrigin>> filters = Collections.EMPTY_LIST;

        public Builder() {
        }
        
        /**
         * Search for a Class with given name implementing ICanarySelector and
         * load it.
         * @param className fully qualified name of the class to load
         * @return this builder instance
         * @throws io.github.rtib.cassandra.aviary.selector.AbstractSelector.SelectorBuilderException if a class with the given name cannot be found or is not implementing ICanarySelector
         */
        public Builder forName(final String className) throws SelectorBuilderException {
            Class<?> c;
            try {
                c = Class.forName(className);
            } catch (ClassNotFoundException ex) {
                throw new SelectorBuilderException("Cannot find selector class for name.", ex);
            }
            if (!ClassUtils.getAllInterfaces(c).stream()
                    .anyMatch(x -> x == ICanarySelector.class))
                throw new SelectorBuilderException("Loaded class is not implementing ICanarySelector.");
            this.selectorClass = c;
            return this;
        }
        
        /**
         * Setup builder with a CqlSession connected to a Cassandra.
         * @param session connected CqlSession object
         * @return this builder instance
         */
        public Builder withCqlSession(final CqlSession session) {
            this.session = session;
            return this;
        }
        
        /**
         * Set any number of OriginFilter instances as filter. If called without
         * argument, a null value or an empty array, filters will be reset to
         * empty list. The list will be reduced to a single predicate by
         * combining all filters of the list with and operations.
         * @param filters zero of any number of predicate instances, e.g. of OriginFilter class
         * @return this builder instance
         */
        public Builder withOriginFilters(final Predicate<IOrigin>... filters) {
            if (filters == null || filters.length == 0)
                this.filters = Collections.EMPTY_LIST;
            else
                this.filters = Arrays.asList(filters);
            return this;
        }

        /**
         * Setup builder with an ICanaryWriter to be used for storing selected canaries.
         * @param writer an instance of an ICanaryWriter
         * @return this builder instance
         */
        public Builder withCanaryWriter(final ICanaryWriter writer) {
            this.writer = writer;
            return this;
        }
        
        /**
         * Instantiate the selected ICanarySelector class and set up with
         * parameter provided to this builder.
         * @return newly built ICanarySelector instance
         * @throws io.github.rtib.cassandra.aviary.selector.AbstractSelector.SelectorBuilderException 
         */
        public ICanarySelector build() throws SelectorBuilderException {
            ICanarySelector inst;
            try {
                inst = (ICanarySelector) selectorClass.getConstructor(CqlSession.class, ICanaryWriter.class).newInstance(session, writer);
                inst.setOriginFilter(filters.stream().reduce(x->true, Predicate::and));
                return inst;
            } catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
                throw new SelectorBuilderException("Failed to build selector instance.", ex);
            }
        }
    }

    public static class SelectorBuilderException extends Exception {

        public SelectorBuilderException(String message, Throwable cause) {
            super(message, cause);
        }

        private SelectorBuilderException(String message) {
            super(message);
        }
    }
}
