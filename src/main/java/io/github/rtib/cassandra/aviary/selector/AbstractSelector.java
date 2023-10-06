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
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import io.github.rtib.cassandra.aviary.model.IOrigin;
import io.github.rtib.cassandra.aviary.storage.ICanaryWriter;
import io.github.rtib.cassandra.aviary.storage.Origin;
import io.github.rtib.cassandra.aviary.utils.CassandraMetadataHelper;
import io.github.rtib.cassandra.aviary.utils.StatementCache;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
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

    private String keyspaceFilter = ".*";
    private String tableFilter = ".*";
    
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
    public void setKeyspaceFilter(final String keyspaceFilter) {
        if (keyspaceFilter == null)
            return;
        try {
            Pattern.compile(keyspaceFilter);
            this.keyspaceFilter = keyspaceFilter;
        } catch (PatternSyntaxException ex) {
            LOG.log(Level.WARNING, "failed to set keyspace filter.", ex);
        }
    }

    @Override
    public void setTableFilter(final String tableFilter) {
        if (tableFilter == null)
            return;
        try {
            Pattern.compile(tableFilter);
            this.tableFilter = tableFilter;
        } catch (PatternSyntaxException ex) {
            LOG.log(Level.WARNING, "failed to set table filter.", ex);
        }
    }

    @Override
    public void selectCanaries() {
        executeSelectCanaries();
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
        HashSet<IOrigin> res = new HashSet<>();
        // TODO: a more versatile filter infrastructure
        for (KeyspaceMetadata keyspace : cqlSession.getMetadata().getKeyspaces().values()
                .stream()
                .filter(ks -> ks.getName().asCql(true).matches(keyspaceFilter))
                .collect(Collectors.toList())
                ) {
            for (CqlIdentifier table : keyspace.getTables().keySet()
                    .stream()
                    .filter(table -> table.asCql(true).matches(tableFilter))
                    .collect(Collectors.toSet())
                    ) {
                res.add(new Origin(keyspace.getName().asCql(true), table.asCql(true)));
            }
        }
        return Set.copyOf(res);
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
        private String keyspaceFilter;
        private String tableFilter;

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
         * Setup builder with an ICanaryWriter to be used for storing selected canaries.
         * @param writer an instance of an ICanaryWriter
         * @return this builder instance
         */
        public Builder withCanaryWriter(final ICanaryWriter writer) {
            this.writer = writer;
            return this;
        }
        
        /**
         * Setup ICanarySelector to be built with a keyspace filter.
         * @param keyspaceFilter regex to filter keyspace names
         * @return this builder instance
         */
        public Builder withKeyspaceFilter(final String keyspaceFilter) {
            this.keyspaceFilter = keyspaceFilter;
            return this;
        }
        
        /**
         * Setup ICanarySelector to be built with a table filter.
         * @param tableFilter regex to filter table names
         * @return this builder instance
         */
        public Builder withTableFilter(final String tableFilter) {
            this.tableFilter = tableFilter;
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
                inst.setKeyspaceFilter(keyspaceFilter);
                inst.setTableFilter(tableFilter);
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