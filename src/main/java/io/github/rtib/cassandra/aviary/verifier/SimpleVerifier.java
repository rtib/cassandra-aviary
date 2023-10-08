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
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.github.rtib.cassandra.aviary.model.ICanary;
import io.github.rtib.cassandra.aviary.model.IOrigin;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 *
 * @author repasi
 */
public class SimpleVerifier extends AbstractVerifier {

    private static final Logger LOG = Logger.getLogger(SimpleVerifier.class.getName());
    private Predicate<IOrigin> originFilter;

    public SimpleVerifier(CqlSession session, Iterable<ICanary> reader, ExecutorService executor) {
        super(session, reader, executor);
    }

    @Override
    public void setOriginFilter(Predicate<IOrigin> filter) {
        this.originFilter = filter;
    }

    @Override
    public void verifyCanaries() {
        // Create callable tasks for each canary, submit them and store their Futures.
        Queue<Future<Verified>> tasks = new ConcurrentLinkedQueue<>();
        for (var canary : reader) {
            if (originFilter.test(canary.getOrigin())) {
                Callable<Verified> task = () -> {
                    return verifyCanary(canary);
                };
                tasks.add(executor.submit(task));
            }
        }
        
        // Process the Futures and sum up the results by origin
        Map<IOrigin, Counters> results = new ConcurrentHashMap<>();
        while (!tasks.isEmpty()) {
            tasks.stream()
                    .filter(f -> f.isDone())
                    .forEach((Future<Verified> f) -> {
                        try {
                            var origin = f.get().canary().getOrigin();
                            if (!results.containsKey(origin)) {
                                results.put(origin, new Counters());
                            }
                            results.get(origin).add(f.get().exists());
                        } catch (InterruptedException | ExecutionException ex) {
                            LOG.log(Level.SEVERE, "Failed to process results of: " + f, ex);
                        } finally {
                            tasks.remove(f);
                        }
                    });
        }
        
        // Print results
        results.entrySet().stream()
                .map(e -> String.format("%s: %s", e.getKey(), e.getValue()))
                .forEach(System.out::println);
    }
    
    /**
     * Verify the existence of a canary.
     * @param canary the canary to be verify
     * @return a Verified record holding the canary and the result of its existence check
     */
    private Verified verifyCanary(ICanary canary) {
        BoundStatement query;
        query = getStatement(canary.getOrigin())
                .bind(
                        helper.getPrimaryKey(canary.getOrigin()).stream()
                                .map(pkField -> canary.getIdentifier().get(pkField))
                                .collect(Collectors.toList())
                                .toArray()
                );
        ResultSet res = cqlSession.execute(query);
        return new Verified(canary, !res.all().isEmpty());
    }

    @Override
    public PreparedStatement prepareStatementFor(IOrigin key) {
        Select query = QueryBuilder
                .selectFrom(key.getKeyspace(), key.getTable())
                .columns(helper.getPrimaryKey(key));
        for (var pkField : helper.getPrimaryKey(key))
            query = query
                    .whereColumn(pkField)
                    .isEqualTo(bindMarker());
                    
        LOG.log(Level.FINE, "Preparing for {0} statement {1}", new Object[]{key, query});
        return cqlSession.prepare(query.build());
    }
    
    /**
     * Count the total number and the success of canary verifications.
     */
    public final class Counters {
        private int verified;
        private int total;
        
        public Counters() {
            this(0, 0);
        }
        
        public Counters(int verified, int total) {
            this.verified = verified;
            this.total = total;
        }
        
        public void add(boolean verified) {
            this.verified += (verified ? 1 : 0);
            this.total++;
        }
        
        public void add(Counters other) {
            this.verified += other.verified;
            this.total += other.total;
        }
        
        public int verified() {
            return this.verified;
        }
        
        public int total() {
            return this.total;
        }
        
        @Override
        public String toString() {
            return verified + "/" + total;
        }
    };
    
    /**
     * Represents the result of a canary verification.
     */
    public record Verified(ICanary canary, boolean exists) {};
}
