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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.github.rtib.cassandra.aviary.model.IOrigin;
import io.github.rtib.cassandra.aviary.storage.Canary;
import io.github.rtib.cassandra.aviary.storage.Canary.IncompletePrimaryKeyException;
import io.github.rtib.cassandra.aviary.storage.ICanaryWriter;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Canary selector acquiring canaries from each token range of a Cassandra
 * cluster.
 * 
 * @author repasi
 */
public class RangeSelector extends AbstractSelector {

    private static final Logger LOG = Logger.getLogger(RangeSelector.class.getName());

    public RangeSelector(CqlSession cqlSession, ICanaryWriter writer) {
        super(cqlSession, writer);
    }

    @Override
    public void executeSelectCanaries() {
        Queue<Future<Result>> tasks = new ConcurrentLinkedQueue<>();
        Map<IOrigin, Counters> results = new ConcurrentHashMap<>();

        TokenMap tm = getTokenMap();
        int limit = 1;
        
        for (var origin : getOrigins()) {
            int ranges = 0;
            for (TokenRange range : tm.getTokenRanges()) {
                Callable<Result> task = () -> {
                    return selectCanaryForRange(origin, range, limit);
                };
                ranges++;
                tasks.add(executor.submit(task));
            }
            results.put(origin, new Counters(ranges, 0));
        }
        
        while (!tasks.isEmpty()) {
            tasks.stream()
                    .filter(f -> f.isDone())
                    .forEach((Future<Result> f) -> {
                        try {
                            results.get(f.get().origin()).addCanaries(f.get().count());
                        } catch (InterruptedException | ExecutionException ex) {
                            LOG.log(Level.SEVERE, null, ex);
                        } finally {
                            tasks.remove(f);
                        }
                    });
        }
        
        results.entrySet().stream()
                .map(e -> String.format("%s: %s", e.getKey(), e.getValue()))
                .forEach(System.out::println);
    }
    
    private Result selectCanaryForRange(IOrigin origin, TokenRange range, int limit) {
        var builder = Canary.builder()
                .withOrigin(origin)
                .withPrimaryKeyFields(helper.getPrimaryKey(origin));
        BoundStatement stmt = getStatement(origin).bind(range.getStart(), range.getEnd(), limit);
        LOG.fine(stmt.toString());
        ResultSet rs = cqlSession.execute(stmt);
        int count = 0;
        for (Row r : rs.all()) {
            count++;
            for (ColumnDefinition column : r.getColumnDefinitions()) {
                builder.withField(column.getName().asCql(true), r.getObject(column.getName()));
            }
            try {
                canaryWriter.write(builder.build());
            } catch (IncompletePrimaryKeyException ex) {
                LOG.log(Level.SEVERE, null, ex);
            }
            builder.resetFieldValues();
        }
        return new Result(origin, range, count);
    }

    @Override
    public PreparedStatement prepareStatementFor(IOrigin origin) {
        Select query = QueryBuilder
                .selectFrom(origin.getKeyspace(), origin.getTable())
                .columns(helper.getPrimaryKey(origin))
                .whereTokenFromIds(helper.getPartitionKey(origin))
                    .isGreaterThan(bindMarker())
                .whereTokenFromIds(helper.getPartitionKey(origin))
                    .isLessThanOrEqualTo(bindMarker())
                .limit(bindMarker());
        LOG.log(Level.FINE, "Preparing for {0} statement {1}", new Object[]{origin, query});
        return cqlSession.prepare(query.build());
    }

    public record Result(IOrigin origin, TokenRange range, int count) {};

    public final class Counters {
        private final int ranges;
        private int canaries;
        
        public Counters(int ranges, int canaries) {
            this.ranges = ranges;
            this.canaries = canaries;
        }
        
        public void addCanaries(int count) {
            this.canaries += count;
        }
        
        public int ranges() {
            return this.ranges;
        }
        
        public int canaries() {
            return this.canaries;
        }

        @Override
        public String toString() {
            return canaries + "/" + ranges;
        }
        
    };
}
