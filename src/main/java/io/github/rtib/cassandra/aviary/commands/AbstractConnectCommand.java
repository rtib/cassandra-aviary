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
package io.github.rtib.cassandra.aviary.commands;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import picocli.CommandLine.Option;

/**
 * Abstract super class of CLI commands that need to connect to a Cassandra.
 * @author repasi
 */
public abstract class AbstractConnectCommand extends AbstractCommand {

    private static final Logger LOG = Logger.getLogger(AbstractConnectCommand.class.getName());

    private final CqlSessionBuilder cqlSessionBuilder;
    private CqlSession cqlSession;
    private Properties properties;

    
    @Option(
            names = {"--contact-point"},
            description = "A Cassandra contact point to be used for connecting the cluster"
    )
    InetAddress[] contactPoint;
    
    @Option(
        names = {"--port"},
        description = "port to use when connecting Cassandra"
    ) int port = 9042;
    
    @Option(
        names = "--auth-user",
        description = "Add authentication credentials username to Cassandra connection properties"
    )
    String username;
    
    @Option(
        names = "--auth-password",
        description = "Prompt for authentication credential password",
        interactive = true
    )
    String password = "cassandra";
    
    @Option(
        names = "--local-dc",
        description = "Datacenter the Cassandra driver is to be connect to."
    )
    String localDc;
    
    public AbstractConnectCommand() {
        cqlSessionBuilder = CqlSession.builder();
        properties = new Properties();
        try {
            properties.load(this.getClass().getClassLoader().getResourceAsStream("project.properties"));
        } catch (IOException ex) {
            LOG.log(Level.WARNING, "Failed to load properties", ex);
        }
    }
    
    /**
     * Create a singleton cqlSession. A new session will be created if not exists
     * using the parameter got from command line options.
     * 
     * @return the CQL session instance
     */
    protected CqlSession getCqlSession() {
        if (cqlSession == null) {
            if (contactPoint != null)
                for (InetAddress cp : contactPoint)
                    cqlSessionBuilder.addContactPoint(new InetSocketAddress(cp, port));
            if (username != null)
                cqlSessionBuilder.withAuthCredentials(username, password);
            if (localDc != null)
                cqlSessionBuilder.withLocalDatacenter(localDc);
            cqlSessionBuilder
                    .withApplicationName(properties.getProperty("application-name"))
                    .withApplicationVersion(properties.getProperty("version"));
            cqlSession = cqlSessionBuilder.build();
        }
            
        return cqlSession;
    } 
}
