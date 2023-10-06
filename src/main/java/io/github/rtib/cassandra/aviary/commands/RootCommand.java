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

import java.io.IOException;
import static java.lang.System.out;
import java.lang.invoke.MethodHandles;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ScopeType;
import picocli.CommandLine.Spec;

/**
 * Implementation of root CLI command.
 * 
 * @author repasi
 */
@Command(
        name = "aviary",
        description = "Tool for selecting and maintaining canary data from a Cassandra dataset.",
        versionProvider = RootCommand.PropertiesVersionProvider.class,
        mixinStandardHelpOptions = true,
        subcommands = {
            CommandLine.HelpCommand.class,
            Select.class,
            List.class,
            Verify.class
        }
)
public class RootCommand extends AbstractCommand {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Spec
    CommandSpec spec;
    
    @Option(
            names = {"-v", "--verbose"},
            description = "Verbose logging.",
            scope = ScopeType.INHERIT
    )
    public void setVerbosity(boolean[] verbose) {
        ch.qos.logback.classic.Logger logger =
                ((ch.qos.logback.classic.LoggerContext)LoggerFactory.getILoggerFactory())
                .getLogger(Logger.ROOT_LOGGER_NAME);
        if (verbose.length > 1) {
            logger.setLevel(ch.qos.logback.classic.Level.DEBUG);
            LOGGER.debug("Debug logging enabled.");
        } else if (verbose.length > 0) {
            logger.setLevel(ch.qos.logback.classic.Level.INFO);
            LOGGER.info("Verbose logging enabled.");
        }
    }
    
    @Override
    protected void execute() {
        LOGGER.debug("Print usage.");
        spec.commandLine().usage(out);
    }
    
    /**
     * Helper class to implement version string provider.
     */
    public static class PropertiesVersionProvider implements CommandLine.IVersionProvider {
        @Override
        public String[] getVersion() throws Exception {
            Properties properties = new Properties();
            LOGGER.info("Loading properties.");
            try {
                properties.load(this.getClass().getClassLoader().getResourceAsStream("project.properties"));
            } catch (IOException ex) {
                LOGGER.error("Failed to load properties", ex);
            }
            return new String[] {
                properties.getProperty("application-name") + " version " + properties.getProperty("version"),
            };
        }        
    }
}
