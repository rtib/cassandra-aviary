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

import io.github.rtib.cassandra.aviary.storage.AviaryReader;
import io.github.rtib.cassandra.aviary.utils.OriginFilter;
import io.github.rtib.cassandra.aviary.utils.OriginFilterConverter;
import io.github.rtib.cassandra.aviary.verifier.AbstractVerifier;
import io.github.rtib.cassandra.aviary.verifier.ICanaryVerifier;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 *
 * @author repasi
 */
@Command(
        name = "verify",
        description = """
                      This command will read an Aviary file and verify the
                      stored canaries against the dataset they originate from.
                      """
)
public class Verify extends AbstractConnectCommand {

    private static final Logger LOG = Logger.getLogger(Verify.class.getName());

    @Option(
            names = "--verifier",
            description = "Class name of the Verifier to use."
    )
    @SuppressWarnings("FieldMayBeFinal")
    private String verifierClassName = "io.github.rtib.cassandra.aviary.verifier.SimpleVerifier";
    
    @Option(
            names = {"-f", "--filter"},
            description = """
                          Pattern of origins to exclude from processing. Note, that
                          a filter is denoted as "<keyspace filter>:<table filter>"
                          where both keyspace and table filters are regex, delimited
                          by a colon (:). Filter regex are compiled case insensitive.
                          Example: -f "test:.*"
                          """,
            converter = OriginFilterConverter.class
    )
    private OriginFilter[] filters;
    
    @Option(
            names = {"-i", "--input"},
            description = "Input file store the canaries to be verify."
    )
    @SuppressWarnings("FieldMayBeFinal")
    private File inFile = new File("aviary.json");
    
    @Override
    protected void execute() {
        try {
            ICanaryVerifier verifier = AbstractVerifier.builder()
                    .forName(verifierClassName)
                    .withAviaryReader(new AviaryReader(inFile))
                    .withCqlSession(getCqlSession())
                    .withOriginFilters(filters)
                    .build();
            verifier.verifyCanaries();
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Failed to initialize AviaryReader.", ex);
            System.exit(-1);
        } catch (AbstractVerifier.VerifierBuilderException ex) {
            LOG.log(Level.SEVERE, "Failed to initialize canary verifier.", ex);
            System.exit(-1);
        }
    }
    
}
