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

import io.github.rtib.cassandra.aviary.selector.AbstractSelector;
import io.github.rtib.cassandra.aviary.selector.ICanarySelector;
import io.github.rtib.cassandra.aviary.storage.AviaryWriter;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Implementation of select command.
 * 
 * @author repasi
 */
@Command(
        name = "select",
        description = """
                      Selection of canary entries from a Cassandra database.
                      """
)
public class Select extends AbstractConnectCommand {

    private static final Logger LOG = Logger.getLogger(Select.class.getName());
    
    @Option(
            names = "--selector",
            description = "Class name of the Selector to use."
    )
    @SuppressWarnings("FieldMayBeFinal")
    private String selectorClassName = "io.github.rtib.cassandra.aviary.selector.RangeSelector";
    
    @Option(
            names = {"-o", "--output"},
            description = "Output file to store the selected canaries."
    )
    @SuppressWarnings("FieldMayBeFinal")
    private File outFile = new File("aviary.json");
    
    @Override
    protected void execute() {
        try (var writer = new AviaryWriter(outFile)) {
            ICanarySelector selector = AbstractSelector.builder()
                    .forName(selectorClassName)
                    .withCqlSession(getCqlSession())
                    .withCanaryWriter(writer)
                    .build();
            selector.selectCanaries();
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Failed to initialize AviaryWriter.", ex);
            System.exit(-1);
        } catch (AbstractSelector.SelectorBuilderException ex) {
            LOG.log(Level.SEVERE, "Failed to initialize canary selector.", ex);
            System.exit(-1);
        }
    }
}
