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
        name = "list",
        description = """
                      List entries of an Aviary file and print them to the
                      standard output.
                      """
)
public class List extends AbstractCommand {

    @Option(
            names = {"-i", "--input"},
            description = "Input file store the canaries to be verify."
    )
    @SuppressWarnings("FieldMayBeFinal")
    private File inFile = new File("aviary.json");
    

    @Override
    protected void execute() {
        try {
            AviaryReader.getReader(inFile).forEach(System.out::println);
        } catch (IOException ex) {
            Logger.getLogger(List.class.getName()).log(Level.SEVERE, null, ex);
        }
            
    }
    
}
