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
package io.github.rtib.cassandra.aviary.storage;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.rtib.cassandra.aviary.model.ICanary;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of ICanaryWriter storing canaries in an aviary. An
 * aviary is considered a file storing a stream of Canary objects serialized
 * as JSON. This writer is thread safe.
 * @author repasi
 */
public class AviaryWriter implements ICanaryWriter, Flushable, Closeable, AutoCloseable {

    private static final Logger LOG = Logger.getLogger(AviaryWriter.class.getName());

    private final BufferedOutputStream out;
    private final ObjectWriter writer;

    /**
     * Create of rewrite a file to which Canary objects can be written.
     * @param outputFile file to write
     * @throws IOException 
     */
    public AviaryWriter(File outputFile) throws IOException {
        writer = new ObjectMapper()
                .configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)
                .registerModule(new Jdk8Module())
                .writer();
        outputFile.createNewFile();
        out = new BufferedOutputStream(new FileOutputStream(outputFile));
    }
    
    @Override
    public synchronized void write(ICanary canary) {
        try {
            writer.writeValue(out, canary);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Failed to store canary", ex);
        }
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}
