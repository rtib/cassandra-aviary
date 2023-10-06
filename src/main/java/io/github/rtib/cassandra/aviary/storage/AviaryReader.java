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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rtib.cassandra.aviary.model.ICanary;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A reader class enable to read and iterate over Canary instances from an
 * aviary. Where an aviary is considered a file containing a stream of JSON
 * serialized Canary objects. Such an aviary might be created by AviaryWriter.
 * @see AviaryWriter
 * @author repasi
 */
public final class AviaryReader implements Iterable<ICanary> {
    
    private final ObjectMapper mapper;
    private final File aviaryFile;

    /**
     * Construct a reader object. The aviaryFile is a JSON file containing a
     * stream of ICanary objects.
     * @param aviaryFile the file to be read
     * @throws IOException 
     */
    public AviaryReader(final File aviaryFile) throws IOException {
        this.mapper = new ObjectMapper();
        this.aviaryFile = aviaryFile;
    }
    
    /**
     * Static method to get an AviaryReader instance directly casted to an
     * Iterable.
     * @param aviaryFile the file to be read
     * @return the Iterable interface of an AviaryReader instance associated to the given file
     * @throws IOException 
     */
    public static Iterable<ICanary> getReader(final File aviaryFile) throws IOException {
        return new AviaryReader(aviaryFile);
    }

    @Override
    public Iterator<ICanary> iterator() {
        try {
            var parser = mapper.getFactory().createParser(aviaryFile)
                    .configure(JsonParser.Feature.ALLOW_COMMENTS, true);
            return new CanaryIterator(mapper, parser);
        } catch (IOException ex) {
            Logger.getLogger(AviaryReader.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    private static class CanaryIterator implements Iterator<ICanary> {

        private final JsonParser parser;
        private final ObjectMapper mapper;

        public CanaryIterator(ObjectMapper mapper, JsonParser parser) throws IOException {
            this.mapper = mapper;
            this.parser = parser;
            this.parser.nextToken();
        }

        @Override
        public boolean hasNext() {
            return parser.hasCurrentToken();
        }

        @Override
        public ICanary next() {
            ICanary canary = null;
            try {
                canary = mapper.readValue(parser, Canary.class);
                parser.nextToken();
            } catch (IOException ex) {
                Logger.getLogger(AviaryReader.class.getName()).log(Level.SEVERE, null, ex);
            }
            return canary;
        }
    }
}
