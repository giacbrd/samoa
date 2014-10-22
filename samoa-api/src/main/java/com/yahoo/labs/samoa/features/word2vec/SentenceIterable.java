package com.yahoo.labs.samoa.features.word2vec;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2013 - 2014 Yahoo! Inc.
 * %%
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
 * #L%
 */

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
 class SentencesIterable implements Iterable<List<String>> {

    private File file;

    public SentencesIterable(File file) {
        this.file = file;
    }

    @Override
    public Iterator<List<String>> iterator() {
        return new SentencesIterator(this.file);
    }

    private class SentencesIterator implements Iterator<List<String>> {

        private LineIterator it;

        public SentencesIterator(File file) {
            try {
                it = FileUtils.lineIterator(file, "UTF-8");
            } catch (java.io.IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        @Override
        public boolean hasNext() {
            return this.it.hasNext();
        }

        @Override
        public List<String> next() {
            return Arrays.asList(this.it.next().trim().split(" "));
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}