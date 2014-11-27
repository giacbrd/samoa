package com.yahoo.labs.samoa.features.wordembedding.tasks;

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

import org.apache.commons.io.LineIterator;

import java.io.Reader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class SentenceIterator implements Iterator<List<String>> {

    private final LineIterator iterator;
    private String separator;

    public SentenceIterator(LineIterator iterator, String separator) {
        this.iterator = iterator;
        this.separator = separator;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    public List<String> next() {
        return Arrays.asList(iterator.next().trim().split(separator));
    }

    @Override
    public void remove() {
        iterator.remove();
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }
}
