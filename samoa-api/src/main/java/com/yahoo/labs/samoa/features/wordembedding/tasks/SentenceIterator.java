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

import org.apache.commons.collections.ResettableIterator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class SentenceIterator implements ResettableIterator, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(SentenceIterator.class);
    private static final long serialVersionUID = -7994779092521326011L;
    private final File file;
    private final String codec;

    transient private LineIterator iterator = null;
    private String separator;

    public SentenceIterator(File file, String separator, String codec) {
        this.file = file;
        this.separator = separator;
        this.codec = codec;
        reset();
    }

    public SentenceIterator(SentenceIterator sentenceIterator) {
        this(sentenceIterator.file, sentenceIterator.separator, sentenceIterator.codec);
    }

    public void reset() {
        // Produce data stream from text file, each data sample (sentence) is a file line
        try {
            iterator = FileUtils.lineIterator(file, codec);
        } catch (java.io.IOException e) {
            logger.error("Error with file " + file.getPath());
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    public List<String> next() {
        // This "double" construction of the list is necessary for making Kryo works
        return new ArrayList<String>(Arrays.asList(iterator.next().trim().split(separator)));
    }

    @Override
    public void remove() {
        iterator.remove();
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }
}
