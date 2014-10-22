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

import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.EntranceProcessor;
import com.yahoo.labs.samoa.core.Processor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class Word2vecProcessor  implements EntranceProcessor {

    private static final Logger logger = LoggerFactory.getLogger(Word2vecProcessor.class);
    private boolean save_model;
    private  Word2vec w2v;
    private File file;
    private int id;
    private boolean isFinished;

    public Word2vecProcessor(File file, boolean save_model) {
        this.file = file;
        this.save_model = save_model;
    }

    public Word2vecProcessor(File file, Word2vec w2v) {
        this.file = file;
        this.w2v = w2v;
    }

    @Override
    public boolean process(ContentEvent event) {
        return false;
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        isFinished = false;
        SentencesIterable sit = new SentencesIterable(file);
        logger.info("Learning...");
        w2v = new Word2vec(sit, 200, 0.025, (short)5, (short)5, 0.0, 1, 0.0001, true, false, (short)10, false);
        logger.info("Learning completed!\nTesting...");
        ArrayList<ImmutablePair<String, Double>> results = w2v.most_similar(new ArrayList<String>(Arrays.asList(new String[]{"anarchism"})), new ArrayList<String>(), 10);
        for (ImmutablePair<String, Double> p: results) {
            logger.info(p.getLeft() + " " + Double.toString(p.getRight()));
        }
        if (save_model) {
            try {
                w2v.save(new File("word2vec_model_" + this.id + "_" + this.hashCode()));
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        isFinished = true;
    }

    @Override
    public Processor newProcessor(Processor processor) {
        Word2vecProcessor p = (Word2vecProcessor) processor;
        return new Word2vecProcessor(p.file, p.w2v);
    }

    @Override
    public boolean isFinished() {
        return isFinished;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public ContentEvent nextEvent() {
        return null;
    }


}
