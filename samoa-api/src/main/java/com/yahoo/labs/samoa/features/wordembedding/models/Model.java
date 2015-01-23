package com.yahoo.labs.samoa.features.wordembedding.models;

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
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.features.wordembedding.tasks.OneContentEvent;
import com.yahoo.labs.samoa.topology.Stream;
import org.apache.commons.lang3.tuple.MutablePair;
import org.jblas.DoubleMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.List;

import static org.jblas.Geometry.normalize;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class Model<T> implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(Model.class);
    private static final long serialVersionUID = -1427330729767682263L;

    private Stream outputStream;
    private int id;
    private HashMap<T, MutablePair<DoubleMatrix, Long>> syn0norm;
    private File outPath;
    private int lastEventCount = 0;
    private int learnerCount;

    public Model(int learnerCount) {
        this.learnerCount = learnerCount;
        this.outPath = null;
    }

    public Model(int learnerCount, File outPath) {
        this.learnerCount = learnerCount;
        this.outPath = outPath;
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        syn0norm = new HashMap<>(1000000);
    }

    @Override
    public boolean process(ContentEvent event) {
        if (event.isLastEvent()) {
            lastEventCount++;
            if (lastEventCount >= learnerCount) {
                try {
                    if (outPath != null) {
                        if (!outPath.isFile()) {
                            outPath.mkdirs();
                        } else {
                            throw new IOException("Model path is an existing file.");
                        }
                        FileOutputStream fos = new FileOutputStream(outPath.getAbsolutePath() + File.separator + "syn0norm");
                        ObjectOutputStream oos = new ObjectOutputStream(fos);
                        oos.writeObject(syn0norm);
                        oos.close();
                        fos.close();
                        logger.info("Model written in " + outPath.getAbsolutePath());
                        logger.info("Exit!");
                        System.exit(0);
                    } else {
                        throw new IOException("Model path is not set.");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }
            }
            //outputStream.put(new ModelUpdateEvent(null, null, true));
            return true;
        }
        if (event instanceof ModelUpdateEvent) {
            ModelUpdateEvent newRow = (ModelUpdateEvent) event;
            T word = (T) newRow.getWord();
            MutablePair<DoubleMatrix, Long> wordInfo = syn0norm.get(word);
            if (wordInfo == null) {
                wordInfo = new MutablePair<DoubleMatrix, Long>(normalize(newRow.getRow()), (long) 0);
                syn0norm.put(word, wordInfo);
            } else {
                wordInfo.setLeft(normalize(newRow.getRow()));
            }
        } else if (event instanceof OneContentEvent) {
            OneContentEvent sentence = (OneContentEvent) event;
            for (T word: (List<T>) sentence.getContent()) {
                MutablePair<DoubleMatrix, Long> wordInfo = syn0norm.get(word);
                if (wordInfo == null) {
                    wordInfo = new MutablePair<DoubleMatrix, Long>(null, (long) 1);
                    syn0norm.put(word, wordInfo);
                } else {
                    wordInfo.setRight(wordInfo.getRight()+1);
                }
            }
        } else {
            return false;
        }
        return true;
    }

    @Override
    public Processor newProcessor(Processor processor) {
        Model p = (Model) processor;
        Model m = new Model(p.learnerCount);
        m.lastEventCount = p.lastEventCount;
        m.outputStream = p.outputStream;
        m.syn0norm = p.syn0norm;
        m.outPath = p.outPath;
        return m;
    }

    public void setOutputStream(Stream outputStream) {
        this.outputStream = outputStream;
    }
}
