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
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.topology.Stream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jblas.DoubleMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;

import static org.jblas.Geometry.normalize;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class Model implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(Model.class);
    private Stream outputStream;
    private int id;
    private HashMap<String, ImmutablePair<DoubleMatrix, Long>> syn0norm;
    private File outPath;

    public Model() {
        this.outPath = null;
    }

    public Model(File outPath) {
        this.outPath = outPath;
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        syn0norm = new HashMap<String, ImmutablePair<DoubleMatrix, Long>>(1000000);
    }

    @Override
    public boolean process(ContentEvent event) {
        if (event.isLastEvent()) {
            if (outPath != null) {
                try {
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
                } catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }
            }
            //outputStream.put(new ModelUpdateEvent(null, null, true));
            return true;
        }
        ModelUpdateEvent newRow = (ModelUpdateEvent) event;
        String word = newRow.getWord();
        long count = 0;
        if (syn0norm.containsKey(word)) {
            count = syn0norm.get(word).getRight();
        }
        syn0norm.put(word, new ImmutablePair<DoubleMatrix, Long>(normalize(newRow.getRow()), count+1));
        return true;
    }

    @Override
    public Processor newProcessor(Processor processor) {
        Model p = (Model) processor;
        Model m = new Model();
        m.outputStream = p.outputStream;
        m.syn0norm = p.syn0norm;
        m.outPath = p.outPath;
        return m;
    }

    public void setOutputStream(Stream outputStream) {
        this.outputStream = outputStream;
    }
}
