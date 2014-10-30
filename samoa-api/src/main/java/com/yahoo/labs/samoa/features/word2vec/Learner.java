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
import org.jblas.DoubleMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class Learner implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(Learner.class);
    private Stream outputStream;
    private int id;
    private HashMap<String, DoubleMatrix> syn0;
    private int layer1Size;
    private  HashMap<String, DoubleMatrix> syn1neg;
    double alpha;

    public Learner(double alpha, int layer1Size) {
        this.alpha = alpha;
        this.layer1Size = layer1Size;
    }

    public Learner() {
        this(0.025, 100);
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        syn0 = new HashMap<String, DoubleMatrix>(1000000);
        syn1neg = new HashMap<String, DoubleMatrix>(1000000);
    }

    @Override
    public boolean process(ContentEvent event) {
        if (event.isLastEvent()) {
            outputStream.put(new ModelUpdateEvent(null, null, true));
            return true;
        }
        WordPairEvent wordPair = (WordPairEvent) event;
        String word = wordPair.getWord();
        DoubleMatrix WRow = trainPair(word, wordPair.getWordC(), wordPair.getLabel());
        outputStream.put(new ModelUpdateEvent(word, WRow, false));
        return true;
    }

    private DoubleMatrix trainPair(String word, String wordC, boolean label) {
        // Get the word vectors from matrices
        DoubleMatrix WRow = syn0.get(word);
        if (WRow == null) {
            WRow = DoubleMatrix.rand(layer1Size).subi(0.5).divi(layer1Size);
            syn0.put(word, WRow);
        }
        DoubleMatrix CRow = syn1neg.get(wordC);
        if (CRow == null) {
            CRow = DoubleMatrix.zeros(layer1Size);
            syn1neg.put(wordC, CRow);
        }
        // Partial computation of the gradient (it misses the multiplication by the input vector)
        Double gb = alpha * ((label ? 1.0 : 0.0) - 1.0 / (1.0 + Math.exp(-CRow.dot(WRow))));
        // Learn syn1neg
        CRow.addi(WRow.mul(gb));
        // Learn syn0
        WRow.addi(CRow.mul(gb));
        return WRow;
    }

    @Override
    public Processor newProcessor(Processor processor) {
        Learner p = (Learner) processor;
        Learner l = new Learner(p.alpha, p.layer1Size);
        l.outputStream = p.outputStream;
        l.syn0 = p.syn0;
        l.syn1neg = p.syn1neg;
        return l;
    }

    public void setOutputStream(Stream outputStream) {
        this.outputStream = outputStream;
    }
}
