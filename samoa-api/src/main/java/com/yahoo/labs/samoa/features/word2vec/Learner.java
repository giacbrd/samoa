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

import com.google.common.primitives.Ints;
import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.topology.Stream;
import org.jblas.DoubleMatrix;
import org.jblas.MatrixFunctions;
import org.jblas.util.*;
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
    // FIXME substitute double[] with DoubleMatrix, BE AWARE OF THE BUG
    private HashMap<String, double[]> syn0;
    private int layer1Size;
    private  HashMap<String, double[]> syn1neg;
    double alpha;
    private double minAlpha;
    private double wordCount;

    public Learner(double alpha, double minAlpha, int layer1Size) {
        this.alpha = alpha;
        this.minAlpha = minAlpha;
        this.layer1Size = layer1Size;
    }

    public Learner() {
        this(0.025, 0.0001, 100);
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        syn0 = new HashMap<String, double[]>(1000000);
        syn1neg = new HashMap<String, double[]>(1000000);
        wordCount = 0.0;
    }

    @Override
    public boolean process(ContentEvent event) {
        if (event.isLastEvent()) {
            logger.info(String.format("Learner-%d: finished at approx %d words",
                    id, (int) wordCount));
            outputStream.put(new ModelUpdateEvent(null, null, true));
            return true;
        }
        WordPairEvent wordPair = (WordPairEvent) event;
        String word = wordPair.getWord();
        DoubleMatrix WRow = trainPair(word, wordPair.getWordC(), wordPair.getWordsNeg());
        outputStream.put(new ModelUpdateEvent(word, WRow, false));
        return true;
    }

    private DoubleMatrix trainPair(String word, String wordC, List<String> wordsNeg) {
        wordCount+=1.0/(6.0*11.0); //FIXME HARDCODED!
        // Get the word vectors from matrices
        double[] l1Array = syn0.get(word);
        DoubleMatrix l1 = null;
        if (l1Array == null) {
            org.jblas.util.Random.seed((long) (word + Integer.toString(1)).hashCode()); //FIXME seed HARDCODED!
            l1 = DoubleMatrix.rand(layer1Size).subi(0.5).divi(layer1Size);
            syn0.put(word, l1.toArray());
        } else {
            l1 = new DoubleMatrix(l1Array);
        }
        // Matrix of vectors of contexts, the first is the "true" one
        DoubleMatrix l2b = new DoubleMatrix(wordsNeg.size()+1, layer1Size);
        l2b.putRow(0, getCRow(wordC));
        for (int i = 1; i < l2b.rows; i++) {
            l2b.putRow(i, getCRow(wordsNeg.get(i-1)));
        }
        DoubleMatrix labels = DoubleMatrix.zeros(l2b.rows);
        labels.put(0, 1.0);
        // Compute the outputs of the model, for the true context and the other negatives one (propagate hidden -> output)
        DoubleMatrix fb = MatrixFunctions.expi(l2b.mmul(l1).negi());
        for (int i = 0; i < fb.length; i++) {
            fb.put(i, 1.0 / (1.0 + fb.get(i)));
        }
        // Partial computation of the gradient (it misses the multiplication by the input vector)
        //double alpha = Math.max(minAlpha, this.alpha * (1 - ((double) wordCount / 17005207))); //FIXME HARDCODED!
        // Partial computation of the gradient (it misses the multiplication by the input vector)
        DoubleMatrix gb = (labels.sub(fb)).muli(alpha); // vector of error gradients multiplied by the learning rate
        //logger.info(word + " " + wordC);
        //logger.info(WRow.toString() + "\n" + CRow.toString());
        //logger.info(""+alpha+ " " + gb);
        // Now update matrices X and C
        // Learn C
        ListIterator<String> wordsNegIter = wordsNeg.listIterator();
        syn1neg.put(wordC, l2b.getRow(0).add(l1.mul(gb.get(0))).toArray());
        for (int i = 1; i < l2b.rows; i++) {
            String wordNeg = wordsNegIter.next();
            syn1neg.put(wordNeg, l2b.getRow(i).add(l1.mul(gb.get(i))).toArray());
        }
        // Gradient error for learning W
        DoubleMatrix neu1e = gb.transpose().mmul(l2b);
        //FIXME not necessary to put back l1? for now yes
        syn0.put(word, l1.addi(neu1e).toArray());
        //logger.info(WRow.toString() + "\n" + CRow.toString());
        if (wordCount % 100000 == 0) {
            logger.info(String.format("Learner-%d: at %d words (%.3f of total words), alpha %.5f",
                    id, (int) wordCount, (double) wordCount / 17005207, alpha));
        }
        return l1;
    }

    private DoubleMatrix getCRow(String word) {
        double[] CRowArray = syn1neg.get(word);
        DoubleMatrix CRow = null;
        if (CRowArray == null) {
            CRow = DoubleMatrix.zeros(layer1Size);
            syn1neg.put(word, CRow.toArray());
        } else {
            CRow = new DoubleMatrix(CRowArray);
        }
        return CRow;
    }

    @Override
    public Processor newProcessor(Processor processor) {
        Learner p = (Learner) processor;
        Learner l = new Learner(p.alpha, p.minAlpha, p.layer1Size);
        l.outputStream = p.outputStream;
        l.syn0 = p.syn0;
        l.syn1neg = p.syn1neg;
        l.wordCount = p.wordCount;
        return l;
    }

    public void setOutputStream(Stream outputStream) {
        this.outputStream = outputStream;
    }
}
