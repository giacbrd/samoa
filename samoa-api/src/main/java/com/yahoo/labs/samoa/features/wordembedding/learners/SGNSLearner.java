package com.yahoo.labs.samoa.features.wordembedding.learners;

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


import com.github.javacliparser.FloatOption;
import com.github.javacliparser.IntOption;
import org.jblas.DoubleMatrix;
import org.jblas.MatrixFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class SGNSLearner<T> implements Learner<T> {

    private static final Logger logger = LoggerFactory.getLogger(SGNSLearner.class);

    private Map<T, DoubleMatrix> syn0;
    private int layerSize = 200;
    private long seed = 1;
    private Map<T, DoubleMatrix> syn1neg;
    private double alpha = 0.025;
    private double minAlpha = 0.0001;

    public IntOption layerSizeOption = new IntOption("layerSize", 'l', "The number of columns of the model matrices.",
            layerSize);
    public FloatOption alphaOption = new FloatOption("alpha", 'a', "The initial learning rate value.", alpha);
    public FloatOption minAlphaOption = new FloatOption("minAlpha", 'm', "The minimal learning rate value.", minAlpha);

    public SGNSLearner(int layerSize, double alpha, double minAlpha) {
        init(layerSize, alpha, minAlpha, seed);
    }

    public SGNSLearner() {
        init(layerSize, alpha, minAlpha, seed);
    }

    @Override
    public boolean initConfiguration() {
        int newLayerSize = layerSizeOption.getValue();
        double newAlpha = alphaOption.getValue();
        double newMinAlpha = minAlphaOption.getValue();
        if (newLayerSize != layerSize || newAlpha != alpha || newMinAlpha != minAlpha) {
            init(newLayerSize, newAlpha, newMinAlpha, 1);
            return true;
        } else {
            return false;
        }
    }

    public void init(int layer1Size, double alpha, double minAlpha, long seed) {
        this.alpha = alpha;
        this.minAlpha = minAlpha;
        this.layerSize = layer1Size;
        this.seed = seed;
        syn0 = new HashMap<T, DoubleMatrix>(1000000);
        syn1neg = new HashMap<T, DoubleMatrix>(1000000);
    }


    public Map<T, DoubleMatrix> train(T item, T contextItem, List<T> negItems, Map<T, DoubleMatrix> externalRows) {
        Map<T, DoubleMatrix> gradients = new HashMap<>(externalRows.size());
        DoubleMatrix l1 = getRowGlobally(item, externalRows);
        DoubleMatrix l2b = new DoubleMatrix(negItems.size()+1, layerSize);
        l2b.putRow(0, getContextRowGlobally(contextItem, externalRows));
        for (int i = 1; i < l2b.rows; i++) {
            l2b.putRow(i, getContextRowGlobally(negItems.get(i - 1), externalRows));
        }
        DoubleMatrix labels = DoubleMatrix.zeros(l2b.rows);
        labels.put(0, 1.0);
        // Compute the outputs of the model, for the true context and the other negatives one (propagate hidden -> output)
        DoubleMatrix fb = MatrixFunctions.expi(l2b.mmul(l1).negi());
        for (int i = 0; i < fb.length; i++) {
            fb.put(i, 1.0 / (1.0 + fb.get(i)));
        }
        // Partial computation of the gradient (it misses the multiplication by the input vector)
        DoubleMatrix gb = (labels.sub(fb)).muli(alpha); // vector of error gradients multiplied by the learning rate
//        logger.info(item + " " + contextItem);
//        logger.info(l1.toString() + "\n" + l2b.getRow(0));
//        logger.info(alpha+ " " + gb);
        // Now update matrices X and C
        // Learn C
        if (externalRows.containsKey(contextItem)) {
            gradients.put(contextItem, l1.mul(gb.get(0)));
        } else {
            syn1neg.put(contextItem, l2b.getRow(0).add(l1.mul(gb.get(0))));
        }
        ListIterator<T> negItemsIter = negItems.listIterator();
        for (int i = 1; i < l2b.rows; i++) {
            T wordNeg = negItemsIter.next();
            if (externalRows.containsKey(wordNeg)) {
                gradients.put(wordNeg, l1.mul(gb.get(i)));
            } else {
                syn1neg.put(wordNeg, l2b.getRow(i).add(l1.mul(gb.get(i))));
            }
        }
        // Gradient error for learning W
        DoubleMatrix neu1e = gb.transpose().mmul(l2b);
        if (externalRows.containsKey(item)) {
            gradients.put(item, neu1e);
        } else {
            //FIXME is it necessary to put back l1? for now yes
            syn0.put(item, l1.addi(neu1e));
        }
        //logger.info(syn0.get(item) + "\n" + syn1neg.get(contextItem));
        return gradients;
    }

    private DoubleMatrix getContextRowGlobally(T contextItem,  Map<T, DoubleMatrix> externalRows) {
        if (externalRows.containsKey(contextItem)) {
            return externalRows.get(contextItem);
        } else {
            return getContextRowRef(contextItem);
        }
    }

    private DoubleMatrix getRowGlobally(T item, Map<T, DoubleMatrix> externalRows) {
        if (externalRows.containsKey(item)) {
            return externalRows.get(item);
        } else {
            return getRowRef(item);
        }
    }

    public DoubleMatrix getRowRef(T item) {
        DoubleMatrix row = syn0.get(item);
        if (row == null) {
            org.jblas.util.Random.seed((long) (item + Long.toString(seed)).hashCode());
            row = DoubleMatrix.rand(layerSize).subi(0.5).divi(layerSize);
            syn0.put(item, row);
        }
        return row;
    }

    public DoubleMatrix getContextRowRef(T item) {
        DoubleMatrix row = syn1neg.get(item);
        if (row == null) {
            row = DoubleMatrix.zeros(layerSize);
            syn1neg.put(item, row);
        }
        return row;
    }

    @Override
    public double getAlpha() {
        return alpha;
    }

    @Override
    public double getMinAlpha() {
        return minAlpha;
    }

    @Override
    public int columns() {
        return layerSize;
    }

    @Override
    public long rows() {
        return syn0.size();
    }

    @Override
    public DoubleMatrix getRow(T item) {
        return getRowRef(item).dup();
    }

    @Override
    public DoubleMatrix getContextRow(T item) {
        return getContextRowRef(item).dup();
    }

    @Override
    public void updateRow(T item, DoubleMatrix update) {
        syn0.get(item).addi(update);
    }

    @Override
    public void updateContextRow(T item, DoubleMatrix update) {
        syn1neg.get(item).addi(update);
    }

    @Override
    public boolean contains(T item) {
        return syn0.containsKey(item);
    }

    @Override
    public void setSeed(long seed) {
        this.seed = seed;
    }

    @Override
    public Learner copy() {
        SGNSLearner<T> l = new SGNSLearner<T>(layerSize, alpha, minAlpha);
        l.alphaOption = (FloatOption) alphaOption.copy();
        l.minAlphaOption = (FloatOption) minAlphaOption.copy();
        l.layerSizeOption = (IntOption) layerSizeOption.copy();
        l.seed = seed;
        l.syn0 = new HashMap<>(syn0.size());
        for (T item: l.syn0.keySet()) {
            l.syn0.put(item, syn0.get(item).dup());
        }
        l.syn1neg = new HashMap<>(syn1neg.size());
        for (T item: l.syn0.keySet()) {
            l.syn0.put(item, syn0.get(item).dup());
        }
        return l;
    }
}