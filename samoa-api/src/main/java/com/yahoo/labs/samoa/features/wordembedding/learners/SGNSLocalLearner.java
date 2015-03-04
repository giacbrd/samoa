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
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.jblas.DoubleMatrix;
import org.jblas.MatrixFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
//FIXME subclass SGNSLocalModel
public class SGNSLocalLearner<T> implements Model<T> {

    private static final Logger logger = LoggerFactory.getLogger(SGNSLocalLearner.class);
    private static final long serialVersionUID = 8581722962299330045L;

    private ConcurrentHashMap<T, DoubleMatrix> syn0;
    private int layerSize = 200;
    private long seed = 1;
    private ConcurrentHashMap<T, DoubleMatrix> syn1neg;
    private double alpha = 0.025;
    private double minAlpha = 0.0001;
    private ConcurrentHashMap<Long, Map<T, MutablePair<DoubleMatrix, DoubleMatrix>>> externalRows;
    private ConcurrentHashMap<Long, Map<T, MutablePair<DoubleMatrix, DoubleMatrix>>> gradients;

    public IntOption layerSizeOption = new IntOption("layerSize", 'l', "The number of columns of the model matrices.",
            layerSize);
    public FloatOption alphaOption = new FloatOption("alpha", 'a', "The initial learning rate value.", alpha);
    public FloatOption minAlphaOption = new FloatOption("minAlpha", 'm', "The minimal learning rate value.", minAlpha);
    private boolean firstInit = true;

    public SGNSLocalLearner(int layerSize, double alpha, double minAlpha) {
        init(layerSize, alpha, minAlpha, seed);
    }

    public SGNSLocalLearner() {
        init(layerSize, alpha, minAlpha, seed);
    }

    @Override
    public boolean initConfiguration() {
        int newLayerSize = layerSizeOption.getValue();
        double newAlpha = alphaOption.getValue();
        double newMinAlpha = minAlphaOption.getValue();
        if (firstInit || newLayerSize != layerSize || newAlpha != alpha || newMinAlpha != minAlpha) {
            init(newLayerSize, newAlpha, newMinAlpha, 1);
            firstInit = false;
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
        syn0 = new ConcurrentHashMap<T, DoubleMatrix>(1000000);
        syn1neg = new ConcurrentHashMap<T, DoubleMatrix>(1000000);
        externalRows = new ConcurrentHashMap<>();
        gradients = new ConcurrentHashMap<>();
    }


    public Map<T, MutablePair<DoubleMatrix, DoubleMatrix>> train(long dataID, T item, T contextItem, List<T> negItems) {
        DoubleMatrix l1 = getRowGlobally(dataID, item);
        //logger.info("_ "+item +" "+getRowGlobally(item));
        DoubleMatrix l2b = new DoubleMatrix(negItems.size()+1, layerSize);
        //logger.info("c "+contextItem +" "+getContextRowGlobally(contextItem));
        l2b.putRow(0, getContextRowGlobally(dataID, contextItem));
        for (int i = 1; i < l2b.rows; i++) {
            //logger.info("N " + negItems.get(i - 1) +" "+getContextRowGlobally(negItems.get(i - 1)));
            l2b.putRow(i, getContextRowGlobally(dataID, negItems.get(i - 1)));
        }
        //FIXME precompute this
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
        // Now update matrices W of words and C of contexts
        // Learn C
        DoubleMatrix contextGradient = l1.mul(gb.get(0));
        DoubleMatrix newContextRow = l2b.getRow(0).add(contextGradient);
        if (gradients.get(dataID).containsKey(contextItem)) {
            addContextGradient(dataID, contextItem, contextGradient);
        }
        setContextRowGlobally(dataID, contextItem, newContextRow);
        ListIterator<T> negItemsIter = negItems.listIterator();
        for (int i = 1; i < l2b.rows; i++) {
            T negItem = negItemsIter.next();
            DoubleMatrix negGradient = l1.mul(gb.get(i));
            DoubleMatrix newNegRow = l2b.getRow(i).add(negGradient);
            if (gradients.get(dataID).containsKey(negItem)) {
                addContextGradient(dataID, negItem, negGradient);
            }
            setContextRowGlobally(dataID, negItem, newNegRow);
        }
        // Gradient error for learning W
        DoubleMatrix neu1e = gb.transpose().mmul(l2b);
        DoubleMatrix newRow = l1.addi(neu1e);
        if (gradients.get(dataID).containsKey(item)) {
            addGradient(dataID, item, neu1e);
        }
        //FIXME is it necessary to put back l1? for now yes
        setRowGlobally(dataID, item, newRow);
        //logger.info(syn0.get(item) + "\n" + syn1neg.get(contextItem));
        return gradients.get(dataID);
    }

    //FIXME optimize redundancy!

    private void addGradient(long dataID, T item, DoubleMatrix gradient) {
        //FIXME gradients must contain item!
        Map<T, MutablePair<DoubleMatrix, DoubleMatrix>> tempGrads = gradients.get(dataID);
        if (tempGrads.get(item).getLeft() != null) {
            tempGrads.get(item).getLeft().addi(gradient);
        } else {
            tempGrads.get(item).setLeft(gradient);
        }
    }

    private void addContextGradient(long dataID, T item, DoubleMatrix gradient) {
        //FIXME gradients must contain item!
        Map<T, MutablePair<DoubleMatrix, DoubleMatrix>> tempGrads = gradients.get(dataID);
        if (tempGrads.get(item).getRight() != null) {
            tempGrads.get(item).getRight().addi(gradient);
        } else {
            tempGrads.get(item).setRight(gradient);
        }
    }

    private void setContextRowGlobally(long dataID, T contextItem, DoubleMatrix contextRow) {
        Map<T, MutablePair<DoubleMatrix, DoubleMatrix>> tempExtRows = externalRows.get(dataID);
        if (tempExtRows.containsKey(contextItem)) {
            tempExtRows.get(contextItem).setRight(contextRow);
        } else {
            syn1neg.put(contextItem, contextRow);
        }
    }

    private void setRowGlobally(long dataID, T item, DoubleMatrix row) {
        Map<T, MutablePair<DoubleMatrix, DoubleMatrix>> tempExtRows = externalRows.get(dataID);
        if (tempExtRows.containsKey(item)) {
            tempExtRows.get(item).setLeft(row);
        } else {
            syn0.put(item, row);
        }
    }

    private DoubleMatrix getContextRowGlobally(long dataID, T contextItem) {
        Map<T, MutablePair<DoubleMatrix, DoubleMatrix>> tempExtRows = externalRows.get(dataID);
        if (tempExtRows.containsKey(contextItem) &&
                tempExtRows.get(contextItem).getRight() != null) {
            return tempExtRows.get(contextItem).getRight();
        } else {
            return getContextRowRef(contextItem);
        }
    }

    private DoubleMatrix getRowGlobally(long dataID, T item) {
        Map<T, MutablePair<DoubleMatrix, DoubleMatrix>> tempExtRows = externalRows.get(dataID);
        if (tempExtRows.containsKey(item) && tempExtRows.get(item).getLeft() != null) {
            return tempExtRows.get(item).getLeft();
        } else {
            return getRowRef(item);
        }
    }

    public DoubleMatrix getRowRef(T item) {
        if (!syn0.containsKey(item)) {
            org.jblas.util.Random.seed((long) (item + Long.toString(seed)).hashCode());
            DoubleMatrix row = DoubleMatrix.rand(layerSize).subi(0.5).divi(layerSize);
            syn0.put(item, row);
        }
        return syn0.get(item);
    }

    public DoubleMatrix getContextRowRef(T item) {
        if (!syn1neg.containsKey(item)) {
            DoubleMatrix row = DoubleMatrix.zeros(layerSize);
            syn1neg.put(item, row);
        }
        return syn1neg.get(item);
    }

    public double getAlpha() {
        return alpha;
    }

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

//    public void setRow(T item, DoubleMatrix row) {
//        syn0.put(item, row);
//    }
//
//    public void setContextRow(T item, DoubleMatrix row) {
//        syn1neg.put(item, row);
//    }

    @Override
    public void updateRow(T item, DoubleMatrix gradient) {
        getRowRef(item).addi(gradient);
    }

    @Override
    public void updateContextRow(T item, DoubleMatrix gradient) {
        getContextRowRef(item).addi(gradient);
    }

    @Override
    public boolean contains(T item) {
        return syn0.containsKey(item);
    }

    @Override
    public long size() {
        return syn0.size();
    }

    @Override
    public void setSeed(long seed) {
        this.seed = seed;
    }

    public void setExternalRows(long dataID, Map<T, MutablePair<DoubleMatrix, DoubleMatrix>> externalRows) {
        this.externalRows.put(dataID, externalRows);
        initGradients(dataID);
    }

    private void initGradients(long dataID) {
        if (gradients == null) {
            gradients = new ConcurrentHashMap<>(externalRows.size());
        }
        Map<T, MutablePair<DoubleMatrix, DoubleMatrix>> tempExtRows = externalRows.get(dataID);
        HashMap<T, MutablePair<DoubleMatrix, DoubleMatrix>> newGradients = new HashMap<T, MutablePair<DoubleMatrix, DoubleMatrix>>(tempExtRows.size());
        gradients.put(dataID, newGradients);
        for (T key: tempExtRows.keySet()) {
            newGradients.put(key, new MutablePair<DoubleMatrix, DoubleMatrix>(null, null));
        }
    }

    public Map<T, MutablePair<DoubleMatrix, DoubleMatrix>> getGradients(long dataID) {
        return gradients.get(dataID);
    }

    public Map<T, MutablePair<DoubleMatrix, DoubleMatrix>> getExternalRows(long dataID) {
        return externalRows.get(dataID);
    }

    public void clean(long dataID) {
        externalRows.remove(dataID);
        gradients.remove(dataID);
    }

    @Override
    public SGNSLocalLearner<T> copy() {
        SGNSLocalLearner<T> l = new SGNSLocalLearner<T>(layerSize, alpha, minAlpha);
        l.alphaOption = (FloatOption) alphaOption.copy();
        l.minAlphaOption = (FloatOption) minAlphaOption.copy();
        l.layerSizeOption = (IntOption) layerSizeOption.copy();
        l.seed = seed;
        l.syn0 = new ConcurrentHashMap<>(syn0.size());
        for (T item: l.syn0.keySet()) {
            l.syn0.put(item, syn0.get(item).dup());
        }
        l.syn1neg = new ConcurrentHashMap<>(syn1neg.size());
        for (T item: l.syn0.keySet()) {
            l.syn0.put(item, syn0.get(item).dup());
        }
        l.externalRows = new ConcurrentHashMap<>(externalRows.size());
        for (long dataID: externalRows.keySet()) {
            Map<T, MutablePair<DoubleMatrix, DoubleMatrix>> newExtRow = new HashMap<>();
            l.externalRows.put(dataID, newExtRow);
            for (T item : externalRows.get(dataID).keySet()) {
                newExtRow.put(item, new MutablePair<DoubleMatrix, DoubleMatrix>(
                        externalRows.get(dataID).get(item).getKey().dup(),
                        externalRows.get(dataID).get(item).getValue().dup()));
            }
        }
        l.gradients = new ConcurrentHashMap<>(gradients.size());
        for (long dataID: gradients.keySet()) {
            Map<T, MutablePair<DoubleMatrix, DoubleMatrix>> newGradient = new HashMap<>();
            l.gradients.put(dataID, newGradient);
            for (T item : gradients.get(dataID).keySet()) {
                newGradient.put(item, new MutablePair<DoubleMatrix, DoubleMatrix>(
                        gradients.get(dataID).get(item).getKey().dup(),
                        gradients.get(dataID).get(item).getValue().dup()));
            }
        }
        return l;
    }

}
