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
public class SGNSLocalModel<T> implements Model<T> {

    private static final Logger logger = LoggerFactory.getLogger(SGNSLocalModel.class);
    private static final long serialVersionUID = 8581722952299330046L;

    private Map<T, DoubleMatrix> syn0;
    private int layerSize = 200;
    private long seed = 1;
    private Map<T, DoubleMatrix> syn1neg;

    public IntOption layerSizeOption = new IntOption("layerSize", 'l', "The number of columns of the model matrices.",
            layerSize);
    private boolean firstInit = true;

    public SGNSLocalModel(int layerSize) {
        init(layerSize, seed);
    }

    public SGNSLocalModel() {
        init(layerSize, seed);
    }

    @Override
    public boolean initConfiguration() {
        int newLayerSize = layerSizeOption.getValue();
        if (firstInit || newLayerSize != layerSize) {
            init(newLayerSize, 1);
            firstInit = false;
            return true;
        } else {
            return false;
        }
    }

    public void init(int layer1Size, long seed) {
        this.layerSize = layer1Size;
        this.seed = seed;
        syn0 = new HashMap<T, DoubleMatrix>(1000000);
        syn1neg = new HashMap<T, DoubleMatrix>(1000000);
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
    public SGNSLocalModel<T> copy() {
        SGNSLocalModel<T> l = new SGNSLocalModel<T>(layerSize);
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
