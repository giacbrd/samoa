package com.yahoo.labs.samoa.features.wordembedding.learners.LocalSamplingSGNS;

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

import org.apache.commons.lang3.tuple.MutablePair;
import org.jblas.DoubleMatrix;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
* Created by giacomo on 27/01/15.
*/
class LocalData<T> implements Serializable {

    private static final long serialVersionUID = -6584987133642894630L;

    T[] data;
    // Contains the hash of the item vector at the moment of setting the item
    Map<T, Integer> rowHashes;
    Map<T, MutablePair<DoubleMatrix, DoubleMatrix>> externalRows;
    int dataCount = 0;

    public LocalData(T[] data) {
        this.data = data;
        externalRows = new HashMap<>(data.length);
        rowHashes = new HashMap<>(data.length);
    }

    public boolean setItem(int index, T item, DoubleMatrix row, boolean local) {
        if (local) {
            rowHashes.put(item, row.hashCode());
        }
        data[index] = item;
        dataCount++;
        if (dataCount >= data.length) {
            return true;
        }
        return false;
    }

    public boolean setItem(int index, T item, DoubleMatrix row) {
        return setItem(index, item, row, true);
    }

    public boolean setExternalItem(int index, T item, DoubleMatrix row, DoubleMatrix contextRow) {
        externalRows.put(item, new MutablePair<DoubleMatrix, DoubleMatrix>(row, contextRow));
        return setItem(index, item, row, false);
    }

    LocalData<T> copy() {
        LocalData<T> l = new LocalData<T>(data.clone());
        for (T item: externalRows.keySet()) {
            l.externalRows.put(item, new MutablePair<DoubleMatrix, DoubleMatrix>(
                    externalRows.get(item).getLeft(), externalRows.get(item).getRight()
            ));
        }
        l.dataCount = dataCount;
        return l;
    }

    public boolean rowChanged(T item, DoubleMatrix row) {
        if (rowHashes.get(item) != row.hashCode()) {
            return true;
        } else {
            return false;
        }
    }
}
