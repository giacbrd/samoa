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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.jblas.DoubleMatrix;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
* Created by giacomo on 27/01/15.
*/
class LocalData<T> implements Serializable {

    private static final long serialVersionUID = -6584987133642894630L;

    T[] data;
    Map<T, MutablePair<DoubleMatrix, DoubleMatrix>> externalRows;
    int dataCount = 0;

    public LocalData(T[] data) {
        this.data = data;
        externalRows = new HashMap<>(data.length);
    }

    public boolean setLocalItem(int index, T item) {
        data[index] = item;
        dataCount++;
        if (dataCount >= data.length) {
            return true;
        }
        return false;
    }

    public boolean setExternalItem(int index, T item, DoubleMatrix row, DoubleMatrix contextRow) {
        externalRows.put(item, new MutablePair<DoubleMatrix, DoubleMatrix>(row, contextRow));
        return setLocalItem(index, item);
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
}
