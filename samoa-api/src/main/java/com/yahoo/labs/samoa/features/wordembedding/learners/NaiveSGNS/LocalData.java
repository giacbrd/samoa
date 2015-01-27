package com.yahoo.labs.samoa.features.wordembedding.learners.NaiveSGNS;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2013 - 2015 Yahoo! Inc.
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
import java.util.List;
import java.util.Map;

/**
* Created by giacomo on 27/01/15.
*/
class LocalData<T> implements Serializable {

    private static final long serialVersionUID = -5572720849350089181L;

    final T item;
    final T contextItem;
    final List<T> negItems;
    final Map<T, MutablePair<DoubleMatrix, DoubleMatrix>> externalRows;
    int dataCount = 0;
    final int totalData;

    LocalData(T item, T contextItem, List<T> negItems, Map<T, MutablePair<DoubleMatrix, DoubleMatrix>> externalRows, int totalData) {
        this.item = item;
        this.contextItem = contextItem;
        this.negItems = negItems;
        this.externalRows = externalRows;
        this.totalData = totalData;
    }

    LocalData<T> copy() {
        LocalData<T> l = new LocalData<T>(item, contextItem, negItems, externalRows, totalData);
        for (T extItem: externalRows.keySet()) {
            l.externalRows.put(extItem, new MutablePair<DoubleMatrix, DoubleMatrix>(
                    externalRows.get(extItem).getLeft().dup(), externalRows.get(extItem).getRight().dup()));
        }
        l.dataCount = dataCount;
        return l;
    }
}
