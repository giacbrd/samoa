package com.yahoo.labs.samoa.features.wordembedding.indexers;

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

import com.github.javacliparser.Configurable;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public interface Indexer<T> extends Configurable, Serializable {

    /**
     * Initialize the configuration with the CLI options.
     * If configuration options have changed everything is re-initialized.
     * @return True if new configuration options are set.
     */
    boolean initConfiguration();

    /**
     * Add items to the index and return their counts.
     * The returned items are a subset of the input data,
     * they are filtered according to some Indexer rule (e.g., no items less frequent than...)
     * @param data A list of items to index/count
     * @return The new, total counts of a subset of the added items, and the sum of the count increments.
     */
    Map.Entry<Map<T, Long>, Long> add(List<T> data);

    /**
     * The items eventually removed after any operation on the index.
     * Each call of this method clear the Indexer internal list of the items to be removed.
     * @return A map of items removed from the index with their counts
     */
    Map<T, Long> getRemoved();

    /**
     * Number of unique items.
     * @return
     */
    long size();

    /**
     * Number of data elements (sentences, documents,...) processed.
     * @return
     */
    long dataCount();

    /**
     * Sum of all item counts (occurrences count of items in processed data).
     * @return
     */
    long itemCount();

    Indexer<T> copy();

}
