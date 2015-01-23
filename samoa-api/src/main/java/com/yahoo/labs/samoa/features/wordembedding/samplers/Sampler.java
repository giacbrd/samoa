package com.yahoo.labs.samoa.features.wordembedding.samplers;

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
import org.jblas.DoubleMatrix;

import java.io.Serializable;
import java.util.List;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public interface Sampler<T> extends Configurable, Serializable {

    /**
     * Initialize the configuration with the CLI options.
     * If configuration options have changed everything is re-initialized.
     * @return True if new configuration options are set.
     */
    boolean initConfiguration();

    /**
     * Undersample input data according to the Sampler type.
     * @param data
     * @return The same data input, order is preserved, some items are removed.
     */
    List<T> undersample(List<T> data);

    /**
     * "Manually" update internal items distribution representation, possibly asynchronously.
     */
    void update();

    /**
     * The frequency (count) of an item, used for computing sampling probabilities.
     * @param item
     * @return The item's frequency, 0 if the item does not exist.
     */
    long get(T item);

    /**
     * Set the new frequency of the item.
     * @param item
     * @param frequency
     */
    void put(T item, long frequency);

    void remove(T item);

    /**
     * Number of unique items.
     * @return
     */
    long size();

    void setSeed(long seed);

    Sampler<T> copy();

    /**
     * Set the true sum of item frequencies, so that the sampler can have the exact value and not an approximated one.
     * @param itemCount
     */
    void setItemCount(long itemCount);

    long getItemCount();
}
