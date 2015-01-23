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

import com.github.javacliparser.Configurable;
import org.jblas.DoubleMatrix;

import java.io.Serializable;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public interface Model<T> {

    /**
     * Initialize the configuration with the CLI options.
     * If configuration options have changed everything is re-initialized.
     * @return True if new configuration options are set.
     */
    boolean initConfiguration();

    int columns();

    long rows();

    DoubleMatrix getRow(T item);

    DoubleMatrix getContextRow(T item);

    void updateRow(T item, DoubleMatrix update);

    void updateContextRow(T item, DoubleMatrix update);

    boolean contains(T item);

    void setSeed(long seed);

//    /**
//     * Execute a learning step.
//     * @param item Learn the representation of this item
//     * @param contextItem An item in the context of the first one
//     */
//    void train(T item, T contextItem);

    Model<T> copy();
}
