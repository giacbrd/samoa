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

import com.yahoo.labs.samoa.core.ContentEvent;
import org.jblas.DoubleMatrix;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class RowUpdate<T> implements ContentEvent {

    private static final long serialVersionUID = 65885385385095409L;
    private T item;
    private DoubleMatrix gradient;
    private DoubleMatrix contextGradient;
    private String key;
    private boolean isLastEvent = false;

    public RowUpdate() {
    }

    public RowUpdate(T item, DoubleMatrix gradient, DoubleMatrix contextGradient) {
        this.item = item;
        this.gradient = gradient;
        this.contextGradient = contextGradient;
        this.key = item.toString();
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public void setKey(String key) {

        this.key = key;
    }

    @Override
    public boolean isLastEvent() {
        return isLastEvent;
    }

    public T getItem() {
        return item;
    }

    public DoubleMatrix getGradient() {
        return gradient;
    }

    public DoubleMatrix getContextGradient() {
        return contextGradient;
    }
}
