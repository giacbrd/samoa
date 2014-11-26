package com.yahoo.labs.samoa.features.wordembedding.models;

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
public class ModelUpdateEvent<T> implements ContentEvent {

    private String key;
    private T word;
    private DoubleMatrix row;
    private boolean isLastEvent;

    public ModelUpdateEvent(T word, DoubleMatrix row, boolean isLastEvent) {
        this.word = word;
        this.row = row;
        this.isLastEvent = isLastEvent;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public void setKey(String key) {

        this.key = key;
    }

    public T getWord() {
        return word;
    }

    public DoubleMatrix getRow() {
        return row;
    }

    @Override
    public boolean isLastEvent() {
        return isLastEvent;
    }

}
