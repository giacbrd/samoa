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
public class RowResponse<T> implements ContentEvent {

    private static final long serialVersionUID = -6269812669182472509L;
    private T item;
    private int position;
    private DoubleMatrix row;
    private DoubleMatrix contextRow;
    private String key;
    private boolean isLastEvent = false;

    public RowResponse() {
    }

    public RowResponse(T item, int position, DoubleMatrix row, DoubleMatrix contextRow, String key) {
        this.item = item;
        this.position = position;
        this.row = row;
        this.contextRow = contextRow;
        this.key = key;
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

    public DoubleMatrix getRow() {
        return row;
    }

    public DoubleMatrix getContextRow() {
        return contextRow;
    }

    public int getPosition() {
        return position;
    }
}
