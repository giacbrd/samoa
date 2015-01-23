package com.yahoo.labs.samoa.features.wordembedding.learners.NaiveSGNS;

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

import java.util.List;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class RowRequest<T> implements ContentEvent {

    private static final long serialVersionUID = 4705812110929345641L;
    private String sourceKey;
    private T sourceItem;
    private T requestedItem;
    private String key;
    private boolean isLastEvent = false;

    public RowRequest() {
    }

    public RowRequest(String sourceKey, T sourceItem, T requestedItem) {
        this.sourceKey = sourceKey;
        this.sourceItem = sourceItem;
        this.requestedItem = requestedItem;
        this.key = requestedItem.toString();
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

    public T getSourceItem() {
        return sourceItem;
    }

    public T getRequestedItem() {
        return requestedItem;
    }

    public String getSourceKey() {
        return sourceKey;
    }

}
