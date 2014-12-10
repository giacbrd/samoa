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

import com.yahoo.labs.samoa.core.ContentEvent;

import java.util.Map;
import java.util.Set;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class IndexUpdateEvent<T> implements ContentEvent {

    private static final long serialVersionUID = 7608806911635852512L;
    private final Map<T, Long> itemVocab;
    private final Set<T> removedItems;
    private long itemCount;
    private String key;
    private boolean isLastEvent;

    public IndexUpdateEvent(Map<T, Long> itemVocab, Set<T> removedItems, long itemCount, boolean isLastEvent) {
        this.itemVocab = itemVocab;
        this.removedItems = removedItems;
        this.itemCount = itemCount;
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

    @Override
    public boolean isLastEvent() {
        return isLastEvent;
    }

    public long getItemCount() {
        return itemCount;
    }
    public Map<T, Long> getItemVocab() {
        return itemVocab;
    }

    public Set<T> getRemovedItems() {
        return removedItems;
    }

}
