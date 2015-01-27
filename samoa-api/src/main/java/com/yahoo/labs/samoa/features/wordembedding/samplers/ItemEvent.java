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

import com.yahoo.labs.samoa.core.ContentEvent;

import java.util.List;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class ItemEvent<T> implements ContentEvent {

    private static final long serialVersionUID = 278023268642657276L;
    private T item;
    private T contextItem;
    private List<T> negItems;
    private String key;
    private boolean isLastEvent;

    public ItemEvent() {
    }

    public ItemEvent(T item, T contextItem, List<T> negItems, boolean isLastEvent) {
        this.item = item;
        this.contextItem = contextItem;
        this.negItems = negItems;
        this.isLastEvent = isLastEvent;
    }

    public ItemEvent(T item, T contextItem, List<T> negItems, boolean isLastEvent, String key) {
        this(item, contextItem, negItems, isLastEvent);
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

    public T getContextItem() {
        return contextItem;
    }

    public List<T> getNegItems() {
        return negItems;
    }

}