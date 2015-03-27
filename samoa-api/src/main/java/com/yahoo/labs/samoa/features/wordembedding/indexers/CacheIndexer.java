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


import com.github.javacliparser.IntOption;
import com.google.common.cache.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class CacheIndexer<T> implements Indexer<T> {

    private static final Logger logger = LoggerFactory.getLogger(CacheIndexer.class);
    private static final long serialVersionUID = 6644093609991285167L;

    private long cacheSize = Integer.MAX_VALUE;
    private long itemExpiry = 24*60;
    //FIXME is this transient safe?
    transient private LoadingCache<T, Long> vocab;
    private long totalItems;
    private Map<T, Long> removeVocab;
    private short minCount = 5;

    public IntOption cacheSizeOption = new IntOption("cacheSize", 'c', "The max number of elements in the main memory " +
            "cache.", (int) cacheSize);
    public IntOption itemExpiryOption = new IntOption("itemExpiry", 'e', "If a item does not occur in the stream after " +
            "this time (in minutes), it is removed from the index and the model.", (int) itemExpiry);
    public IntOption minCountOption = new IntOption("minCount", 'm', "Ignore all items with total frequency lower than " +
            "this.", minCount);
    private boolean firstInit = true;

    public CacheIndexer() {
        init(cacheSize, itemExpiry, minCount);
    }

    public CacheIndexer(long cacheSize, long itemExpiry, short minCount) {
        init(cacheSize, itemExpiry, minCount);
    }

    @Override
    public boolean initConfiguration() {
        long newCacheSize = cacheSizeOption.getValue();
        long newItemExpiry = itemExpiryOption.getValue();
        short newMinCount = (short) minCountOption.getValue();
        if (firstInit || newCacheSize != cacheSize || newItemExpiry != itemExpiry || newMinCount != minCount) {
            init(newCacheSize, newItemExpiry, newMinCount);
            firstInit = false;
            return true;
        } else {
            return false;
        }
    }

    protected void init(long cacheSize, long itemExpiry, short minCount) {
        this.cacheSize = cacheSize;
        this.itemExpiry = itemExpiry;
        this.minCount = minCount;
        vocab = initCache(cacheSize, itemExpiry);
        removeVocab = new HashMap<>();
        totalItems = 0;
    }

    LoadingCache<T, Long> initCache(long cacheSize, long itemExpiry) {
        RemovalListener<T, Long> removalListener = new RemovalListener<T, Long>() {
            public void onRemoval(RemovalNotification<T, Long> removal) {
                if (removal.getCause() != RemovalCause.REPLACED) {
                    logger.debug("Remove from cache: " + removal.getKey() + ". Cause: " + removal.getCause().toString());
                    removeVocab.put(removal.getKey(), removal.getValue());
                }
            }
        };
        vocab = CacheBuilder.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterWrite(itemExpiry, TimeUnit.MINUTES)
//                .maximumWeight(125000000)
//                .weigher(
//                        new Weigher<String, Long>() {
//                            public int weigh(String key, Long value) {
//                                return key.length();
//                            }
//                        })
                .removalListener(removalListener)
                .build(
                        new CacheLoader<T, Long>() {
                            public Long load(T key) {
                                return (long) 0;
                            }
                        });
        return vocab;
    }

    @Override
    public long add(T item) {
        totalItems++;
        long count = 0;
        try {
            count = vocab.get(item) + 1;
            vocab.put(item, count);
        } catch (ExecutionException e) {
            logger.error("Cache access error for item: " + item);
            e.printStackTrace();
        }
        removeVocab.keySet().remove(item);
        //FIXME 10 should be a parameter, or maybe following a zip distribution
        if (count > minCount && (count == minCount || (count % 10 == 0))) {
            return count;
        } else {
            return (long) 0;
        }
    }

    private Map<T, Long> dataCount(List<T> data) {
        HashMap<T, Long> currVocab = new HashMap<T, Long>(data.size());
        for (T item: data) {
            if (currVocab.containsKey(item)) {
                currVocab.put(item, currVocab.get(item) + 1);
            } else {
                currVocab.put(item, (long) 1);
            }
        }
        return currVocab;
    }

    @Override
    public Map getRemoved() {
        Map<T, Long> result = removeVocab;
        removeVocab = new HashMap<>();
        return result;
    }

    @Override
    public long size() {
        return vocab.size();
    }

    @Override
    public long itemTotalCount() {
        return totalItems;
    }

    /**
     * Almost exact clone, some cache internals are not copied.
     * @return
     */
    @Override
    public Indexer<T> copy() {
        CacheIndexer<T> c = new CacheIndexer<T>(cacheSize, itemExpiry, minCount);
        c.cacheSizeOption = (IntOption) cacheSizeOption.copy();
        c.itemExpiryOption = (IntOption) itemExpiryOption.copy();
        c.minCountOption = (IntOption) minCountOption.copy();
        c.totalItems = totalItems;
        c.removeVocab = new HashMap<>(removeVocab);
        c.vocab = initCache(cacheSize, itemExpiry);
        for (Map.Entry<T, Long> entry: vocab.asMap().entrySet()) {
            c.vocab.put(entry.getKey(), entry.getValue());
        }
        return c;
    }
}
