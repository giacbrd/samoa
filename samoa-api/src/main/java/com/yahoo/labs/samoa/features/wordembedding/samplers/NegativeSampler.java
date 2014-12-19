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

import com.github.javacliparser.FloatOption;
import com.github.javacliparser.IntOption;
import com.yahoo.labs.samoa.features.counters.Counter;
import com.yahoo.labs.samoa.features.counters.StreamSummary;
import org.jblas.util.Random;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */

public class NegativeSampler<T> implements Sampler<T> {

    private static final long serialVersionUID = 7708675565227109637L;
    private double subsamplThr = 0.0;
    private Counter<T> vocab;
    private int itemsPerUpdate = 100000;
    private Double normFactor;
    private double power = 0.75;
    private int[] table;
    private int tableSize = 100000000;
    private long seed = 1;
    //FIXME setting the capacity to MAX_VALUE for some data structures can be dangerous (huge allocation of memory)
    private int capacity = Integer.MAX_VALUE;
    //FIXME use hash values instead of strings (from IndexerProcessor to the model)
    private Object[] index2item;
    private int negative = 10;
    private long itemUpdates;
    private long itemCount;

    public IntOption itemsPerUpdateOption = new IntOption("itemsPerUpdateOption", 'u', "Number of word index updates" +
            "necessary for a new update of the table for negative sampling.", itemsPerUpdate);
    public FloatOption subsamplThrOption = new FloatOption("subsampleThreshold", 's', "Threshold in words sub-sampling, " +
            "the t parameter in the article.", subsamplThr);
    public FloatOption powerOption = new FloatOption("power", 'p', "The power parameter in the unigram distribution for" +
            "negative sampling.", power);
    public IntOption tableSizeOption = new IntOption("tableSize", 't', "The size of the table for negative sampling.",
            tableSize);
    public IntOption negativeOption = new IntOption("negative", 'n', "The number of negative samples, the k parameter " +
            "in the article.", negative);
    public IntOption capacityOption = new IntOption("capacity", 'c', "The capacity of the counters for word counts " +
            "estimation.", capacity);
    private boolean firstInit = true;


    public NegativeSampler(int negative, double power, int tableSize, double subsamplThr, int capacity,
                           int itemsPerUpdate) {
        init(negative, power, tableSize, subsamplThr, capacity, itemsPerUpdate, seed);
    }

    public NegativeSampler() {
        init(negative, power, tableSize, subsamplThr, capacity, itemsPerUpdate, seed);
    }

    @Override
    public boolean initConfiguration() {
        int newNegative = negativeOption.getValue();
        double newPower = powerOption.getValue();
        int newTableSize = tableSizeOption.getValue();
        double newSubsamplThr = subsamplThrOption.getValue();
        int newCapacity = capacityOption.getValue();
        int newItemsPerUpdate = itemsPerUpdateOption.getValue();
        if (firstInit || newNegative != negative || newPower != power || newTableSize != tableSize
                || newSubsamplThr != subsamplThr || newCapacity != capacity || newItemsPerUpdate != itemsPerUpdate) {
            init(newNegative, newPower, newTableSize, newSubsamplThr, newCapacity, newItemsPerUpdate, 1);
            firstInit = false;
            return true;
        } else {
            return false;
        }
    }

    public void init(int negative, double power, int tableSize, double subsamplThr, int capacity,
                           int wordsPerUpdate, long seed) {
        this.negative = negative;
        this.power = power;
        this.tableSize = tableSize;
        this.subsamplThr = subsamplThr;
        this.capacity = capacity;
        this.itemsPerUpdate = wordsPerUpdate;
        this.setSeed(seed);
        itemUpdates = 0;
        index2item = new Object[0];
        normFactor = 1.0;
        itemCount = 0;
        vocab = new StreamSummary<T>(capacity);
    }

    @Override
    public List<T> undersample(List<T> data) {
        List<T> sampledData = new ArrayList<T>();
        for (T item: data) {
            if (vocab.containsKey(item)) {
                long count = vocab.get(item);
                // Subsampling probability
                double prob = Math.min(subsamplThr > 0 ? Math.sqrt(subsamplThr / ((double)count / itemCount)) : 1.0, 1.0);
                if (prob >= 1.0 || prob >= Random.nextDouble()) {
                    sampledData.add(item);
                }
            }
        }
        return sampledData;
    }

    // FIXME need a more fine and intelligent update, also to be asynchronous!
    @Override
    public void update() {
        table = new int[tableSize]; //table (= list of words) of noise distribution for negative sampling
        //compute sum of all power (Z in paper)
        normFactor = 0.0;
        int vocabSize = vocab.size();
        Iterator<Map.Entry<T, Long>> vocabIter = vocab.iterator();
        while (vocabIter.hasNext()) {
            normFactor += Math.pow(vocabIter.next().getValue(), power);
        }
        //logger.info("SGNSSampler: constructing a table with noise distribution from {} words", vocabSize);
        index2item = new Object[vocabSize];
        //go through the whole table and fill it up with the word indexes proportional to a word's count**power
        int widx = 0;
        vocabIter = vocab.iterator();
        Map.Entry<T, Long> vocabItem = vocabIter.next();
        long count = vocabItem.getValue();
        index2item[widx] = vocabItem.getKey();
        // normalize count^0.75 by Z
        double d1 = Math.pow(count, power) / normFactor;
        for (int tidx = 0; tidx < tableSize; tidx++) {
            table[tidx] = widx;
            if ((double)tidx / tableSize > d1) {
                widx++;
                if (vocabIter.hasNext()) {
                    vocabItem = vocabIter.next();
                    count = vocabItem.getValue();
                    index2item[widx] = vocabItem.getKey();
                    d1 += Math.pow(count, power) / normFactor;
                }

            }
            if (widx >= vocabSize) {
                widx = vocabSize - 1;
            }
        }
    }

    public List<T> negItems() {
        List<T> negItems = new ArrayList<T>(negative);
        for (int i = 0; i < negative; i++) {
            int neg = table[Random.nextInt(table.length)];
            negItems.add((T) index2item[neg]);
        }
        return negItems;
    }

    @Override
    public long getItemCount() {
        return itemCount;
    }

    public void setItemCount(long itemCount) {
        this.itemCount = itemCount;
    }

    @Override
    public long get(T item) {
        return vocab.get(item);
    }

    @Override
    public void put(T item, long frequency) {
        vocab.put(item, frequency);
        checkUpdate();
    }

    @Override
    public void remove(T item) {
        vocab.remove(item);
        checkUpdate();
    }

    private void checkUpdate() {
        itemUpdates++;
        if (itemUpdates % itemsPerUpdate == 0) {
            update();
        }
    }

    @Override
    public long size() {
        return vocab.size();
    }

    @Override
    public void setSeed(long seed) {
        this.seed = seed;
        Random.seed(seed);
    }

    @Override
    public Sampler<T> copy() {
        NegativeSampler<T> s = new NegativeSampler<T>(negative, power, tableSize, subsamplThr, capacity, itemsPerUpdate);
        s.setSeed(seed);
        s.setSeed(seed);
        s.itemUpdates = itemUpdates;
        s.index2item = index2item.clone();
        s.normFactor = normFactor;
        s.itemCount = itemCount;
        //FIXME need to clone this!
        s.vocab = vocab;
        return s;
    }
}
