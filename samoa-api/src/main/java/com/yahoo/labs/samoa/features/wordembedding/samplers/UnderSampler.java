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

public class UnderSampler<T> implements Sampler<T> {

    private static final long serialVersionUID = 7708676565227109647L;
    private double subsamplThr = 0.0;
    //FIXME the vocab type has to be a parameter
    private Counter<T> vocab;
    private Double normFactor;
    private long seed = 1;
    //FIXME setting the capacity to MAX_VALUE for some data structures can be dangerous (huge allocation of memory)
    private int capacity = Integer.MAX_VALUE;
    //FIXME use hash values instead of strings (from IndexerProcessor to the model)
    private long itemCount;

    public FloatOption subsamplThrOption = new FloatOption("subsampleThreshold", 's', "Threshold in words sub-sampling, " +
            "the t parameter in the article.", subsamplThr);
    public IntOption capacityOption = new IntOption("capacity", 'c', "The capacity of the counters for word counts " +
            "estimation.", capacity);
    private boolean firstInit = true;


    public UnderSampler(double subsamplThr, int capacity) {
        init(subsamplThr, capacity, seed);
    }

    public UnderSampler() {
        init(subsamplThr, capacity, seed);
    }

    @Override
    public boolean initConfiguration() {
        double newSubsamplThr = subsamplThrOption.getValue();
        int newCapacity = capacityOption.getValue();
        if (firstInit || newSubsamplThr != subsamplThr || newCapacity != capacity) {
            init(newSubsamplThr, newCapacity, 1);
            firstInit = false;
            return true;
        } else {
            return false;
        }
    }

    public void init(double subsamplThr, int capacity, long seed) {
        this.subsamplThr = subsamplThr;
        this.capacity = capacity;
        this.setSeed(seed);
        normFactor = 1.0;
        itemCount = 0;
        //TODO a very interesting alternative is time-aware counter: yongsub_CIKM2014.pdf
        vocab = new StreamSummary<T>(capacity);
    }

    @Override
    public List<T> undersample(List<T> data) {
        List<T> sampledData = new ArrayList<T>();
        for (T item: data) {
            if (vocab.containsKey(item)) {
                long count = vocab.get(item);
                // Subsampling probability
                double prob = Math.min(subsamplThr > 0 ? Math.sqrt(subsamplThr / ((double) count / itemCount)) : 1.0, 1.0);
                if (prob >= 1.0 || prob >= Random.nextDouble()) {
                    sampledData.add(item);
                }
            }
        }
        return sampledData;
    }

    @Override
    public void update() {

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
    }

    @Override
    public void remove(T item) {
        vocab.remove(item);
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
        UnderSampler<T> s = new UnderSampler<T>(subsamplThr, capacity);
        s.setSeed(seed);
        s.normFactor = normFactor;
        s.itemCount = itemCount;
        //FIXME need to clone this!
        s.vocab = vocab;
        return s;
    }
}
