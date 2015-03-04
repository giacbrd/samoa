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
import java.util.List;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */

public class NullSampler<T> implements Sampler<T> {

    private static final long serialVersionUID = 7708676565338109647L;
    private long itemCount = 0;

    @Override
    public boolean initConfiguration() {
        return false;
    }

    @Override
    public List<T> undersample(List<T> data) {
        return data;
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
        return -1;
    }

    @Override
    public void put(T item, long frequency) {};

    @Override
    public void remove(T item) {};

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void setSeed(long seed) {};

    @Override
    public Sampler<T> copy() {
        NullSampler<T> s = new NullSampler<T>();
        s.itemCount = itemCount;
        return s;
    }
}
