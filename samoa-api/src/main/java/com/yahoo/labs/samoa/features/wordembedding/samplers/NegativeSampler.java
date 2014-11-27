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

import java.util.List;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */

public class NegativeSampler<T> implements Sampler<T> {
    @Override
    public boolean initConfiguration() {
        return false;
    }

    @Override
    public List<T> undersample(List<T> data) {
        return null;
    }

    @Override
    public long get(T item) {
        return 0;
    }

    @Override
    public void put(T item, long frequency) {

    }

    @Override
    public void remove(T item) {

    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public void setSeed(long seed) {

    }

    @Override
    public Sampler<T> copy() {
        return null;
    }
}
