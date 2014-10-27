package com.yahoo.labs.samoa.features.word2vec;

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

import java.io.Serializable;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class Vocab implements Serializable, Comparable {
    public int count;
    public int index;
    public double sampleProb = 1.0;

    public Vocab(int count) {
        this.count = count;
    }

    /**
     * Order by word count, top words are the most frequent.
     * @param o
     * @return
     */
    @Override
    public int compareTo(Object o) {
        Vocab other = (Vocab) o;
        return Integer.compare(other.count, this.count);
    }
}
