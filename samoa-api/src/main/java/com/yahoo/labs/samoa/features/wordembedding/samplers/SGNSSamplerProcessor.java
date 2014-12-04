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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.yahoo.labs.samoa.core.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class SGNSSamplerProcessor<T> extends SamplerProcessor<T> {

    private static final Logger logger = LoggerFactory.getLogger(SGNSSamplerProcessor.class);
    private final int parallelism;

    public SGNSSamplerProcessor(Sampler sampler, short window, int parallelism) {
        super(sampler, window);
        this.parallelism = parallelism;
    }

    @Override
    protected void generateTraining(T item, T contextItem) {
        List<T> tempNegItems = ((NegativeSampler<T>) sampler).negItems();
        List<T> negItems = new ArrayList<>(tempNegItems.size());
        Multiset<Integer> hashes = HashMultiset.create(tempNegItems.size() + 2);
        hashes.add(Math.abs(item.hashCode()) % parallelism);
        hashes.add(Math.abs(contextItem.hashCode()) % parallelism);
        for (T negItem: tempNegItems) {
            //FIXME if the condition is not met, word pair is not sent (the original word2vec does the same)
            if (!negItem.equals(contextItem)) {
                negItems.add(negItem);
                hashes.add(Math.abs(negItem.hashCode()) % parallelism);
            }
        }
        // Set the message key with the most frequent item hashcode
        String outKey = "";
        int maxCount = -1;
        for (Multiset.Entry entry: hashes.entrySet()) {
            if (entry.getCount() > maxCount) {
                outKey = Integer.toString((int) entry.getElement());
                maxCount = entry.getCount();
            }
        }
        learnerStream.put(new SGNSItemEvent(item, contextItem, negItems, false, outKey));
    }

    @Override
    public Processor newProcessor(Processor processor) {
        SGNSSamplerProcessor p = (SGNSSamplerProcessor) processor;
        SGNSSamplerProcessor w = new SGNSSamplerProcessor(p.sampler.copy(), p.window, p.parallelism);
        w.learnerStream = p.learnerStream;
        w.modelStream = p.modelStream;
        w.dataCount = p.dataCount;
        w.firstDataReceived = p.firstDataReceived;
        w.setSeed(p.seed);
        return w;
    }
}
