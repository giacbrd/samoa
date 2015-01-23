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

import com.yahoo.labs.samoa.core.Processor;
import org.jblas.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class SGNSDataGenerator<T> extends SamplerProcessor<T> {

    private static final Logger logger = LoggerFactory.getLogger(SGNSDataGenerator.class);
    private static final long serialVersionUID = -5611465946745393746L;

    private final int parallelism;

    public SGNSDataGenerator(Sampler sampler, short window, int parallelism) {
        super(sampler, window);
        this.parallelism = parallelism;
    }

    /**
     * Send the training samples from a sentence
     * @param data
     */
    protected void generateTraining(List<T> data) {
        long dataID = UUID.randomUUID().toString().hashCode();
        for (int pos = 0; pos < data.size(); pos++) {
            T item = data.get(pos);
            // Generate a random window for each item
            List<T> negItems = ((NegativeSampler<T>) sampler).negItems();
            for (T negItem: negItems) {
                learnerStream.put(new ItemInDataEvent(negItem, dataID, -1, data.size(), false, negItem.toString()));
            }
            learnerStream.put(new ItemInDataEvent(item, dataID, pos, data.size(),false, item.toString()));
        }
    }

    @Override
    public Processor newProcessor(Processor processor) {
        SGNSDataGenerator p = (SGNSDataGenerator) processor;
        SGNSDataGenerator w = new SGNSDataGenerator(p.sampler.copy(), p.window, p.parallelism);
        w.learnerStream = p.learnerStream;
        w.learnerAllStream = p.learnerAllStream;
        w.modelStream = p.modelStream;
        w.dataCount = p.dataCount;
        w.firstDataReceived = p.firstDataReceived;
        w.setSeed(p.seed);
        return w;
    }
}
