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


import com.google.common.base.Joiner;
import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.features.wordembedding.indexers.IndexUpdateEvent;
import com.yahoo.labs.samoa.features.wordembedding.tasks.OneContentEvent;
import com.yahoo.labs.samoa.topology.Stream;
import org.jblas.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public abstract class SamplerProcessor<T> implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(SamplerProcessor.class);
    private static final long serialVersionUID = -4732844812145527288L;

    protected long seed = 1;
    protected Sampler sampler;
    protected int id;
    protected boolean firstDataReceived = false;
    protected long dataCount = 0;
    protected Stream learnerStream;
    protected Stream learnerAllStream;
    protected Stream modelStream;

    public SamplerProcessor(Sampler sampler) {
        this.sampler = sampler;
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        // If the seed is not explicitly set
        seed = id + 1;
        this.sampler.initConfiguration();
        this.sampler.setSeed(seed);
    }


    @Override
    public boolean process(ContentEvent event) {
//        if (sampler.getItemCount() % 10 == 0) {
//            logger.info(this.getClass().getSimpleName()+"-{} 111: after {} data samples, the vocabulary contains {} items " +
//                    "and {} item types", id, dataCount, sampler.getItemCount(), sampler.size());
//        }
        if (event instanceof IndexUpdateEvent) {
            //TODO only one last IndexUpdateEvent will arrive
            if (event.isLastEvent()) {
                logger.info(this.getClass().getSimpleName()+"-{}: collected {} item types from a " +
                                "corpus of {} items.", id, sampler.size(), sampler.getItemCount());
                return true;
            }
            IndexUpdateEvent<T> update = (IndexUpdateEvent<T>) event;
            // Update local vocabulary
            T item = update.getItem();
            long count = update.getCount();
            sampler.put(item, count);
            long itemCount = 1;
            Map<T, Long> removeUpdate = update.getRemovedItems();
            for(T removedItem: removeUpdate.keySet()) {
                itemCount -= removeUpdate.get(removedItem);
                sampler.remove(removedItem);
            }
            sampler.setItemCount(sampler.getItemCount() + itemCount);
//            if (sampler.getItemCount() % 10 == 0) {
//                logger.info(this.getClass().getSimpleName()+"-{} 222: after {} data samples, the vocabulary contains {} items " +
//                        "and {} item types", id, dataCount, sampler.getItemCount(), sampler.size());
//            }
        // When this type of events start to arrive, the vocabulary is already well filled
        } else if (event instanceof OneContentEvent) {
//               logger.info(this.getClass().getSimpleName()+"-{} 333: after {} data samples, the vocabulary contains {} items. {} " +
//                        "and {} item types", id, dataCount, sampler.getItemCount(), sampler.size(), event.isLastEvent());
            if (event.isLastEvent()) {
                logger.info(this.getClass().getSimpleName()+"-{}: ended with {} item types from a corpus of {} items " +
                                "and {} data samples", id, sampler.size(), sampler.getItemCount(), dataCount);
                learnerAllStream.put(new ItemEvent(null, null, null, true));
                return true;
            }
            if (!firstDataReceived) {
                firstDataReceived = true;
                logger.info(this.getClass().getSimpleName()+"-{}: starting sampling data, the vocabulary " +
                                "contains {} items and {} item types", id, sampler.getItemCount(), sampler.size());
                if (sampler.getItemCount() > 0) {
                    sampler.update();
                }
            }
            dataCount++;
            OneContentEvent content = (OneContentEvent) event;
            List<T> contentData = (List<T>) content.getContent();
            List<T> data = sampler.undersample(contentData);
            modelStream.put(new OneContentEvent<List<T>>(data, false));
            generateTraining(data);
            if (dataCount % 1000 == 0 && dataCount > 0) {
                logger.info(this.getClass().getSimpleName()+"-{}: after {} data samples, the vocabulary contains {} items " +
                        "and {} item types", id, dataCount, sampler.getItemCount(), sampler.size());
            }
        }
        return true;
    }

    public void setSeed(long seed) {
        this.seed = seed;
        this.sampler.setSeed(seed);
    }

    public void setLearnerStream(Stream learnerStream) {
        this.learnerStream = learnerStream;
    }

    public void setLearnerAllStream(Stream learnerAllStream) {
        this.learnerAllStream = learnerAllStream;
    }

    public void setModelStream(Stream modelStream) {
        this.modelStream = modelStream;
    }

    /**
     * Send the training samples from a sentence
     * @param data
     */
    protected abstract void generateTraining(List<T> data);

}
