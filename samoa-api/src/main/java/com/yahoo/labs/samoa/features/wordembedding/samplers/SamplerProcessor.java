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
    protected boolean firstDataReceived;
    protected long dataCount;
    protected Stream learnerStream;
    protected Stream modelStream;
    protected short window;

    public SamplerProcessor(Sampler sampler, short window) {
        this.sampler = sampler;
        this.window = window;
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
            if (event.isLastEvent()) {
                logger.info(this.getClass().getSimpleName()+"-{}: collected in vocabulary {} item types from a " +
                                "corpus of {} items.", id, sampler.size(), sampler.getItemCount());
                return true;
            }
            IndexUpdateEvent update = (IndexUpdateEvent) event;
            sampler.setItemCount(sampler.getItemCount() + update.getItemCount());
            // Update local vocabulary
            Map<T, Long> vocabUpdate = update.getItemVocab();
            for(Map.Entry<T, Long> v: vocabUpdate.entrySet()) {
                sampler.put(v.getKey(), v.getValue());
            }
            Set<T> removeUpdate = update.getRemovedItems();
            for(T word: removeUpdate) {
                sampler.remove(word);
            }
//            if (sampler.getItemCount() % 10 == 0) {
//                logger.info(this.getClass().getSimpleName()+"-{} 222: after {} data samples, the vocabulary contains {} items " +
//                        "and {} item types", id, dataCount, sampler.getItemCount(), sampler.size());
//            }
        // When this type of events start to arrive, the vocabulary is already well filled
        } else if (event instanceof OneContentEvent) {
//               logger.info(this.getClass().getSimpleName()+"-{} 333: after {} data samples, the vocabulary contains {} items. {} " +
//                        "and {} item types", id, dataCount, sampler.getItemCount(), sampler.size(), event.isLastEvent());
            if (event.isLastEvent()) {
                logger.info(this.getClass().getSimpleName()+"-{}: collected {} item types from a corpus of {} items " +
                                "and {} data samples", id, sampler.size(), sampler.getItemCount(), dataCount);
                learnerStream.put(new SGNSItemEvent(null, null, null, true));
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
            // Iterate through data items. For each item in context (contextItem) predict an item which is in the
            // range of |window - reduced_window|
            for (int pos = 0; pos < data.size(); pos++) {
                T contextItem = data.get(pos);
                // Generate a random window for each item
                int reduced_window = Random.nextInt(window); // `b` in the original word2vec code
                // now go over all items from the (reduced) window, predicting each one in turn
                int start = Math.max(0, pos - window + reduced_window);
                int end = pos + window + 1 - reduced_window;
                List<T> sentence2 = data.subList(start, end > data.size() ? data.size() : end);
                // Fixed a context item, iterate through items which have it in their context
                for (int pos2 = 0; pos2 < sentence2.size(); pos2++) {
                    T item = sentence2.get(pos2);
                    // don't train on OOV items and on the `item` itself
                    if (item != null && pos != pos2 + start) {
                        generateTraining(item, contextItem);
                    }
                }
            }
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

    public void setModelStream(Stream modelStream) {
        this.modelStream = modelStream;
    }

    /**
     * Generate the message for the distributed learner processors
     * @param item
     * @param contextItem
     */
    protected abstract void generateTraining(T item, T contextItem);

}
