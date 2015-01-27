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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.yahoo.labs.samoa.utils.StreamDestination.getPIIndexForKey;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public abstract class SamplerProcessor2<T> implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(SamplerProcessor2.class);
    private static final long serialVersionUID = -4732844812145527288L;

    protected Sampler sampler;
    protected long seed = 1;
    protected ArrayList<Sampler> samplers;
    protected int id;
    protected boolean firstDataReceived;
    protected long dataCount;
    protected Stream learnerStream;
    protected Stream learnerAllStream;
    protected Stream modelStream;
    protected short window;
    protected int learnerParallelism;

    // FIXME the sampler parameter is a type not a class argument! pass it as a generic
    public SamplerProcessor2(Sampler sampler, short window, int learnerParallelism) {
        this.sampler = sampler;
        this.window = window;
        this.learnerParallelism = learnerParallelism;
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        // If the seed is not explicitly set
        seed = id + 1;
        if (samplers == null || samplers.isEmpty()) {
            samplers = new ArrayList<Sampler>(learnerParallelism);
            for (int i = 0; i < learnerParallelism; i++) {
                samplers.add(sampler.copy());
                samplers.get(i).initConfiguration();
                samplers.get(i).setSeed(seed);
            }
        }
    }


    @Override
    public boolean process(ContentEvent event) {
//        if (sampler.getItemCount() % 10 == 0) {
//            logger.info(this.getClass().getSimpleName()+"-{} 111: after {} data samples, the vocabulary contains {} items " +
//                    "and {} item types", id, dataCount, sampler.getItemCount(), sampler.size());
//        }
        if (event instanceof IndexUpdateEvent) {
            if (event.isLastEvent()) {
                logger.info(this.getClass().getSimpleName()+"-{}: collected in vocabulary.", id);
                return true;
            }
            IndexUpdateEvent update = (IndexUpdateEvent) event;
            // Update local vocabulary
            Map<T, Map.Entry<Long, Long>> vocabUpdate = update.getItemVocab();
            for(Map.Entry<T, Map.Entry<Long, Long>> v: vocabUpdate.entrySet()) {
                int key = getPIIndexForKey(v.getKey().toString(), learnerParallelism);
                samplers.get(key).setItemCount(samplers.get(key).getItemCount() + v.getValue().getValue());
                samplers.get(key).put(v.getKey(), v.getValue().getKey() + v.getValue().getValue());
            }
            Map<T, Long> removeUpdate = update.getRemovedItems();
            for(T item: removeUpdate.keySet()) {
                int key = getPIIndexForKey(item.toString(), learnerParallelism);
                samplers.get(key).setItemCount(samplers.get(key).getItemCount() - removeUpdate.get(item));
                samplers.get(key).remove(item);
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
                logger.info(this.getClass().getSimpleName()+"-{}: collected {} data samples", id, dataCount);
                learnerAllStream.put(new ItemEvent(null, null, null, true));
                return true;
            }
            if (!firstDataReceived) {
                firstDataReceived = true;
                logger.info(this.getClass().getSimpleName()+"-{}: starting sampling data, the vocabulary", id);
                for (int i = 0; i < learnerParallelism; i++) {
                    if (samplers.get(i).getItemCount() > 0) {
                        samplers.get(i).update();
                    }
                }
            }
            dataCount++;
            OneContentEvent content = (OneContentEvent) event;
            List<T> data = (List<T>) content.getContent();
//            data = samplers.get(0).undersample(data);
//            for (int i = 1; i < learnerParallelism; i++) {
//                data = samplers.get(i).undersample(data);
//            }
            List<T> sampledData = new ArrayList<T>();
            for (T item: data) {
                for (int i = 0; i < learnerParallelism; i++) {
                    if (samplers.get(i).get(item) > 0) {
                        sampledData.add(item);
                        break;
                    }
                }
            }
            modelStream.put(new OneContentEvent<List<T>>(sampledData, false));
            generateTraining(sampledData);
            if (dataCount % 1000 == 0 && dataCount > 0) {
                logger.info(this.getClass().getSimpleName()+"-{}: after {} data samples", id, dataCount);
            }
        }
        return true;
    }

    public void setSeed(long seed) {
        this.seed = seed;
        if (samplers != null && !samplers.isEmpty()) {
            for (int i = 0; i < learnerParallelism; i++) {
                samplers.get(i).setSeed(seed);
            }
        }
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
