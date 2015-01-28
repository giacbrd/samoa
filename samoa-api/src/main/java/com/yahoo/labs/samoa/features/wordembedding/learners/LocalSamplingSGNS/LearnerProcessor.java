package com.yahoo.labs.samoa.features.wordembedding.learners.LocalSamplingSGNS;

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
import com.yahoo.labs.samoa.features.wordembedding.learners.SGNSLocalLearner;
import com.yahoo.labs.samoa.features.wordembedding.models.ModelUpdateEvent;
import com.yahoo.labs.samoa.features.wordembedding.samplers.*;
import com.yahoo.labs.samoa.topology.Stream;
import org.jblas.DoubleMatrix;
import org.jblas.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class LearnerProcessor<T> implements Processor {

    private static final long serialVersionUID = -2266852696732445189L;

    private static final Logger logger = LoggerFactory.getLogger(LearnerProcessor.class);

    private final SGNSLocalLearner learner;
    private short window;
    private final Sampler<T> sampler;
    private long seed = 1;
    private Stream synchroStream;
    private Stream modelStream;
    private int id;
    private long iterations;
    private boolean modelAckSent = false;
    //FIXME substitute with guava cache
    private Map<Long, LocalData<T>> tempData = new HashMap<Long, LocalData<T>>();
    private int lastEventCount = 0;
    private int samplerCount;
    private boolean firstDataReceived = false;

    public LearnerProcessor(short window, SGNSLocalLearner localLearner, Sampler<T> sampler, int samplerCount) {
        this.window = window;
        this.sampler = sampler;
        this.learner = localLearner;
        this.samplerCount = samplerCount;
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        this.learner.initConfiguration();
        this.learner.setSeed(seed);
        this.sampler.initConfiguration();
        this.sampler.setSeed(seed);
    }

    @Override
    public boolean process(ContentEvent event) {
        if (event instanceof IndexUpdateEvent) {
            if (event.isLastEvent()) {
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
        } if (event.isLastEvent()) {
            lastEventCount++;
        } else if (event instanceof DataIDEvent) {
            if (!firstDataReceived) {
                firstDataReceived = true;
                logger.info(this.getClass().getSimpleName()+"-{}: starting learning, the local negative sampler " +
                        "contains {} items and {} item types", id, sampler.getItemCount(), sampler.size());
                if (sampler.getItemCount() > 0) {
                    sampler.update();
                }
            }
            DataIDEvent<T> dataIDEvent = (DataIDEvent) event;
            long dataID = dataIDEvent.geDataID();
            LocalData<T> localData = new LocalData<T>((T[]) new Object[dataIDEvent.getDataSize()]);
            tempData.put(dataID, localData);
        } else if (event instanceof ItemInDataEvent) {
            ItemInDataEvent<T> newItemEvent = (ItemInDataEvent<T>) event;
            long dataID = newItemEvent.getDataID();
            T item = newItemEvent.getItem();
            // datID must exist in tempData
            LocalData<T> currData = tempData.get(dataID);
            if (tempData.containsKey(dataID)) {
                if (learner.contains(item)) {
                    if (currData.setItem(newItemEvent.getPosition(), newItemEvent.getItem(), false)) {
                        learn(currData);
                    }
                } else {
                    currData.setItem(newItemEvent.getPosition(), newItemEvent.getItem(), true);
                }
            } else {
                synchroStream.put(new RowResponse<T>(item, learner.getRow(item), learner.getContextRow(item), Long.toString(dataID)));
            }
        } else if (event instanceof RowResponse) {
            RowResponse response = (RowResponse) event;
            Long dataID = Long.parseLong(response.getKey());
            T item = (T) response.getItem();
            DoubleMatrix row = response.getRow();
            DoubleMatrix contextRow = response.getContextRow();
            LocalData<T> currData = tempData.get(dataID);
            if (currData.addExternalRow(item, row, contextRow)) {
                learn(currData);
                tempData.remove(dataID);
            }
        } else if (event instanceof RowUpdate) {
            RowUpdate update = (RowUpdate) event;
            learner.updateRow(update.getItem(), update.getGradient());
            learner.updateContextRow(update.getItem(), update.getContextGradient());
        }
        //FIXME optimize all this ugly stuff
        if (lastEventCount >= samplerCount && tempData.isEmpty() && !modelAckSent) {
            logger.info(String.format("LearnerProcessor-%d: finished after %d iterations", id, iterations));
            modelStream.put(new ModelUpdateEvent(null, null, true));
            modelAckSent = true;
        }
        return true;
    }

    private void learn(LocalData<T> currData) {
        T[] data = currData.data;
        Map<T, Map.Entry<DoubleMatrix, DoubleMatrix>> gradientUpdates = new HashMap<>(data.length);
        learner.setExternalRows(currData.externalRows);
        for (int pos = 0; pos < data.length; pos++) {
            T contextItem = data[pos];
            // Generate a random window for each item
            int reduced_window = org.jblas.util.Random.nextInt(window); // `b` in the original word2vec code
            // now go over all items from the (reduced) window, predicting each one in turn
            int start = Math.max(0, pos - window + reduced_window);
            int end = pos + window + 1 - reduced_window;
            //TODO shuffle data2 so that word pairs are not ordered by item: probably less collisions in the learning
            T[] data2 = Arrays.copyOfRange(data, start, end > data.length ? data.length : end);
            // Fixed a context item, iterate through items which have it in their context
            for (int pos2 = 0; pos2 < data2.length; pos2++) {
                T item = data2[pos2];
                // don't train on OOV items and on the `item` itself
                if (item != null && pos != pos2 + start) {
                    List<T> tempNegItems = ((NegativeSampler<T>) sampler).negItems();
                    List<T> negItems = new ArrayList<>(tempNegItems.size());
                    for (T negItem: tempNegItems) {
                        //FIXME if the condition is not met, word pair is not sent (the original word2vec does the same)
                        if (!negItem.equals(contextItem)) {
                            negItems.add(negItem);
                        }
                    }
                    gradientUpdates.putAll(learner.train(item, contextItem, negItems));
                    iterations++;
                    if (iterations % 1000000 == 0) {
                        logger.info(String.format("LearnerProcessor-%d: at %d iterations", id, iterations));
                    }
                }
            }
        }
        for (int pos = 0; pos < data.length; pos++) {
            T item = data[pos];
            if (!learner.contains(item)) {
                synchroStream.put(new RowUpdate(item, gradientUpdates.get(item).getKey(), gradientUpdates.get(item).getValue()));
            }
            modelStream.put(new ModelUpdateEvent(item, learner.getRow(item), false));
        }
    }

    @Override
    public Processor newProcessor(Processor processor) {
        LearnerProcessor p = (LearnerProcessor) processor;
        LearnerProcessor l = new LearnerProcessor(p.window, p.learner.copy(), p.sampler.copy(), p.samplerCount);
        l.setSeed(p.seed);
        l.modelStream = p.modelStream;
        l.synchroStream = p.synchroStream;
        l.iterations = p.iterations;
        l.modelAckSent = p.modelAckSent;
        l.lastEventCount = p.lastEventCount;
        l.firstDataReceived = p.firstDataReceived;
        l.tempData = new HashMap();
        for (Object key: p.tempData.keySet()) {
            l.tempData.put(key, ((LocalData<T>) p.tempData.get(key)).copy());
        }
        return l;
    }

    public void setModelStream(Stream modelStream) {
        this.modelStream = modelStream;
    }

    public void setSynchroStream(Stream synchroStream) {
        this.synchroStream = synchroStream;
    }

    public void setSeed(long seed) {
        this.seed = seed;
        Random.seed(seed);
        this.learner.setSeed(seed);
        this.sampler.setSeed(seed);
    }
}
