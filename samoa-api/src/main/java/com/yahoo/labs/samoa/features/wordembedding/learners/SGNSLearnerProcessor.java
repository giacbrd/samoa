package com.yahoo.labs.samoa.features.wordembedding.learners;

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
import com.yahoo.labs.samoa.features.wordembedding.samplers.SGNSItemEvent;
import com.yahoo.labs.samoa.features.wordembedding.models.ModelUpdateEvent;
import com.yahoo.labs.samoa.topology.Stream;
import org.jblas.DoubleMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class SGNSLearnerProcessor<T> implements Processor {

    class LocalData<T> {
        final T contextItem;
        final List<T> negItems;
        final Map<T, DoubleMatrix> externalData;
        int dataCount = 0;
        final int totalData;
        LocalData(T contextItem, List<T> negItems, Map<T, DoubleMatrix> externalData, int totalData) {
            this.contextItem = contextItem;
            this.negItems = negItems;
            this.externalData = externalData;
            this.totalData = totalData;
        }
        LocalData<T> copy() {
            LocalData<T> l = new LocalData<T>(contextItem, negItems, externalData, totalData);
            l.dataCount = dataCount;
            return l;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(SGNSLearnerProcessor.class);

    private final SGNSLearner learner;
    private boolean lasteEventReceived = false;
    private long seed = 1;
    private Stream synchroStream;
    private Stream modelStream;
    private int id;
    private long iterations;
    //FIXME substitute with guava cache
    private Map<T, LocalData<T>> tempData = new HashMap<T, LocalData<T>>();

    public SGNSLearnerProcessor(Learner learner) {
        this.learner = (SGNSLearner) learner;
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        this.learner.initConfiguration();
        this.learner.setSeed(seed);
    }

    @Override
    public boolean process(ContentEvent event) {
        if (event.isLastEvent()) {
            lasteEventReceived = true;
            return true;
        }
        if (event instanceof SGNSItemEvent) {
            SGNSItemEvent itemPair = (SGNSItemEvent) event;
            T item = (T) itemPair.getItem();
            T contextItem = (T) itemPair.getContextItem();
            List<T> negItems = itemPair.getNegItems();
            LocalData<T> localData = new LocalData<T>(
                    contextItem, negItems, new HashMap<T, DoubleMatrix>(negItems.size() + 1), negItems.size() + 1);
            if (learner.contains(contextItem)) {
                localData.dataCount++;
            } else {
                synchroStream.put(new RowRequest(item, itemPair.getContextItem()));
            }
            for (T negItem: negItems) {
                if (learner.contains(negItem)) {
                    localData.dataCount++;
                } else {
                    synchroStream.put(new RowRequest(item, negItem));
                }
            }
            if (localData.dataCount >= localData.totalData) {
                learn(item, localData);
            } else {
                tempData.put(item, localData);
            }
        } else if (event instanceof RowRequest) {
            RowRequest request = (RowRequest) event;
            T requestedItem = (T) request.getRequestedItem();
            synchroStream.put(new RowResponse(
                    request.getSourceItem(), requestedItem, learner.getContextRow(requestedItem)));
        } else if (event instanceof RowResponse) {
            RowResponse response = (RowResponse) event;
            T sourceItem = (T) response.getSourceItem();
            T item = (T) response.getResponseItem();
            DoubleMatrix row = response.getResponseRow();
            LocalData<T> localData = tempData.get(sourceItem);
            localData.externalData.put(item, row);
            localData.dataCount++;
            if (localData.dataCount >= localData.totalData) {
                learn(sourceItem, localData);
            }
        } else if (event instanceof RowUpdate) {
            RowUpdate update = (RowUpdate) event;
            learner.updateContextRow(update.getItem(), update.getGradient());
        }
        if (lasteEventReceived && tempData.isEmpty()) {
            logger.info(String.format("SGNSLearnerProcessor-%d: finished after %d iterations", id, iterations));
            modelStream.put(new ModelUpdateEvent(null, null, true));
        }
        return true;
    }

    private void learn(T item, LocalData<T> localData) {
        T contextItem = (T) localData.contextItem;
        List<T> negItems = localData.negItems;
        Map<T, DoubleMatrix> gradientUpdates = learner.train(item, contextItem, negItems, localData.externalData);
        DoubleMatrix outRow = learner.getRow(item);
        iterations++;
        if (iterations % 1000000 == 0) {
            logger.info(String.format("SGNSLearnerProcessor-%d: at %d iterations", id, iterations));
        }
        if (!learner.contains(contextItem)) {
            synchroStream.put(new RowUpdate(contextItem, gradientUpdates.get(contextItem)));
        }
        for (T negItem: negItems) {
            if (!learner.contains(negItem)) {
                synchroStream.put(new RowUpdate(negItem, gradientUpdates.get(negItem)));
            }
        }
        modelStream.put(new ModelUpdateEvent(item, outRow, false));
        tempData.remove(item);
    }

//    @Override
//    public boolean process(ContentEvent event) {
//        if (event.isLastEvent()) {
//            logger.info(String.format("Learner-%d: finished after %d iterations", id, iterations));
//            modelStream.put(new ModelUpdateEvent(null, null, true));
//            return true;
//        }
//        SGNSItemEvent itemPair = (SGNSItemEvent) event;
//        T item = (T) itemPair.getItem();
//        learner.train(item, itemPair.getContextItem(), itemPair.getNegItems(), new HashMap());
//        DoubleMatrix row = learner.getRow(item);
//        iterations++;
//        if (iterations % 1000000 == 0) {
//            logger.info(String.format("Learner-%d: at %d iterations", id, iterations));
//        }
//        modelStream.put(new ModelUpdateEvent(item, row, false));
//        return true;
//    }

    @Override
    public Processor newProcessor(Processor processor) {
        SGNSLearnerProcessor p = (SGNSLearnerProcessor) processor;
        SGNSLearnerProcessor l = new SGNSLearnerProcessor(p.learner.copy());
        l.setSeed(p.seed);
        l.modelStream = p.modelStream;
        l.synchroStream = p.synchroStream;
        l.iterations = p.iterations;
        l.lasteEventReceived = p.lasteEventReceived;
        l.tempData = new HashMap<T, LocalData<T>>();
        for (Object item: p.tempData.keySet()) {
            l.tempData.put(item, ((LocalData<T>) p.tempData.get(item)).copy());
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
        this.learner.setSeed(seed);
    }
}
