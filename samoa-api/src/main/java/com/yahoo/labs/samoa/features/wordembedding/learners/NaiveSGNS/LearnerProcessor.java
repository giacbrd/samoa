package com.yahoo.labs.samoa.features.wordembedding.learners.NaiveSGNS;

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
import com.yahoo.labs.samoa.features.wordembedding.learners.SGNSLocalLearner;
import com.yahoo.labs.samoa.features.wordembedding.samplers.ItemEvent;
import com.yahoo.labs.samoa.features.wordembedding.models.ModelUpdateEvent;
import com.yahoo.labs.samoa.topology.Stream;
import org.apache.commons.lang3.tuple.MutablePair;
import org.jblas.DoubleMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class LearnerProcessor<T> implements Processor {

    private static final long serialVersionUID = -1333212366354785743L;

    private static final Logger logger = LoggerFactory.getLogger(LearnerProcessor.class);

    private SGNSLocalLearner learner;
    private long seed = 1;
    private Stream synchroStream;
    private Stream modelStream;
    private int id;
    private long iterations;
    private boolean modelAckSent = false;
    //FIXME substitute with guava cache
    private Map<String, LocalData<T>> tempData = new HashMap<String, LocalData<T>>();
    private int lastEventCount = 0;
    private int samplerCount;

    public LearnerProcessor(SGNSLocalLearner localLearner, int samplerCount) {
        this.samplerCount = samplerCount;
        this.learner = localLearner;
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
            lastEventCount++;
        } else if (event instanceof ItemEvent) {
            ItemEvent itemPair = (ItemEvent) event;
            T item = (T) itemPair.getItem();
            T contextItem = (T) itemPair.getContextItem();
            List<T> negItems = itemPair.getNegItems();
            LocalData<T> localData = new LocalData<T>(item, contextItem, negItems,
                    new HashMap<T, MutablePair<DoubleMatrix, DoubleMatrix>>(negItems.size() + 1), negItems.size() + 1);
            String localKey = uniqueKey();
            //FIXME ugly code, but "recursion" must be put at the end because non asynchronous put() of samoa-local
            if (learner.contains(contextItem)) {
                localData.dataCount++;
            }
            for (T negItem: negItems) {
                if (learner.contains(negItem)) {
                    localData.dataCount++;
                }
            }
            if (localData.dataCount >= localData.totalData) {
//                logger.info(id+":learn1 "+item+"-"+contextItem+" "+localData.dataCount+" "+localData.totalData);
                learn(item, localData);
                tempData.remove(localKey);
            } else {
//                logger.info(id+":put "+item+"-"+contextItem+" SIZE:"+tempData.size());
                tempData.put(localKey, localData);
                if (!learner.contains(contextItem)) {
                    synchroStream.put(new RowRequest(localKey, item, itemPair.getContextItem()));
                }
                for (T negItem: negItems) {
                    if (!learner.contains(negItem)) {
                        synchroStream.put(new RowRequest(localKey, item, negItem));
                    }
                }
            }
        } else if (event instanceof RowRequest) {
            RowRequest request = (RowRequest) event;
            T requestedItem = (T) request.getRequestedItem();
//            logger.info(id+":request "+request.getSourceItem()+" "+requestedItem);
            synchroStream.put(new RowResponse(
                    request.getSourceKey(), request.getSourceItem(), requestedItem, learner.getContextRow(requestedItem)));
        } else if (event instanceof RowResponse) {
            RowResponse response = (RowResponse) event;
            String sourceKey = response.getSourceKey();
            T sourceItem = (T) response.getSourceItem();
            T newItem = (T) response.getResponseItem();
            DoubleMatrix row = response.getResponseRow();
//            logger.info(id+":get "+sourceItem+" "+newItem+" "+tempData.containsKey(sourceKey));
            LocalData<T> localData = tempData.get(sourceKey);
            // External rows are only, and all, context rows (actual context and negatives)
            localData.externalRows.put(newItem, new MutablePair<DoubleMatrix, DoubleMatrix>(null, row));
            localData.dataCount++;
            if (localData.dataCount >= localData.totalData) {
//                logger.info(id+":learn2 "+sourceItem+"-"+localData.contextItem+" "+localData.dataCount+" "+localData.totalData);
                learn(sourceItem, localData);
                tempData.remove(sourceKey);
            }
        } else if (event instanceof RowUpdate) {
            RowUpdate update = (RowUpdate) event;
//            logger.info(id+":update "+update.getItem());
            learner.updateContextRow(update.getItem(), update.getGradient());
        }
        //FIXME optimize all this ugly stuff
        if (lastEventCount >= samplerCount && tempData.isEmpty() && !modelAckSent) {
            logger.info(String.format("LearnerProcessor-%d: finished after %d iterations", id, iterations));
            modelStream.put(new ModelUpdateEvent(null, null, true));
            modelAckSent = true;
        }
        return true;
    }

    private String uniqueKey() {
        return UUID.randomUUID().toString();
    }

    private void learn(T item, LocalData<T> localData) {
        T contextItem = (T) localData.contextItem;
        List<T> negItems = localData.negItems;
        learner.setExternalRows(localData.externalRows);
        Map<T, MutablePair<DoubleMatrix, DoubleMatrix>> gradientUpdates = learner.train(item, contextItem, negItems);
        DoubleMatrix outRow = learner.getRow(item);
        iterations++;
        if (iterations % 1000000 == 0) {
            logger.info(String.format("LearnerProcessor-%d: at %d iterations", id, iterations));
        }
        if (!learner.contains(contextItem)) {
            synchroStream.put(new RowUpdate(contextItem, gradientUpdates.get(contextItem).getRight()));
        }
        for (T negItem: negItems) {
            if (!learner.contains(negItem)) {
                synchroStream.put(new RowUpdate(negItem, gradientUpdates.get(negItem).getRight()));
            }
        }
        modelStream.put(new ModelUpdateEvent(item, outRow, false));
    }

//    @Override
//    public boolean process(ContentEvent event) {
//        if (event.isLastEvent()) {
//            logger.info(String.format("Learner-%d: finished after %d iterations", id, iterations));
//            modelStream.put(new ModelUpdateEvent(null, null, true));
//            return true;
//        }
//        ItemEvent itemPair = (ItemEvent) event;
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
        LearnerProcessor p = (LearnerProcessor) processor;
        LearnerProcessor l = new LearnerProcessor(p.learner.copy(), p.samplerCount);
        l.setSeed(p.seed);
        l.lastEventCount = p.lastEventCount;
        l.modelStream = p.modelStream;
        l.synchroStream = p.synchroStream;
        l.iterations = p.iterations;
        l.modelAckSent = p.modelAckSent;
        l.tempData = new HashMap<String, LocalData<T>>();
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
        this.learner.setSeed(seed);
    }
}
