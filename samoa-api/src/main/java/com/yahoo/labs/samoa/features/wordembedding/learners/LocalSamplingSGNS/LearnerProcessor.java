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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class LearnerProcessor<T> implements Processor {

    private static final long serialVersionUID = -2266852696732445189L;

    private static final Logger logger = LoggerFactory.getLogger(LearnerProcessor.class);

    private volatile SGNSLocalLearner learner;
    private short window;
    private Sampler<T> sampler;
    private long seed = 1;
    private Stream synchroStream;
    private Stream modelStream;
    private volatile ConcurrentLinkedQueue<RowUpdate> synchroEvents;
    private volatile ConcurrentLinkedQueue<ModelUpdateEvent> modelEvents;
    private int id;
    private long iterations;
    //FIXME substitute with guava cache
    private ConcurrentHashMap<Long, LocalData<T>> tempData = new ConcurrentHashMap<Long, LocalData<T>>();
    private int lastEventCount = 0;
    private int samplerCount;
    private boolean firstDataReceived = false;
    //FIXME is necessary to control last events? it should be always only one
    private boolean lastEventSent = false;

    public LearnerProcessor(short window, SGNSLocalLearner localLearner, Sampler<T> sampler, int samplerCount) {
        this.window = window;
        this.sampler = sampler;
        this.learner = localLearner;
        this.samplerCount = samplerCount;
        this.synchroEvents = new ConcurrentLinkedQueue<>();
        this.modelEvents = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        this.learner.initConfiguration();
        this.sampler.initConfiguration();
        setSeed(seed);
    }

    //FIXME sampler and learner have to use the same local index (no Space Saving necessary)
    @Override
    public boolean process(ContentEvent event) {
        // Empty the queues of outgoing events generated after a learning call
        pollEvents();
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
            //FIXME expensive hack for adding a new item to the local learner and the model
            modelStream.put(new ModelUpdateEvent(item, learner.getRow(item), false));
            return true;
        }
        if (event.isLastEvent()) {
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
            //logger.info(id + ": new data: "+dataID+", length: "+localData.data.length + ", data size:" + tempData.size());
            tempData.put(dataID, localData);
        } else if (event instanceof ItemInDataEvent) {
            ItemInDataEvent<T> newItemEvent = (ItemInDataEvent<T>) event;
            long dataID = newItemEvent.getDataID();
            T item = newItemEvent.getItem();
            int pos = newItemEvent.getPosition();
            //logger.info(id +": new item: "+item+" - data: "+dataID+", is local: "+tempData.containsKey(dataID));
            if (tempData.containsKey(dataID)) {
                LocalData<T> currData = tempData.get(dataID);
                if (currData.setItem(pos, item, learner.getRowRef(item))) {
                    long startTime = System.nanoTime();
                    asyncLearn(dataID, currData);
                    long endTime = System.nanoTime();
                    //logger.info(id + ": instant learn " + dataID + ", data size " + tempData.size());
                    //logger.info("TIME: " + (endTime-startTime)/1000000);
                }
            } else {
                synchroStream.put(new RowResponse<T>(
                        item, pos, learner.getRow(item), learner.getContextRow(item), Long.toString(dataID)));
            }
        } else if (event instanceof RowResponse) {
            RowResponse response = (RowResponse) event;
            Long dataID = Long.parseLong(response.getKey());
            T item = (T) response.getItem();
            int pos = response.getPosition();
            DoubleMatrix row = response.getRow();
            DoubleMatrix contextRow = response.getContextRow();
            if (tempData.containsKey(dataID)) {
                LocalData<T> currData = tempData.get(dataID);
                if (currData.setExternalItem(pos, item, row, contextRow)) {
//                        + " " + currData.externalRows.keySet() + "\n" + Arrays.toString(currData.data));
                    long startTime = System.nanoTime();
                    asyncLearn(dataID, currData);
                    long endTime = System.nanoTime();
                    //logger.info(id + ": learn " + dataID + ", data size " + tempData.size() + ", external rows " +
                    //        currData.externalRows.size());
                    //logger.info("TIME: " + (endTime - startTime) / 1000000 + " last events: " + lastEventCount);
                }
            } else {
                //FIXME this should never happen!
                logger.error(this.getClass().getSimpleName()+"-{}: the item row for \"" + item +
                        "\" has reached this learner before data " + dataID + " initialization.", id);
            }
        } else if (event instanceof RowUpdate) {
            RowUpdate<T> update = (RowUpdate<T>) event;
            T item = update.getItem();
            if (!learner.contains(item)) {
                logger.error(this.getClass().getSimpleName()+"-{}: wrong row update for item {}, this learner does " +
                        "not contain it", id, item);
                return false;
            }
            //logger.info(id +": update item: "+item);
            if (update.getRow() != null) {
//                if(item.toString().equals("and")) logger.info(update.getRow()+"");
                learner.updateRow(item, update.getRow());
            }
            if (update.getContextRow() != null) {
//                if(item.toString().equals("and")) logger.info(update.getContextRow()+" context");
                learner.updateContextRow(item, update.getContextRow());
            }
//            if(item.toString().equals("and")) logger.info("ROW "+learner.getRow(item));
            modelStream.put(new ModelUpdateEvent(item, learner.getRow(item), false));
        }
        //FIXME it is not guaranteed that all the model updates after row updates will be sent!
        if (lastEventCount >= samplerCount && !lastEventSent) {
            //FIXME wait indefinitely! here we should do a join on the learning threads
            //FIXME  commented because it does not work on storm, it won't execute the last learning iterations
            //while (!tempData.isEmpty()) {};
            logger.info(String.format("LearnerProcessor-%d: finished after %d iterations with %d items",
                    id, iterations, learner.size()));
            modelStream.put(new ModelUpdateEvent(null, null, true));
            lastEventSent = true;
        }
        return true;
    }

    private void pollEvents() {
        while (!synchroEvents.isEmpty()) {
            synchroStream.put(synchroEvents.poll());
        }
        while (!modelEvents.isEmpty()) {
            modelStream.put(modelEvents.poll());
        }
    }

//    private void learn(LocalData<T> currData) {
//        try {
//            Thread.sleep(10);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        for (T item: currData.externalRows.keySet()) {
//            DoubleMatrix rowUpdate = currData.externalRows.get(item).getKey();
//            DoubleMatrix contextUpdate = currData.externalRows.get(item).getValue();
//            if ((rowUpdate != null || contextUpdate != null) && (!rowUpdate.isEmpty() || !contextUpdate.isEmpty())) {
//                synchroStream.put(new RowUpdate(item, rowUpdate, contextUpdate, item.toString()));
//            }
//        }
//        for (T item: currData.rowHashes.keySet()) {
//            if (currData.rowChanged(item, learner.getRowRef(item))) {
//                modelStream.put(new ModelUpdateEvent(item, learner.getRow(item), false));
//            }
//        }
//    }

    private void learn(long dataID, LocalData<T> currData) {
        T[] data = currData.data;
        learner.setExternalRows(dataID, currData.externalRows);
        for (int pos = 0; pos < data.length; pos++) {
            T contextItem = data[pos];
            // Generate a random window for each item
            int reduced_window = org.jblas.util.Random.nextInt(window); // `b` in the original word2vec code
            // now go over all items from the (reduced) window, predicting each one in turn
            int start = Math.max(0, pos - window + reduced_window);
            int end = pos + window + 1 - reduced_window;
            T[] data2 = Arrays.copyOfRange(data, start, end > data.length ? data.length : end);
            // Fixed a context item, iterate through items which have it in their context
            for (int pos2 = 0; pos2 < data2.length; pos2++) {
                T item = data2[pos2];
                // don't train on OOV items and on the `item` itself
                if (item != null && pos != pos2 + start) {
                    List<T> tempNegItems = ((NegativeSampler<T>) sampler).negItems();
                    List<T> negItems = new ArrayList<>(tempNegItems.size());
                    for (T negItem: tempNegItems) {
                        //FIXME if the condition is not met, word pair is not learnt (the original word2vec does the same)
                        if (!negItem.equals(contextItem)) {
                            negItems.add(negItem);
                        }
//                        logger.info(item+" "+contextItem+" "+negItem+"");
                    }
                    if (negItems.isEmpty()) {
                        continue;
                    }
                    learner.train(dataID, item, contextItem, negItems);
                    incrIterations();
                }
            }
        }
        Map<T, Map.Entry<DoubleMatrix, DoubleMatrix>> gradients = learner.getGradients(dataID);
//        for (int pos = 0; pos < data.length; pos++) {
//            logger.info(currData.data+" "+currData.dataCount+" "+currData.missingData);
//        }
//                logger.info(gradientUpdates.size());
//                for (T key: gradientUpdates.keySet()) {
//                    logger.info(key+" "+ gradientUpdates.get(key).getKey()+" "+gradientUpdates.get(key).getValue());
//                }
        for (T item: gradients.keySet()) {
            DoubleMatrix rowUpdate = gradients.get(item).getKey();
            DoubleMatrix contextUpdate = gradients.get(item).getValue();
            if ((rowUpdate != null || contextUpdate != null) && (!rowUpdate.isEmpty() || !contextUpdate.isEmpty())) {
                synchroEvents.add(new RowUpdate(item, rowUpdate, contextUpdate, item.toString()));
            }
        }
        learner.clean(dataID);
        for (T item: currData.rowHashes.keySet()) {
            if (currData.rowChanged(item, learner.getRowRef(item))) {
                modelEvents.add(new ModelUpdateEvent(item, learner.getRow(item), false));
            }
        }
    }

    private synchronized void incrIterations() {
        iterations++;
        if (iterations % 1000000 == 0) {
            logger.info(String.format("LearnerProcessor-%d: at %d iterations", id, iterations));
        }
    }

    private void asyncLearn(final long dataID, final LocalData<T> currData){
        Runnable task = new Runnable() {
            @Override
            public void run() {
                try {
                    //logger.info("learning " + dataID);
                    learn(dataID, currData);
                    //logger.info("finished " + dataID);
                    //Thread.sleep(1);
                    tempData.remove(dataID);
                } catch (Exception e) {
                    logger.error("LearnerSubThread-{}: asynchronous learning interruption", id);
                    e.printStackTrace();
                }
            }
        };
        new Thread(task, "LearnerSubThread-"+id).start();
    }

    @Override
    public Processor newProcessor(Processor processor) {
        LearnerProcessor p = (LearnerProcessor) processor;
        LearnerProcessor l = new LearnerProcessor(p.window, p.learner.copy(), p.sampler.copy(), p.samplerCount);
        l.setSeed(p.seed);
        l.modelStream = p.modelStream;
        l.synchroStream = p.synchroStream;
        l.iterations = p.iterations;
        l.lastEventCount = p.lastEventCount;
        l.firstDataReceived = p.firstDataReceived;
        l.lastEventSent = p.lastEventSent;
        l.tempData = new ConcurrentHashMap();
        for (Object key: p.tempData.keySet()) {
            l.tempData.put(key, ((LocalData<T>) p.tempData.get(key)).copy());
        }
        l.synchroEvents = new ConcurrentLinkedQueue();
        Iterator iter = p.synchroEvents.iterator();
        while (iter.hasNext()) {
            l.synchroEvents.add(iter.next());
        }
        l.modelEvents = new ConcurrentLinkedQueue();
        iter = p.modelEvents.iterator();
        while (iter.hasNext()) {
            l.modelEvents.add(iter.next());
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
