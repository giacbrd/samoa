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

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class SGNSItemGenerator<T> extends SamplerProcessor<T> {

    private static final Logger logger = LoggerFactory.getLogger(SGNSItemGenerator.class);
    private static final long serialVersionUID = -4509340061994117991L;

    public SGNSItemGenerator(Sampler sampler, short window) {
        super(sampler, window);
    }

    /**
     * Send the training samples from a sentence
     * @param data
     */
    protected void generateTraining(List<T> data) {
        // Iterate through data items. For each item in context (contextItem) predict an item which is in the
        // range of |window - reduced_window|
        for (int pos = 0; pos < data.size(); pos++) {
            T contextItem = data.get(pos);
            // Generate a random window for each item
            int reduced_window = Random.nextInt(window); // `b` in the original word2vec code
            // now go over all items from the (reduced) window, predicting each one in turn
            int start = Math.max(0, pos - window + reduced_window);
            int end = pos + window + 1 - reduced_window;
            //TODO shuffle sentence2 so that word pairs are not ordered by item: probably less collisions in the learning
            List<T> sentence2 = data.subList(start, end > data.size() ? data.size() : end);
            // Fixed a context item, iterate through items which have it in their context
            for (int pos2 = 0; pos2 < sentence2.size(); pos2++) {
                T item = sentence2.get(pos2);
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
            //        try {
            //            Thread.sleep(0, 1);
            //        } catch (InterruptedException e) {
            //            e.printStackTrace();
            //        }
                    learnerStream.put(new ItemEvent(item, contextItem, negItems, false, item.toString()));
                }
            }
        }
    }

//    /**
//     * Send the training sample to a random learner by event key (for key-based stream distribution)
//     * @param item
//     * @param contextItem
//     */
//    @Override
//    protected void generateTraining(T item, T contextItem) {
//        List<T> tempNegItems = ((NegativeSampler<T>) sampler).negItems();
//        List<T> negItems = new ArrayList<>(tempNegItems.size());
//        int outKeyIndex = Random.nextInt(tempNegItems.size() + 2);
//        for (T negItem: tempNegItems) {
//            //FIXME if the condition is not met, word pair is not sent (the original word2vec does the same)
//            if (!negItem.equals(contextItem)) {
//                negItems.add(negItem);
//            }
//        }
//        String outKey;
//        switch (outKeyIndex) {
//            case 0:
//                outKey = item.toString();
//                break;
//            case 1:
//                outKey = contextItem.toString();
//                break;
//            default:
//                outKey = negItems.get(outKeyIndex - 2).toString();
//                break;
//        }
//        learnerStream.put(new ItemEvent(item, contextItem, negItems, false, outKey));
//    }

//    @Override
//    protected void generateTraining(T item, T contextItem) {
//        //FIXME the rule item->processorID should be global and static for the whole package
//        List<T> tempNegItems = ((NegativeSampler<T>) sampler).negItems();
//        List<T> negItems = new ArrayList<>(tempNegItems.size());
//        Multiset<Integer> hashes = HashMultiset.create(tempNegItems.size() + 2);
//        hashes.add(Math.abs(item.hashCode()) % parallelism);
//        hashes.add(Math.abs(contextItem.hashCode()) % parallelism);
//        for (T negItem: tempNegItems) {
//            //FIXME if the condition is not met, word pair is not sent (the original word2vec does the same)
//            if (!negItem.equals(contextItem)) {
//                negItems.add(negItem);
//                hashes.add(Math.abs(negItem.hashCode()) % parallelism);
//            }
//        }
//        // Set the message key with the most frequent item hashcode
//        String outKey = "";
//        int maxCount = -1;
//        for (Multiset.Entry entry: hashes.entrySet()) {
//            if (entry.getCount() > maxCount) {
//                outKey = Integer.toString((int) entry.getElement());
//                maxCount = entry.getCount();
//            }
//        }
//        learnerStream.put(new ItemEvent(item, contextItem, negItems, false, outKey));
//    }

    @Override
    public Processor newProcessor(Processor processor) {
        SGNSItemGenerator p = (SGNSItemGenerator) processor;
        SGNSItemGenerator w = new SGNSItemGenerator(p.sampler.copy(), p.window);
        w.learnerStream = p.learnerStream;
        w.learnerAllStream = p.learnerAllStream;
        w.modelStream = p.modelStream;
        w.dataCount = p.dataCount;
        w.firstDataReceived = p.firstDataReceived;
        w.setSeed(p.seed);
        return w;
    }
}
