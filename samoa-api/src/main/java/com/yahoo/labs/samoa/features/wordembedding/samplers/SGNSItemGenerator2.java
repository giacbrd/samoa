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

import static com.yahoo.labs.samoa.utils.StreamDestination.getPIIndexForKey;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class SGNSItemGenerator2<T> extends SamplerProcessor2<T> {

    private static final Logger logger = LoggerFactory.getLogger(SGNSItemGenerator2.class);
    private static final long serialVersionUID = -4509340061994117991L;


    public SGNSItemGenerator2(Sampler sampler, short window, int learnerParallelism) {
        super(sampler, window, learnerParallelism);
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
                    List<T> tempNegItems = ((NegativeSampler<T>) samplers.get(Random.nextInt(learnerParallelism))).negItems();
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


    @Override
    public Processor newProcessor(Processor processor) {
        SGNSItemGenerator2 p = (SGNSItemGenerator2) processor;
        SGNSItemGenerator2 w = new SGNSItemGenerator2(p.sampler.copy(), p.window, p.learnerParallelism);
        w.samplers = new ArrayList<Sampler>(w.learnerParallelism);
        if (p.samplers != null && !samplers.isEmpty()) {
            for (int i = 0; i < learnerParallelism; i++) {
                w.samplers.add(((Sampler) p.samplers.get(i)).copy());
            }
        }
        w.learnerStream = p.learnerStream;
        w.learnerAllStream = p.learnerAllStream;
        w.modelStream = p.modelStream;
        w.dataCount = p.dataCount;
        w.firstDataReceived = p.firstDataReceived;
        w.setSeed(p.seed);
        return w;
    }
}
