package com.yahoo.labs.samoa.features.wordembedding.indexers;

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
import com.yahoo.labs.samoa.features.wordembedding.tasks.OneContentEvent;
import com.yahoo.labs.samoa.topology.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class IndexerProcessor<T> implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(IndexerProcessor.class);
    private static final long serialVersionUID = -7609177989430575101L;

    private int id;
    private Stream aggregationStream;
    private Stream learnerStream;
    private Indexer<T> indexer;

    public IndexerProcessor(Indexer indexer) {
        this.indexer = indexer;
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        this.indexer.initConfiguration();
    }

    //FIXME when remove words send also message to the learner/model
    //FIXME create packets of item updates, do not send <one item, removed items> per event
    @Override
    public boolean process(ContentEvent event) {
        if (event.isLastEvent()) {
            logger.info("IndexerProcessor-{}: collected {} item types from a corpus of {} items",
                    id, indexer.size(), indexer.itemTotalCount());
            aggregationStream.put(new IndexUpdateEvent(null, 0, null, true));
            return true;
        }
        OneContentEvent content = (OneContentEvent) event;
        T item = (T) content.getContent();
        Long newItemCount = indexer.add(item);
        Map<T, Long> removeVocab = indexer.getRemoved();
        long itemTotalCount = indexer.itemTotalCount();
        //FIXME this works because data increments by 1 at each add
        if (itemTotalCount > 0 && itemTotalCount % 1000000 == 0) {
            logger.info("IndexerProcessor-{}: processed {} items and {} item types",
                    id, itemTotalCount, indexer.size());
        }
        if (newItemCount > 0 || !removeVocab.isEmpty()) {
            IndexUpdateEvent indexUpdate = new IndexUpdateEvent(
                    (newItemCount > 0) ? item : null, newItemCount, removeVocab, false);
            aggregationStream.put(indexUpdate);
            if (learnerStream != null) {
                indexUpdate.setKey(item.toString());
                learnerStream.put(indexUpdate);
            }
        }
        return true;
    }

    public void setLearnerStream(Stream learnerStream) {
        this.learnerStream = learnerStream;
    }

    public void setAggregationStream(Stream aggregationStream) {
        this.aggregationStream = aggregationStream;
    }

    @Override
    public Processor newProcessor(Processor processor) {
        IndexerProcessor p = (IndexerProcessor) processor;
        // FIXME passing the reference is not good if distributed?!
        IndexerProcessor i = new IndexerProcessor(p.indexer.copy());
        i.aggregationStream = p.aggregationStream;
        i.learnerStream = p.learnerStream;
        return i;
    }

}
