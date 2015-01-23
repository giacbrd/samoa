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

import com.github.javacliparser.ClassOption;
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
    private Stream outputStream;
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
    @Override
    public boolean process(ContentEvent event) {
        if (event.isLastEvent()) {
            logger.info("IndexerProcessor-{}: collected {} item types from a corpus of {} items and {} data samples",
                    id, indexer.size(), indexer.itemCount(), indexer.dataCount());
            outputStream.put(new IndexUpdateEvent(null, null, true));
            return true;
        }
        OneContentEvent content = (OneContentEvent) event;
        List<T> data = (List<T>) content.getContent();
        Map<T, Map.Entry<Long, Long>> update = indexer.add(data);
        Map<T, Long> removeVocab = indexer.getRemoved();
        long dataCount = indexer.dataCount();
        //FIXME this works because data increments by 1 at each add
        if (dataCount % 1000 == 0 && dataCount > 0) {
            logger.info("IndexerProcessor-{}: after {} data samples, processed {} items and {} item types",
                    id, indexer.dataCount(), indexer.itemCount(), indexer.size());
        }
        if (!update.isEmpty() || !removeVocab.isEmpty()) {
            // This "double" construction of the Set is necessary for making Kryo works
            outputStream.put(new IndexUpdateEvent(update, removeVocab, false));
        }
        return true;
    }


    @Override
    public Processor newProcessor(Processor processor) {
        IndexerProcessor p = (IndexerProcessor) processor;
        // FIXME passing the reference is not good if distributed?!
        IndexerProcessor i = new IndexerProcessor(p.indexer.copy());
        i.outputStream = p.outputStream;
        return i;
    }

    public void setOutputStream(Stream outputStream) {
        this.outputStream = outputStream;
    }
}
