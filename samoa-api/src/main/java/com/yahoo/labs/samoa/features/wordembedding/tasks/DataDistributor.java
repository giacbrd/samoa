package com.yahoo.labs.samoa.features.wordembedding.tasks;

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
import com.yahoo.labs.samoa.topology.Stream;
import org.jblas.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * Distribute words and sentences.
 *
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class DataDistributor<T> implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(DataDistributor.class);
    private static final long serialVersionUID = 4821092756503632809L;

    private int id;
    private Stream itemStream;
    private Stream dataStream;
    private Stream dataAllStream;
    private boolean lastEventSent = false;

    @Override
    public void onCreate(int id) {
        this.id = id;
    }

    @Override
    public boolean process(ContentEvent event) {
//        logger.info("DataDistr "+event.isLastEvent() +" "+ ((OneContentEvent<List<T>>) event).getContent());
        //FIXME lastEventSent prevents sending more than one last event (it is necessary for the samoa-local bug)
        if (event.isLastEvent() && !lastEventSent) {
            //TODO only one indexer receives the last message
            itemStream.put(new OneContentEvent<T>(null, true));
            dataAllStream.put(new OneContentEvent<List<T>>(null, true));
            lastEventSent = true;
            return true;
        }
        OneContentEvent<List<T>> data = (OneContentEvent<List<T>>) event;
        List<T> items = data.getContent();
        for (T item: items) {
            itemStream.put(new OneContentEvent<T>(item, false, item.toString()));
        }
        dataStream.put(new OneContentEvent<List<T>>(items, false));
        return true;
    }

    public void setDataStream(Stream dataStream) {
        this.dataStream = dataStream;
    }

    public void setItemStream(Stream itemStream) {
        this.itemStream = itemStream;
    }

    public void setDataAllStream(Stream dataAllStream) {
        this.dataAllStream = dataAllStream;
    }


    @Override
    public Processor newProcessor(Processor processor) {
        DataDistributor p = (DataDistributor) processor;
        DataDistributor m = new DataDistributor();
        m.itemStream = p.itemStream;
        m.dataStream = p.dataStream;
        m.dataAllStream = p.dataAllStream;
        return m;
    }

}
