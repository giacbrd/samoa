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
    private int id;

    private Stream itemStream;
    private Stream dataStream;
    private int parallelism;
    private Stream dataAllStream;

    public DataDistributor(int parallelism) {
        this.parallelism = parallelism;
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
    }

    @Override
    public boolean process(ContentEvent event) {
        if (event.isLastEvent()) {
            for (int i = 0; i < parallelism; i++) {
                itemStream.put(new OneContentEvent<List<T>>(null, true, Integer.toString(i)));
            }
            dataAllStream.put(new OneContentEvent<List<T>>(null, true));
            return true;
        }
        OneContentEvent<List<T>> data = (OneContentEvent<List<T>>) event;
        List<T> items = data.getContent();
        List<List<T>> outItems = initOutItems();
        for(T item: items) {
            outItems.get(Math.abs(item.hashCode()) % parallelism).add(item);
        }
        //FIXME is distribution to all indexers guaranteed?! NO
        for (int i = 0; i < outItems.size(); i++) {
            if (!outItems.get(i).isEmpty()) {
                itemStream.put(new OneContentEvent<List<T>>(outItems.get(i), false, Integer.toString(i)));
            }
        }
        dataStream.put(new OneContentEvent<List<T>>(items, false));
        return true;
    }

    private ArrayList<List<T>> initOutItems() {
        ArrayList<List<T>> outItems = new ArrayList<List<T>>(this.parallelism);
        for (int i = 0; i < parallelism; i++) {
            outItems.add(new ArrayList<T>());
        }
        return outItems;
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
        DataDistributor m = new DataDistributor(p.parallelism);
        m.itemStream = p.itemStream;
        m.dataStream = p.dataStream;
        m.dataAllStream = p.dataAllStream;
        return m;
    }

}
