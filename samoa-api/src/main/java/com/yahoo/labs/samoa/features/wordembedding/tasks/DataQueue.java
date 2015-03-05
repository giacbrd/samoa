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

import java.util.*;


/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class DataQueue<T> implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(DataQueue.class);
    private static final long serialVersionUID = 6700446592470187678L;

    private final int size;
    private int id;
    private Stream outputStream;
    /** Data bytes are computed on the string representation of items */
    private long totalBytes = 0;
    List<List<T>> queue;
    long delay;
    private int counter;
    private LinkedList<List<T>> outData;
    private long seed = 1;

    public DataQueue(int size, long delay, long seed) {
        this.delay = delay;
        this.seed = seed;
        this.size = Math.max(1, size);
        queue = new ArrayList<>(size);
        outData = new LinkedList<>();
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        totalBytes = 0;
        counter = 0;
        setSeed(seed);
    }

    @Override
    public boolean process(ContentEvent event) {
//        logger.info(this.getClass().getSimpleName()+"-{}: {} {}", id, queue.size(), event.isLastEvent());
        if (event.isLastEvent()) {
            //FIXME optimize this O(2n) operation for emptying the queue?
            if (!queue.isEmpty()) {
                Collections.shuffle(queue, new java.util.Random(seed));
                for (int i = 0; i < queue.size(); i++) {
                    if (queue.get(i) != null) {
                        outData.addFirst(queue.get(i));
                    }
                }
                queue.clear();
            }
            while (!outData.isEmpty()) {
                pollData();
            }
//            logger.info(this.getClass().getSimpleName()+"-{}: last event, {}", id, queue.size());
            outputStream.put(new OneContentEvent<T>(null, true));
            return true;
        }
        OneContentEvent contentEvent = (OneContentEvent) event;
        Object content = contentEvent.getContent();
        if (content != null) {
            List<T> data = (List<T>) content;
            // Fill the queue at the beginning
            if (counter < size) {
                queue.add(data);
                counter++;
            } else {
                int index = Random.nextInt(size);
                outData.addFirst(queue.get(index));
                queue.set(index, data);
            }
            for (T item: data) {
                totalBytes += ((Object) item).toString().getBytes().length;
            }
        }
        while (!outData.isEmpty()) {
            pollData();
        }
        return true;
    }

    private void pollData() {
        List<T> data = outData.pollLast();
        if (data != null) {
            for (T item: data) {
                totalBytes -= ((Object) item).toString().getBytes().length;
            }
//            logger.info(this.getClass().getSimpleName()+"-{}: poll, {}", id, queue.size());
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            outputStream.put(new OneContentEvent<List<T>>(data, false));
        }
    }

    public void setSeed(long seed) {
        this.seed = seed;
        Random.seed(seed);
    }

    @Override
    public Processor newProcessor(Processor processor) {
        DataQueue p = (DataQueue) processor;
        DataQueue s = new DataQueue(p.size, p.delay, p.seed);
        s.outputStream = p.outputStream;
        s.totalBytes = p.totalBytes;
        s.counter = p.counter;
        s.outData = new LinkedList<List<T>>();
        for (Object data: p.outData) {
            s.outData.add((List<T>) data);
        }
        s.queue = new ArrayList<List<T>>(p.queue.size());
        for (Object data: p.queue) {
            s.queue.add((List<T>) data);
        }
        return s;
    }

    public void setOutputStream(Stream outputStream) {
        this.outputStream = outputStream;
    }
}
