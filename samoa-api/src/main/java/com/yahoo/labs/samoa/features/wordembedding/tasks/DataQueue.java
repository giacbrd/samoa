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

import java.io.UnsupportedEncodingException;
import java.util.ArrayDeque;
import java.util.List;


/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class DataQueue<T> implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(DataQueue.class);
    private static final long serialVersionUID = 6700446592470187678L;

    private final int maxDataSamples;
    private int id;
    private Stream outputStream;
    /** Data bytes are computed on the string representation of items */
    private long totalBytes = 0;
    ArrayDeque<List<T>> queue;

    public DataQueue(int maxDataSamples) {
        this.maxDataSamples = Math.max(1, maxDataSamples);
        queue = new ArrayDeque<List<T>>((int) this.maxDataSamples);
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        totalBytes = 0;
    }

    @Override
    public boolean process(ContentEvent event) {
        try {
            if (event.isLastEvent()) {
                while (!queue.isEmpty()) {
                    pollData();
                }
                outputStream.put(new OneContentEvent<T>(null, true));
                return true;
            }
            OneContentEvent contentEvent = (OneContentEvent) event;
            Object content = contentEvent.getContent();
            if (content != null) {
                List<T> data = (List<T>) content;
                queue.addFirst(data);
                for (T item: data) {
                    totalBytes += ((Object) item).toString().getBytes().length;
                }
            }
            while (queue.size() >= maxDataSamples) {
                pollData();
            }
            return true;
        } catch (UnsupportedEncodingException e) {
            // This can hardly happen
            e.printStackTrace();
            return false;
        }
    }

    private void pollData() throws UnsupportedEncodingException {
        List<T> outData = queue.pollLast();
        if (outData != null) {
            for (T item: outData) {
                totalBytes -= ((Object) item).toString().getBytes().length;
            }
            outputStream.put(new OneContentEvent<List<T>>(outData, false));
        }
    }

    @Override
    public Processor newProcessor(Processor processor) {
        DataQueue p = (DataQueue) processor;
        DataQueue s = new DataQueue(p.maxDataSamples);
        s.outputStream = p.outputStream;
        s.totalBytes = p.totalBytes;
        s.queue = new ArrayDeque<List<T>>(p.queue);
        return s;
    }

    public void setOutputStream(Stream outputStream) {
        this.outputStream = outputStream;
    }
}
