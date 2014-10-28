package com.yahoo.labs.samoa.features.word2vec;

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


/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class SentenceQueue implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(SentenceQueue.class);
    private final int maxSentences;
    private int id;
    private Stream outputStream;
    private long totalBytes = 0;
    ArrayDeque<String> queue;
    private String charset;


    public SentenceQueue(int maxSentences) {
        this.maxSentences = Math.max(1, maxSentences);
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        queue = new ArrayDeque<String>((int) this.maxSentences);
        totalBytes = 0;
        // FIXME assumes utf8
        charset = "UTF-8";
    }

    @Override
    public boolean process(ContentEvent event) {
        try {
            if (event.isLastEvent()) {
                if (queue.isEmpty()) {
                    outputStream.put(new OneContentEvent<String>(null, true));
                    return true;
                }
                pollSentence();
                return true;
            }
            OneContentEvent contentEvent = (OneContentEvent) event;
            Object content = contentEvent.getContent();
            if (content != null) {
                String sentence = (String) content;
                queue.addFirst(sentence);
                totalBytes += sentence.getBytes(charset).length;
            }
            while (queue.size() >= maxSentences) {
                pollSentence();
            }
            return true;
        } catch (UnsupportedEncodingException e) {
            // This can hardly happens
            e.printStackTrace();
            return false;
        }
    }

    private void pollSentence() throws UnsupportedEncodingException {
        String outSentence = queue.pollLast();
        if (outSentence != null) {
            totalBytes -= outSentence.getBytes(charset).length;
            outputStream.put(new OneContentEvent<String>(queue.peekLast(), false));
        }
    }

    @Override
    public Processor newProcessor(Processor processor) {
        SentenceQueue p = (SentenceQueue) processor;
        SentenceQueue s = new SentenceQueue(p.maxSentences);
        s.outputStream = p.outputStream;
        s.totalBytes = p.totalBytes;
        s.queue = p.queue;
        s.charset = p.charset;
        return s;
    }

    public void setOutputStream(Stream outputStream) {
        this.outputStream = outputStream;
    }
}
