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
import com.yahoo.labs.samoa.core.EntranceProcessor;
import com.yahoo.labs.samoa.core.Processor;
import org.apache.commons.collections.ResettableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple entrance processor which produces events from an iterator.
 *
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class IteratorEntrance<T> implements EntranceProcessor {

    private static final Logger logger = LoggerFactory.getLogger(IteratorEntrance.class);
    private static final long serialVersionUID = -3701928882036160733L;

    private int id;
    private ResettableIterator iterator;
    private boolean isFinished = false;
    private T lastElement;
    private long delay;
    private int noDelaySamples;
    private int samplesCount = 0;

    public IteratorEntrance(ResettableIterator iterator, long delay, int noDelaySamples) {
        this.iterator = iterator;
        this.delay = delay;
        this.noDelaySamples = noDelaySamples;
    }

    @Override
    public boolean process(ContentEvent event) {
        return false;
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        isFinished = false;
        this.iterator.reset();
    }

    @Override
    public Processor newProcessor(Processor processor) {
        IteratorEntrance p = (IteratorEntrance) processor;
        IteratorEntrance i = new IteratorEntrance(p.iterator, p.delay, p.noDelaySamples);
        i.samplesCount = p.samplesCount;
        i.isFinished = p.isFinished;
        return i;
    }

    @Override
    public boolean isFinished() {
        return isFinished;
    }

    @Override
    public boolean hasNext() {
        if (isFinished()) {
            return false;
        }
        if (iterator.hasNext()) {
            lastElement = (T) iterator.next();
        } else {
            isFinished = true;
            //FIXME samoa-local works when this is not commented! (otherwise it sends two last events)
            //return false;
        }
        return true;
    }

    @Override
    public ContentEvent nextEvent() {
        samplesCount++;
        if (samplesCount > noDelaySamples) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (!isFinished()) {
            logger.debug("Sending in output: " + lastElement.toString());
//            logger.info("Sending in output " + lastElement.toString());
            return new OneContentEvent(lastElement, false);
        } else {
            logger.info("Sending in output last event");
            return new OneContentEvent(null, true);
        }
    }
}
