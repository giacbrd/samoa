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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * A simple entrance processor which produces events from an iterator.
 *
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class IteratorEntrance<T> implements EntranceProcessor {

    private static final Logger logger = LoggerFactory.getLogger(IteratorEntrance.class);
    private int id;
    private Iterator iterator;
    private boolean isFinished = false;
    private T lastElement;

    public IteratorEntrance(Iterator<T> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean process(ContentEvent event) {
        return false;
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        isFinished = false;
    }

    @Override
    public Processor newProcessor(Processor processor) {
        IteratorEntrance p = (IteratorEntrance) processor;
        return new IteratorEntrance(p.iterator);
    }

    @Override
    public boolean isFinished() {
        return isFinished;
    }

    @Override
    public boolean hasNext() {
        if (iterator.hasNext()) {
            lastElement = (T) iterator.next();
            return true;
        } else {
            isFinished = true;
            return false;
        }
    }

    @Override
    public ContentEvent nextEvent() {
        if (!isFinished()) {
            logger.debug("Sending in output " + lastElement.toString());
            return new OneContentEvent(lastElement, false);
        } else {
            return new OneContentEvent(null, true);
        }
    }
}
