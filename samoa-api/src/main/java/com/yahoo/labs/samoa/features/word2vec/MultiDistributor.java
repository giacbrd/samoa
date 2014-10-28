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


/**
 * Simple message distributor to multiple outputs.
 *
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class MultiDistributor implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(MultiDistributor.class);
    private int id;
    private Stream[] outputStreams;


    public MultiDistributor(int numOutputs) {

        this.outputStreams = new Stream[numOutputs];
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
    }

    @Override
    public boolean process(ContentEvent event) {
        for (int i = 0; i < outputStreams.length; i++) {
            outputStreams[i].put(event);
        }
        return true;
    }

    @Override
    public Processor newProcessor(Processor processor) {
        MultiDistributor p = (MultiDistributor) processor;
        MultiDistributor m = new MultiDistributor(0);
        m.outputStreams = p.outputStreams;
        return m;
    }

    public void setOutputStream(int i, Stream outputStream) {
        this.outputStreams[i] = outputStream;
    }
}
