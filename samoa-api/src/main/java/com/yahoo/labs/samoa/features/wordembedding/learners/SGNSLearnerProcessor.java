package com.yahoo.labs.samoa.features.wordembedding.learners;

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
import com.yahoo.labs.samoa.features.wordembedding.samplers.SGNSItemEvent;
import com.yahoo.labs.samoa.features.wordembedding.models.ModelUpdateEvent;
import com.yahoo.labs.samoa.topology.Stream;
import org.jblas.DoubleMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class SGNSLearnerProcessor<T> implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(SGNSLearnerProcessor.class);

    private final SGNSLearner learner;
    private long seed = 1;
    private Stream outputStream;
    private int id;
    private long iterations;

    public SGNSLearnerProcessor(Learner learner) {
        this.learner = (SGNSLearner) learner;
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        this.learner.initConfiguration();
        this.learner.setSeed(seed);
    }

    @Override
    public boolean process(ContentEvent event) {
        if (event.isLastEvent()) {
            logger.info(String.format("Learner-%d: finished after %d iterations", id, iterations));
            outputStream.put(new ModelUpdateEvent(null, null, true));
            return true;
        }
        SGNSItemEvent itemPair = (SGNSItemEvent) event;
        T item = (T) itemPair.getItem();
        learner.train(item, itemPair.getContextItem(), itemPair.geNegItems(), new HashMap());
        DoubleMatrix row = learner.getRow(item);
        iterations++;
        if (iterations % 1000000 == 0) {
            logger.info(String.format("Learner-%d: at %d iterations", id, iterations));
        }
        outputStream.put(new ModelUpdateEvent(item, row, false));
        return true;
    }

    @Override
    public Processor newProcessor(Processor processor) {
        SGNSLearnerProcessor p = (SGNSLearnerProcessor) processor;
        SGNSLearnerProcessor l = new SGNSLearnerProcessor(p.learner.copy());
        l.setSeed(p.seed);
        l.outputStream = p.outputStream;
        l.iterations = p.iterations;
        return l;
    }

    public void setOutputStream(Stream outputStream) {
        this.outputStream = outputStream;
    }

    public void setSeed(long seed) {
        this.seed = seed;
        this.learner.setSeed(seed);
    }
}
