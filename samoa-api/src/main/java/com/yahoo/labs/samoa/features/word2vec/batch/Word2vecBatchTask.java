package com.yahoo.labs.samoa.features.word2vec.batch;

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

import com.github.javacliparser.Configurable;
import com.github.javacliparser.FileOption;
import com.github.javacliparser.FlagOption;
import com.github.javacliparser.StringOption;
import com.yahoo.labs.samoa.tasks.Task;
import com.yahoo.labs.samoa.topology.ComponentFactory;
import com.yahoo.labs.samoa.topology.Stream;
import com.yahoo.labs.samoa.topology.Topology;
import com.yahoo.labs.samoa.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A task for testing word2vec, the processor takes all the stream in memory and computes a standard word2vec learning
 *
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class Word2vecBatchTask  implements Task, Configurable {

    private static final Logger logger = LoggerFactory.getLogger(Word2vecBatchTask.class);

    private TopologyBuilder builder;

    public StringOption w2vNameOption = new StringOption("word2vecName", 'n', "Identifier of this Word2vec task",
            "Word2vecTask" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));
    public FileOption inputFileOption = new FileOption("inputFile", 'i', "File with the list of sentences," +
            " one sentence per line, words are divided by a space", null, "csv", false);
    public FlagOption saveModel = new FlagOption("saveModel", 's', "Save the model in the same path of the sentences file.");
    private Topology topology;
    private SentencesEntranceProcessor sentencesProcessor;
    private Stream nullStream;
    private Word2vecProcessor word2vecProcessor;

    /**
     * Initialize this SAMOA task,
     * i.e. create and connect ProcesingItems and Streams
     * and initialize the topology
     */
    @Override
    public void init() {

        //Learning
        word2vecProcessor = new Word2vecProcessor(inputFileOption.getFile(), saveModel.isSet());
        builder.addEntranceProcessor(word2vecProcessor);
        logger.debug("Successfully created Word2vec processor");
        nullStream = builder.createStream(word2vecProcessor);

        // build the topology
        topology = builder.build();
        logger.debug("Successfully built the topology");
    }

    /**
     * Return the final topology object to be executed in the cluster
     *
     * @return topology object to be submitted to be executed in the cluster
     */
    @Override
    public Topology getTopology() {
        return topology;
    }

    /**
     * Sets the factory.
     * TODO: propose to hide factory from task,
     * i.e. Task will only see TopologyBuilder,
     * and factory creation will be handled by TopologyBuilder
     *
     * @param factory the new factory
     */
    @Override
    public void setFactory(ComponentFactory factory) {
        builder = new TopologyBuilder(factory);
        logger.debug("Sucessfully instantiating TopologyBuilder");
        builder.initTopology(w2vNameOption.getValue());
        logger.debug("Sucessfully initializing SAMOA topology with name {}", w2vNameOption.getValue());
    }
}
