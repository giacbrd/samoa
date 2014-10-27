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

import com.github.javacliparser.Configurable;
import com.github.javacliparser.FileOption;
import com.github.javacliparser.FlagOption;
import com.github.javacliparser.StringOption;
import com.yahoo.labs.samoa.tasks.Task;
import com.yahoo.labs.samoa.topology.ComponentFactory;
import com.yahoo.labs.samoa.topology.Stream;
import com.yahoo.labs.samoa.topology.Topology;
import com.yahoo.labs.samoa.topology.TopologyBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A task for testing word2vec on stream data.
 *
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class Word2vecTask implements Task, Configurable {

    private static final Logger logger = LoggerFactory.getLogger(Word2vecTask.class);

    private TopologyBuilder builder;

    public StringOption w2vNameOption = new StringOption("word2vecName", 'n', "Identifier of this Word2vec task",
            "Word2vecTask" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));
    public FileOption inputFileOption = new FileOption("inputFile", 'i', "File with the list of sentences," +
            " one sentence per line, words are divided by a space", null, "txt", false);
    public FlagOption saveModel = new FlagOption("saveModel", 's', "Save the model in the same path of the sentences file.");
    private Topology topology;
    private IteratorEntrance entrance;
    private Stream toIndexer;
    private IndexGenerator indexGenerator;
    private Stream toDistributorStream;
    private Stream toLearnerStream;
    private WordPairSampler wordPairSampler;

    @Override
    public void init() {

        // Produce data stream
        LineIterator iterator = null;
        try {
            iterator = FileUtils.lineIterator(inputFileOption.getFile(), "UTF-8");
        } catch (java.io.IOException e) {
            logger.error("Error with file " + inputFileOption.getFile().getPath());
            e.printStackTrace();
            System.exit(1);
        }
        entrance = new IteratorEntrance(iterator);
        builder.addEntranceProcessor(entrance);
        toIndexer = builder.createStream(entrance);

        // Generate vocabulary
        indexGenerator = new IndexGenerator();
        builder.addProcessor(indexGenerator);
        builder.connectInputAllStream(toIndexer, indexGenerator);
        toDistributorStream = builder.createStream(indexGenerator);
        indexGenerator.setOutputStream(toDistributorStream);

        // Sample and distribute word pairs
        wordPairSampler = new WordPairSampler(10000);
        builder.addProcessor(wordPairSampler);
        builder.connectInputAllStream(toDistributorStream, wordPairSampler);
        toLearnerStream = builder.createStream(wordPairSampler);
        wordPairSampler.setOutputStream(toLearnerStream);

        // build the topology
        topology = builder.build();
        logger.debug("Successfully built the topology");
    }

    @Override
    public Topology getTopology() {
        return topology;
    }

    @Override
    public void setFactory(ComponentFactory factory) {
        builder = new TopologyBuilder(factory);
        logger.debug("Sucessfully instantiating TopologyBuilder");
        builder.initTopology(w2vNameOption.getValue());
        logger.debug("Sucessfully initializing SAMOA topology with name {}", w2vNameOption.getValue());
    }
}
