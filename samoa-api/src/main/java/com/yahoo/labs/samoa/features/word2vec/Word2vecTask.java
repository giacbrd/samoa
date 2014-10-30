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
import com.github.javacliparser.IntOption;
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

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A task for testing word2vec on stream data.
 *
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class Word2vecTask implements Task, Configurable {

    private static final Logger logger = LoggerFactory.getLogger(Word2vecTask.class);

    public StringOption w2vNameOption = new StringOption("word2vecName", 'n', "Identifier of this Word2vec task",
            "Word2vecTask" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));
    public FileOption inputFileOption = new FileOption("inputFile", 'i', "File with the list of sentences," +
            " one sentence per line, words are divided by a space", null, "txt", false);
    public FlagOption saveModel = new FlagOption("saveModel", 's', "Save the model in the same path of the sentences file.");
    public IntOption precomputedSentences = new IntOption("precomputedSentences", 'p', "Number of sentences on which word" +
            "statistics are computed before starting the training on them", 5000);
    public IntOption wordPerSamplingUpdate = new IntOption("wordPerSamplingUpdate", 'u', "Number of indexed words" +
            "necessary for a new update of the table for negative sampling", 1000000);

    private TopologyBuilder builder;
    private Topology topology;
    private IteratorEntrance entrance;
    private Stream toIndexer;
    private IndexGenerator indexGenerator;
    private Stream toSampler2;
    private Stream toLearner;
    private WordPairSampler wordPairSampler;
    private Stream toBuffer;
    private SentenceQueue buffer;
    private Stream toSampler1;
    private Stream toDistributor;
    private MultiDistributor sentenceRouter;
    private Stream toModel;
    private Model model;

    @Override
    public void init() {

        // Produce data stream
        LineIterator iterator = null;
        try {
            // FIXME assumes utf8
            iterator = FileUtils.lineIterator(inputFileOption.getFile(), "UTF-8");
        } catch (java.io.IOException e) {
            logger.error("Error with file " + inputFileOption.getFile().getPath());
            e.printStackTrace();
            System.exit(1);
        }
        entrance = new IteratorEntrance(iterator);
        builder.addEntranceProcessor(entrance);
        toDistributor = builder.createStream(entrance);

        // Routing of sentences to indexer and to buffer
        sentenceRouter = new MultiDistributor(2);
        builder.addProcessor(sentenceRouter);
        builder.connectInputAllStream(toDistributor, sentenceRouter);
        toIndexer = builder.createStream(sentenceRouter);
        toBuffer = builder.createStream(sentenceRouter);
        sentenceRouter.setOutputStream(0, toIndexer);
        sentenceRouter.setOutputStream(1, toBuffer);

        // Buffer sentences before sending to distribution
        // FIXME parameter!
        buffer = new SentenceQueue(precomputedSentences.getValue());
        builder.addProcessor(buffer);
        builder.connectInputAllStream(toBuffer, buffer);
        toSampler1 = builder.createStream(buffer);
        buffer.setOutputStream(toSampler1);

        // Generate vocabulary
        indexGenerator = new IndexGenerator();
        builder.addProcessor(indexGenerator);
        builder.connectInputAllStream(toIndexer, indexGenerator);
        toSampler2 = builder.createStream(indexGenerator);
        indexGenerator.setOutputStream(toSampler2);

        // Sample and distribute word pairs
        // FIXME parameter!
        wordPairSampler = new WordPairSampler(wordPerSamplingUpdate.getValue());
        builder.addProcessor(wordPairSampler);
        builder.connectInputAllStream(toSampler1, wordPairSampler);
        builder.connectInputAllStream(toSampler2, wordPairSampler);
        toLearner = builder.createStream(wordPairSampler);
        wordPairSampler.setOutputStream(toLearner);

        // Learning
        Learner learner = new Learner(0.025, 200);
        builder.addProcessor(learner);
        builder.connectInputAllStream(toLearner, learner);
        toModel = builder.createStream(learner);
        learner.setOutputStream(toModel);

        // Model container
        model = new Model(new File(""));
        builder.addProcessor(model);
        builder.connectInputAllStream(toModel, model);

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
