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

import com.github.javacliparser.*;
import com.yahoo.labs.samoa.features.wordembedding.indexers.CacheIndexer;
import com.yahoo.labs.samoa.features.wordembedding.indexers.Indexer;
import com.yahoo.labs.samoa.features.wordembedding.indexers.IndexerProcessor;
import com.yahoo.labs.samoa.features.wordembedding.learners.NaiveSGNS.LearnerProcessor;
import com.yahoo.labs.samoa.features.wordembedding.learners.SGNSLocalLearner;
import com.yahoo.labs.samoa.features.wordembedding.models.Model;
import com.yahoo.labs.samoa.features.wordembedding.samplers.NegativeSampler;
import com.yahoo.labs.samoa.features.wordembedding.samplers.SGNSItemGenerator;
import com.yahoo.labs.samoa.features.wordembedding.samplers.SGNSItemGenerator2;
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
 * A task for testing word2vec on stream data.
 *
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class Word2vecTask2 implements Task, Configurable {

    private static final Logger logger = LoggerFactory.getLogger(Word2vecTask2.class);
    private static final long serialVersionUID = -8679039729207387792L;

    public StringOption w2vNameOption = new StringOption("word2vecName", 'n', "Identifier of this Word2vec task",
            "Word2vecTask" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));
    public FileOption inputFileOption = new FileOption("inputFile", 'f', "File with the list of sentences," +
            " one sentence per line, words are divided by a space.", null, "txt", false);
    public IntOption precomputedSentences = new IntOption("precomputedSentences", 'p', "Number of sentences on which word" +
            "statistics are computed before starting the training on them.", 20000);
    public IntOption sentenceDelay = new IntOption("sentenceDelay", 'd', "Delay between each sentence sample, " +
            "in millisecond.", 0);
    public IntOption indexParallelism = new IntOption("indexParallelism", 'g', "Number of index generators on which " +
            "words are distributed.", 1);
    public IntOption samplerParallelism = new IntOption("samplerParallelism", 'z', "Number of word samplers, each one " +
            "contains a replica of the vocabulary, but it samples only a partition of the source; there is a sentence " +
            "buffer for each word sampler.", 1);
    public IntOption learnerParallelism = new IntOption("learnerParallelism", 'x', "Number of parallel learners.", 1);
    public IntOption seedOption = new IntOption("seed", 'r', "Seed for random number generation.", 1);
    public FileOption modelOutput = new FileOption("modelOutput", 'o', "Directory where to save the model.",
            null, null, true);
    public IntOption windowOption = new IntOption("window", 'w', "The size of the window context for each word " +
            "occurrence.", 5);

    public ClassOption indexerOption = new ClassOption("indexer", 'i', "Index generator class.",
            CacheIndexer.class, "CacheIndexer");
    public ClassOption itemSamplerOption = new ClassOption("sampler", 's', "Items sampler class.", NegativeSampler.class,
            "NegativeSampler");
    public ClassOption learnerOption = new ClassOption("learner", 'l', "LocalLearner class.", SGNSLocalLearner.class,
            "SGNSLocalLearner");


    private TopologyBuilder builder;
    private Topology topology;
    private IteratorEntrance entrance;
    private Stream toIndexer;
    private IndexerProcessor indexerProcessor;
    private Stream toSampler2;
    private Stream toLearner;
    private SGNSItemGenerator2 samplerProcessor;
    private Stream toBuffer;
    private DataQueue buffer;
    private Stream toSampler1;
    private Stream toDistributor;
    private DataDistributor wordsRouter;
    private Stream learnerToModel;
    private Model model;
    private Stream samplerToModel;
    private Stream toBufferAll;
    private Stream learnerToLearner;
    private Stream toAllLearner;

    @Override
    public void init() {

        //FIXME assumes spaces in sentence splitting and utf8 coding
        entrance = new IteratorEntrance(new SentenceIterator(inputFileOption.getFile(), " ", "UTF-8"),
                sentenceDelay.getValue(), precomputedSentences.getValue());
        builder.addEntranceProcessor(entrance);
        toDistributor = builder.createStream(entrance);

        // Routing of sentences to indexer and to buffer
        wordsRouter = new DataDistributor(indexParallelism.getValue());
        builder.addProcessor(wordsRouter);
        builder.connectInputAllStream(toDistributor, wordsRouter);
        toIndexer = builder.createStream(wordsRouter);
        toBuffer = builder.createStream(wordsRouter);
        // This is for sending to all buffers the last event, so to flush all the buffers
        toBufferAll = builder.createStream(wordsRouter);
        wordsRouter.setItemStream(toIndexer);
        wordsRouter.setDataStream(toBuffer);
        wordsRouter.setDataAllStream(toBufferAll);

        // Buffer sentences before sending to distribution
        buffer = new DataQueue(precomputedSentences.getValue() / samplerParallelism.getValue(), sentenceDelay.getValue());
        // Set the number of buffers equal to the number of word samplers
        builder.addProcessor(buffer, samplerParallelism.getValue());
        builder.connectInputShuffleStream(toBuffer, buffer);
        builder.connectInputAllStream(toBufferAll, buffer);
        toSampler1 = builder.createStream(buffer);
        buffer.setOutputStream(toSampler1);

        // Generate vocabulary
        indexerProcessor = new IndexerProcessor((Indexer) indexerOption.getValue());
        builder.addProcessor(indexerProcessor, indexParallelism.getValue());
        // The same word is sent to the same indexer
        builder.connectInputKeyStream(toIndexer, indexerProcessor);
        toSampler2 = builder.createStream(indexerProcessor);
        indexerProcessor.setOutputStream(toSampler2);

        // Sample and distribute word pairs
        samplerProcessor = new SGNSItemGenerator2((NegativeSampler) itemSamplerOption.getValue(),
                (short) windowOption.getValue(), learnerParallelism.getValue());
        samplerProcessor.setSeed(seedOption.getValue());
        builder.addProcessor(samplerProcessor, samplerParallelism.getValue());
        // Each word sampler receives from a single sentence buffer
        builder.connectInputShuffleStream(toSampler1, samplerProcessor);
        // All words samplers receive all the vocabulary
        builder.connectInputAllStream(toSampler2, samplerProcessor);
        toLearner = builder.createStream(samplerProcessor);
        toAllLearner = builder.createStream(samplerProcessor);
        samplerToModel = builder.createStream(samplerProcessor);
        samplerProcessor.setLearnerStream(toLearner);
        samplerProcessor.setLearnerAllStream(toAllLearner);
        samplerProcessor.setModelStream(samplerToModel);

        // Learning
        LearnerProcessor learner = new LearnerProcessor(
                (SGNSLocalLearner) learnerOption.getValue(), samplerParallelism.getValue());
        learner.setSeed(seedOption.getValue());
        builder.addProcessor(learner, learnerParallelism.getValue());
        builder.connectInputKeyStream(toLearner, learner);
        builder.connectInputAllStream(toAllLearner, learner);
        learnerToModel = builder.createStream(learner);
        learner.setModelStream(learnerToModel);
        // Learning parallelism
        learnerToLearner = builder.createStream(learner);
        learner.setSynchroStream(learnerToLearner);
        builder.connectInputKeyStream(learnerToLearner, learner);

        // Model container
        model = new Model(learnerParallelism.getValue(), modelOutput.getFile());
        builder.addProcessor(model);
        builder.connectInputAllStream(samplerToModel, model);
        builder.connectInputAllStream(learnerToModel, model);

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
