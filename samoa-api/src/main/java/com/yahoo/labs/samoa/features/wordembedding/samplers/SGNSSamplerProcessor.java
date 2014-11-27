package com.yahoo.labs.samoa.features.wordembedding.samplers;

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

import com.github.javacliparser.FloatOption;
import com.github.javacliparser.IntOption;
import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.features.counters.Counter;
import com.yahoo.labs.samoa.features.counters.StreamSummary;
import com.yahoo.labs.samoa.features.wordembedding.tasks.OneContentEvent;
import com.yahoo.labs.samoa.features.wordembedding.indexers.IndexUpdateEvent;
import com.yahoo.labs.samoa.topology.Stream;
import org.jblas.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class SGNSSamplerProcessor<T> implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(SGNSSamplerProcessor.class);

    public IntOption wordsPerUpdateOption = new IntOption("wordsPerUpdateOption", 'u', "Number of word index updates" +
            "necessary for a new update of the table for negative sampling.", 100000);
    public IntOption minCountOption = new IntOption("minCount", 'm', "Ignore all words with total frequency lower than this.", 5);
    public FloatOption subsamplThrOption = new FloatOption("subsampleThreshold", 's', "Threshold in words sub-sampling, " +
            "the t parameter in the article.", 0.0);
    public FloatOption powerOption = new FloatOption("power", 'p', "The power parameter in the unigram distribution for" +
            "negative sampling.", 0.75);
    public IntOption windowOption = new IntOption("window", 'w', "The size of the window context for each word occurrence.", 5);
    public IntOption tableSizeOption = new IntOption("tableSize", 't', "The size of the table for negative sampling.",
            100000000);
    public IntOption negativeOption = new IntOption("negative", 'n', "The number of negative samples, the k parameter " +
            "in the article.", 10);
    public IntOption capacityOption = new IntOption("capacity", 'c', "The capacity of the counters for word counts " +
            "estimation.", Integer.MAX_VALUE);

    private Stream learnerStream;
    private Stream modelStream;
    private double subsamplThr;
    private Counter<T> vocab;
    private int id;
    long totalWords;
    private int wordsPerUpdate;
    private Double normFactor;
    private double power;
    private short window;
    private int[] table;
    private int tableSize;
    private long seed = 1;
    private int capacity;
    //FIXME use hash values instead of strings (from IndexerProcessor to the model)
    private Object[] index2word;
    private int negative;
    private boolean firstSentenceReceived;
    private int totalSentences;
    private long wordUpdates;

    /**
     * @param wordsPerUpdate Number of words after for update of the normalization factor (Z in the paper)
     * @param subsamplThr Subsampling threshold for frequent words (t in the paper)
     * @param power Exponent parameter for the unigram distribution for negative sampling
     * @param window
     *
     */
    public SGNSSamplerProcessor(int wordsPerUpdate, double subsamplThr, double power, short window,
                                int negative, int tableSize, int capacity, long seed) {
        this.wordsPerUpdate = wordsPerUpdate;
        this.subsamplThr = subsamplThr;
        this.power = power;
        this.window = window;
        this.negative = negative;
        this.tableSize = tableSize;
        this.capacity = capacity;
        this.seed = seed;
    }

    public SGNSSamplerProcessor() {};

    @Override
    public void onCreate(int id) {
        this.id = id;
        this.wordsPerUpdate = wordsPerUpdateOption.getValue();
        this.subsamplThr = subsamplThrOption.getValue();
        this.power = powerOption.getValue();
        this.window = (short) windowOption.getValue();
        this.negative = negativeOption.getValue();
        this.tableSize = tableSizeOption.getValue();
        this.capacity = capacityOption.getValue();
        totalWords = 0;
        normFactor = 1.0;
        wordUpdates = 0;
        vocab = new StreamSummary<T>(capacity);
        firstSentenceReceived = false;
        Random.seed(seed);
    }

    @Override
    public boolean process(ContentEvent event) {
        if (event instanceof IndexUpdateEvent) {
            if (event.isLastEvent()) {
                logger.info("SGNSSamplerProcessor-{}: collected in vocabulary {} word types from a corpus of {} words.",
                        id, vocab.size(), totalWords);
                return true;
            }
            IndexUpdateEvent update = (IndexUpdateEvent) event;
            totalWords += update.getWordCount();
            // Update local vocabulary
            Map<T, Long> vocabUpdate = update.getVocab();
            wordUpdates += vocabUpdate.size();
            for(Map.Entry<T, Long> v: vocabUpdate.entrySet()) {
                vocab.put(v.getKey(), v.getValue());
            }
            Set<T> removeUpdate = update.getRemoveVocab();
            for(T word: removeUpdate) {
                vocab.remove(word);
            }
            // Update the noise distribution of negative sampling
            if (wordUpdates % wordsPerUpdate == 0 && !(vocabUpdate.isEmpty() && removeUpdate.isEmpty()) && firstSentenceReceived) {
                updateNegativeSampling();
            }
        // When this type of events start to arrive, the vocabulary is already well filled
        } else if (event instanceof OneContentEvent) {
            if (event.isLastEvent()) {
                logger.info("SGNSSamplerProcessor-{}: collected {} word types from a corpus of {} words and {} sentences",
                        id, vocab.size(), totalWords, totalSentences);
                learnerStream.put(new SGNSItemEvent(null, null, null, true));
                return true;
            }
            if (!firstSentenceReceived) {
                firstSentenceReceived = true;
                logger.info("SGNSSamplerProcessor-{}: starting sampling sentences, the vocabulary contains {} words and {} word types",
                        id, totalWords, vocab.size());
                if (totalWords > 0) {
                    updateNegativeSampling();
                }
            }
            totalSentences++;
            OneContentEvent content = (OneContentEvent) event;
            List<T> contentSentence = (List<T>) content.getContent();
            List<T> sentence = sampleSentence(contentSentence);
            // Iterate through sentence words. For each word in context (wordC) predict a word which is in the range of |window - reduced_window|
            for (int pos = 0; pos < sentence.size(); pos++) {
                T wordC = sentence.get(pos);
                // Generate a random window for each word
                int reduced_window = Random.nextInt(window); // `b` in the original word2vec code
                // now go over all words from the (reduced) window, predicting each one in turn
                int start = Math.max(0, pos - window + reduced_window);
                int end = pos + window + 1 - reduced_window;
                List<T> sentence2 = sentence.subList(start, end > sentence.size() ? sentence.size() : end);
                // Fixed a context word, iterate through words which have it in their context
                for (int pos2 = 0; pos2 < sentence2.size(); pos2++) {
                    T word = sentence2.get(pos2);
                    // don't train on OOV words and on the `word` itself
                    if (word != null && pos != pos2 + start) {
                        sendWordPairs(word, wordC);
                    }
                }
            }
        }
        if (totalSentences % 1000 == 0 && totalSentences > 0) {
            logger.info("SGNSSamplerProcessor-{}: after {} sentences, the vocabulary contains {} words and {} word types",
                    id, totalSentences, totalWords, vocab.size());
        }
        return true;
    }

    /**
     * Send negative + 1 word pairs to learner, one refers to the true pair in the context, the others are from negative sampling
     * @param word
     * @param wordC
     */
    private void sendWordPairs(T word, T wordC) {
        // use this word (label = 1) + `negative` other random words not from this sentence (label = 0)
        ArrayList<T> wordsNeg = new ArrayList<T>(negative);
        for (int i = 0; i < negative; i++) {
            int neg = table[Random.nextInt(table.length)];
            //FIXME if the condition is not met, word pair is not sent (the original word2vec does the same)
            if (!index2word[neg].equals(wordC)) {
                wordsNeg.add((T) index2word[neg]);
            }
        }
        learnerStream.put(new SGNSItemEvent(word, wordC, wordsNeg, false));
    }

    // FIXME need a more fine and intelligent update, also to be asynchronous!
    private void updateNegativeSampling() {
        table = new int[tableSize]; //table (= list of words) of noise distribution for negative sampling
        //compute sum of all power (Z in paper)
        normFactor = 0.0;
        int vocabSize = vocab.size();
        Iterator<Map.Entry<T, Long>> vocabIter = vocab.iterator();
        while (vocabIter.hasNext()) {
            normFactor += Math.pow(vocabIter.next().getValue(), power);
        }
        logger.info("SGNSSamplerProcessor-{}: constructing a table with noise distribution from {} words", id, vocabSize);
        index2word = new Object[vocabSize];
        //go through the whole table and fill it up with the word indexes proportional to a word's count**power
        int widx = 0;
        vocabIter = vocab.iterator();
        Map.Entry<T, Long> vocabWord = vocabIter.next();
        long count = vocabWord.getValue();
        index2word[widx] = vocabWord.getKey();
        // normalize count^0.75 by Z
        double d1 = Math.pow(count, power) / normFactor;
        for (int tidx = 0; tidx < tableSize; tidx++) {
            table[tidx] = widx;
            if ((double)tidx / tableSize > d1) {
                widx++;
                if (vocabIter.hasNext()) {
                    vocabWord = vocabIter.next();
                    count = vocabWord.getValue();
                    index2word[widx] = vocabWord.getKey();
                    d1 += Math.pow(count, power) / normFactor;
                }

            }
            if (widx >= vocabSize) {
                widx = vocabSize - 1;
            }
        }
    }

    private ArrayList<T> sampleSentence(List<T> contentSentence) {
        ArrayList<T> sentence = new ArrayList<T>();
        for (T word: contentSentence) {
            if (vocab.containsKey(word)) {
                long count = vocab.get(word);
                // Subsampling probability
                double prob = Math.min(subsamplThr > 0 ? Math.sqrt(subsamplThr / ((double)count / totalWords)) : 1.0, 1.0);
                if (prob >= 1.0 || prob >= Random.nextDouble()) {
                    sentence.add(word);
                }
            }
        }
        modelStream.put(new OneContentEvent<List<T>>(sentence, false));
        return sentence;
    }

    public void setSeed(long seed) {
        this.seed = seed;
    }


    @Override
    public Processor newProcessor(Processor processor) {
        SGNSSamplerProcessor p = (SGNSSamplerProcessor) processor;
        SGNSSamplerProcessor w = new SGNSSamplerProcessor(p.wordsPerUpdate, p.subsamplThr, p.power, p.window,
                p.negative, p.tableSize, p.capacity, p.seed);
        w.wordsPerUpdateOption = p.wordsPerUpdateOption;
        w.subsamplThrOption = p.subsamplThrOption;
        w.powerOption = p.powerOption;
        w.windowOption = p.windowOption;
        w.negativeOption = p.negativeOption;
        w.tableSizeOption = p.tableSizeOption;
        w.capacityOption = p.capacityOption;
        w.learnerStream = p.learnerStream;
        w.modelStream = p.modelStream;
        w.totalWords = p.totalWords;
        // FIXME not good passing the reference if distributed?!
        w.vocab = p.vocab;
        w.normFactor = p.normFactor;
        w.table = p.table;
        w.index2word = p.index2word;
        w.totalSentences = p.totalSentences;
        w.wordUpdates = p.wordUpdates;
        return w;
    }

    public void setLearnerStream(Stream learnerStream) {
        this.learnerStream = learnerStream;
    }
    public void setModelStream(Stream modelStream) {
        this.modelStream = modelStream;
    }
}
