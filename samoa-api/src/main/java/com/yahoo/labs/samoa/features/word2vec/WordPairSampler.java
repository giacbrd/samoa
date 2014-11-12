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
import org.jblas.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class WordPairSampler implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(WordPairSampler.class);
    private Stream outputStream;
    private double subsamplThr;
    private HashMap<String, Long> vocab;
    private int id;
    long totalWords;
    private int wordsPerUpdate;
    private Double normFactor;
    private double power;
    /* Minimum frequency for a word, in respect to the current vocabulary */
    private short minCount;
    private short window;
    private int[] table;
    private int tableSize;
    private String[] index2word;
    private int negative;
    private boolean firstSentenceReceived;
    private int totalSentences;

    /**
     * @param wordsPerUpdate Number of words after for update of the normalization factor (Z in the paper)
     * @param subsamplThr Subsampling threshold for frequent words (t in the paper)
     * @param power Exponent parameter for the unigram distribution for negative sampling
     * @param window
     *
     */
    public WordPairSampler(int wordsPerUpdate, short minCount, double subsamplThr, double power, short window, int negative, int tableSize) {
        this.wordsPerUpdate = wordsPerUpdate;
        this.minCount = minCount;
        this.subsamplThr = subsamplThr;
        this.power = power;
        this.window = window;
        this.negative = negative;
        this.tableSize = tableSize;
    }

    public WordPairSampler(int wordsPerUpdate) {
        this(wordsPerUpdate, (short) 5, 0.0, 0.75, (short) 5, 10, 100000000);
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        totalWords = 0;
        normFactor = 1.0;
        vocab = new HashMap<String, Long>(1000000);
        firstSentenceReceived = false;
    }

    @Override
    public boolean process(ContentEvent event) {
        if (event instanceof IndexUpdateEvent) {
            if (event.isLastEvent()) {
                logger.info("WordPairSampler-{}: collected in vocabulary {} word types from a corpus of {} words.",
                        id, vocab.size(), totalWords);
                return true;
            }
            IndexUpdateEvent update = (IndexUpdateEvent) event;
            totalWords += update.getWordCount();
            HashMap<String, Long> vocabUpdate = update.getVocab();
            for (Map.Entry<String, Long> vocabWord: vocabUpdate.entrySet()) {
                String word = vocabWord.getKey();
                long count = vocabWord.getValue();
                // Received a word deletion update
                if (count == 0 && vocab.containsKey(word)) {
                    vocab.remove(word);
                } else {
                    vocab.put(word, count);
                }
            }
            // Update the noise distribution of negative sampling
            if (totalWords % wordsPerUpdate == 0 && totalWords > 0 && firstSentenceReceived) {
                updateNegativeSampling();
            }
        // When this type of events start to arrive, the vocabulary is already well filled
        } else if (event instanceof OneContentEvent) {
            if (event.isLastEvent()) {
                logger.info("WordPairSampler-{}: collected {} word types from a corpus of {} words and {} sentences",
                        id, vocab.size(), totalWords, totalSentences);
                outputStream.put(new WordPairEvent(null, null, null, true));
                return true;
            }
            if (!firstSentenceReceived) {
                firstSentenceReceived = true;
                logger.info("WordPairSampler-{}: starting sampling sentences, processed {} words and {} word types",
                        id, totalWords, vocab.size());
                updateNegativeSampling();
            }
            totalSentences++;
            OneContentEvent content = (OneContentEvent) event;
            // FIXME this assumes words are divided by a space
            String[] contentSentence = ((String) content.getContent()).split(" ");
            ArrayList<String> sentence = sampleSentence(contentSentence);
            // Iterate through sentence words. For each word in context (wordC) predict a word which is in the range of |window - reduced_window|
            for (int pos = 0; pos < sentence.size(); pos++) {
                String wordC = sentence.get(pos);
                // Generate a random window for each word
                int reduced_window = Random.nextInt(window); // `b` in the original word2vec code
                // now go over all words from the (reduced) window, predicting each one in turn
                int start = Math.max(0, pos - window + reduced_window);
                int end = pos + window + 1 - reduced_window;
                List<String> sentence2 = sentence.subList(start, end > sentence.size() ? sentence.size() : end);
                // Fixed a context word, iterate through words which have it in their context
                for (int pos2 = 0; pos2 < sentence2.size(); pos2++) {
                    String word = sentence2.get(pos2);
                    // don't train on OOV words and on the `word` itself
                    if (word != null && pos != pos2 + start) {
                        sendWordPairs(word, wordC);
                    }
                }
            }
        }
        if (totalSentences % 1000 == 0 && totalSentences > 0) {
            logger.info("WordPairSampler-{}: after {} sentences, processed {} words and {} word types",
                    id, totalSentences, totalWords, vocab.size());
        }
        return true;
    }

    /**
     * Send negative + 1 word pairs to learner, one refers to the true pair in the context, the others are from negative sampling
     * @param word
     * @param wordC
     */
    private void sendWordPairs(String word, String wordC) {
        // use this word (label = 1) + `negative` other random words not from this sentence (label = 0)
        ArrayList<String> wordsNeg = new ArrayList<String>(negative);
        for (int i = 0; i < negative; i++) {
            int neg = table[Random.nextInt(table.length)];
            //FIXME if the condition is not met, word pair is not sent
            if (!index2word[neg].equals(wordC)) {
                wordsNeg.add(index2word[neg]);
            }
        }
        outputStream.put(new WordPairEvent(word, wordC, wordsNeg, false));
    }

    // FIXME need a more fine and intelligent update, also to be asynchronous!
    // FIXME avoid the use of the vocabulary, so this class dont need it anymore
    private void updateNegativeSampling() {
        table = new int[tableSize]; //table (= list of words) of noise distribution for negative sampling
        //compute sum of all power (Z in paper)
        normFactor = 0.0;
        int vocabSize = 0;
        for (Map.Entry<String, Long> vocabWord: vocab.entrySet()) {
            if (vocabWord.getValue() >= minCount) {
                normFactor += Math.pow(vocabWord.getValue(), power);
                vocabSize++;
            }
        }
        //FIXME logger.debug
        logger.info("WordPairSampler-{}: constructing a table with noise distribution from {} words", id, vocabSize);
        index2word = new String[vocabSize];
        //go through the whole table and fill it up with the word indexes proportional to a word's count**power
        int widx = 0;
        // normalize count^0.75 by Z
        //FIXME optimize the code till the function end
        Iterator<Map.Entry<String, Long>> vocabIter = vocab.entrySet().iterator();
        Map.Entry<String, Long> vocabWord = vocabIter.next();
        long count = vocabWord.getValue();
        while (count < minCount && vocabIter.hasNext()) {
            vocabWord = vocabIter.next();
            count = vocabWord.getValue();
        }
        index2word[widx] = vocabWord.getKey();
        double d1 = Math.pow(count, power) / normFactor;
        for (int tidx = 0; tidx < tableSize; tidx++) {
            table[tidx] = widx;
            if ((double)tidx / tableSize > d1) {
                widx++;
                if (vocabIter.hasNext()) {
                    vocabWord = vocabIter.next();
                    count = vocabWord.getValue();
                    index2word[widx] = vocabWord.getKey();
                    while (count < minCount && vocabIter.hasNext()) {
                        vocabWord = vocabIter.next();
                        count = vocabWord.getValue();
                        index2word[widx] = vocabWord.getKey();
                    }
                    d1 += Math.pow(count, power) / normFactor;
                }

            }
            if (widx >= vocabSize) {
                widx = vocabSize - 1;
            }
        }
    }

    private ArrayList<String> sampleSentence(String[] contentSentence) {
        ArrayList<String> sentence = new ArrayList<String>();
        for (String word: contentSentence) {
            if (vocab.containsKey(word)) {
                long count = vocab.get(word);
                // Subsampling probability
                double prob = Math.min(subsamplThr > 0 ? Math.sqrt(subsamplThr / ((double)count / totalWords)) : 1.0, 1.0);
                // Words sampling, take not too frequent or too infrequent words
                if (count >= minCount && (prob >= 1.0 || prob >= Random.nextDouble())) {
                    sentence.add(word);
                }
            }
        }
        return sentence;
    }

    @Override
    public Processor newProcessor(Processor processor) {
        WordPairSampler p = (WordPairSampler) processor;
        WordPairSampler w = new WordPairSampler(p.wordsPerUpdate, p.minCount, p.subsamplThr, p.power, p.window, p.negative, p.tableSize);
        w.outputStream = p.outputStream;
        w.totalWords = p.totalWords;
        // FIXME not good passing the reference if distributed?!
        w.vocab = p.vocab;
        w.normFactor = p.normFactor;
        w.table = p.table;
        w.index2word = p.index2word;
        w.totalSentences = p.totalSentences;
        return w;
    }

    public void setOutputStream(Stream outputStream) {
        this.outputStream = outputStream;
    }
}
