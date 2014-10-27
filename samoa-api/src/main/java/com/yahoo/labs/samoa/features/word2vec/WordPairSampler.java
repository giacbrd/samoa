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
    private HashMap<String, Vocab> vocab;
    private int id;
    long totalWords;
    private int wordsPerUpdate;
    private Double normFactor;
    private double power;
    private short minCount;
    private short window;
    private int[] table;
    private int tableSize;
    private String[] index2word;
    private int negative;

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
        this(wordsPerUpdate, (short) 5, 1e-05, 0.75, (short) 5, 10, 100000000);
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        totalWords = 0;
        normFactor = 1.0;
    }

    @Override
    public boolean process(ContentEvent event) {
        if (event.isLastEvent()) {
            outputStream.put(new WordPairEvent(null, null, true, true));
            return true;
        }
        if (event instanceof IndexUpdateEvent) {
            IndexUpdateEvent update = (IndexUpdateEvent) event;
            totalWords += update.getWordCount();
            HashMap<String, Vocab> vocabUpdate = update.getVocab();
            for (Map.Entry<String, Vocab> vocabWord: vocabUpdate.entrySet()) {
                String word = vocabWord.getKey();
                Vocab v = vocabWord.getValue();
                // Received a word deletion
                if (v.count == 0 && vocab.containsKey(word)) {
                    vocab.remove(word);
                } else {
                    vocab.put(word, v);
                }
            }
        // When this type of events start to arrive, the vocabulary is already well filled
        } else if (event instanceof OneContentEvent) {
            // FIXME make sure the table is not empty!
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
        // Update the noise distribution of negative sampling
        if (totalWords % wordsPerUpdate == 0) {
            updateNegativeSampling();
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
        outputStream.put(new WordPairEvent(word, wordC, true, false));
        for (int i = 1; i < negative; i++) {
            int neg = table[Random.nextInt(table.length)];
            if (!index2word[neg].equals(word)) {
                outputStream.put(new WordPairEvent(word, index2word[neg], false, false));
            }
        }
    }

    // FIXME need a more fine and intelligent update, also to be asynchronous!
    // FIXME avoid the use of the vocabulary, so this class dont need it anymore
    private void updateNegativeSampling() {
        logger.info("WordPairSampler-{}: constructing a table with noise distribution from {} words", id, vocab.size());
        int vocabSize = vocab.size();
        table = new int[tableSize]; //table (= list of words) of noise distribution for negative sampling
        index2word = new String[vocabSize];
        //compute sum of all power (Z in paper)
        normFactor = 0.0;
        for (Map.Entry<String, Vocab> vocabWord: vocab.entrySet()) {
            normFactor += Math.pow(vocabWord.getValue().count, power);
        }
        //go through the whole table and fill it up with the word indexes proportional to a word's count**power
        int widx = 0;
        // normalize count^0.75 by Z
        Iterator<Map.Entry<String, Vocab>> vocabIter = vocab.entrySet().iterator();
        Map.Entry<String, Vocab> vocabWord = vocabIter.next();
        String word = vocabWord.getKey();
        Vocab v = vocabWord.getValue();
        double d1 = Math.pow(v.count, power) / normFactor;
        index2word[widx] = word;
        for (int tidx = 0; tidx < tableSize; tidx++) {
            table[tidx] = widx;
            if ((double)tidx / tableSize > d1) {
                widx++;
                if (vocabIter.hasNext()) {
                    vocabWord = vocabIter.next();
                    word = vocabWord.getKey();
                    v = vocabWord.getValue();
                    index2word[widx] = word;
                }
                d1 += Math.pow(v.count, power) / normFactor;
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
                Vocab v = vocab.get(word);
                // Subsampling probability
                double prob = Math.min(subsamplThr > 0 ? (1.0 - Math.sqrt(subsamplThr / v.count)) : 1.0, 1.0);
                // Words sampling
                if (v.count >= minCount && (prob >= 1.0 || prob >= Random.nextDouble())) {
                    sentence.add(word);
                }
            }
        }
        return sentence;
    }

    @Override
    public Processor newProcessor(Processor processor) {
        //FIXME
        return null;
    }

    public void setOutputStream(Stream outputStream) {
        this.outputStream = outputStream;
    }
}
