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

import com.github.javacliparser.IntOption;
import com.google.common.cache.*;
import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.topology.Stream;
import org.apache.commons.collections.FastHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class IndexGenerator implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(IndexGenerator.class);

    public IntOption cacheSizeOption = new IntOption("cacheSize", 'c', "The max number of elements in the main memory " +
            "cache.", 100000000);
    public IntOption wordExpiryOption = new IntOption("wordExpiry", 'e', "If a word does not occur in the stream after " +
            "this time (in minutes), it is removed from the index and the model.", 24*60);
    public IntOption minCountOption = new IntOption("minCount", 'm', "Ignore all words with total frequency lower than this.", 5);

    private int id;
    private Stream outputStream;
    private long totalSentences = 0;
    private long cacheSize;
    private long wordExpiry;
    private LoadingCache<String, Long> vocab;
    private long totalWords = 0;
    private Map<String, Long> removeVocab;
    private short minCount;


    public IndexGenerator(long cacheSize, long wordExpiry, short minCount) {
        this.cacheSize = cacheSize;
        this.wordExpiry = wordExpiry;
        this.minCount = minCount;
    }

    public IndexGenerator() {};

    @Override
    public void onCreate(int id) {
        this.id = id;
        this.cacheSize = cacheSizeOption.getValue();
        this.wordExpiry = wordExpiryOption.getValue();
        this.minCount = (short) minCountOption.getValue();
        removeVocab = new HashMap<>();
        RemovalListener<String, Long> removalListener = new RemovalListener<String, Long>() {
            public void onRemoval(RemovalNotification<String, Long> removal) {
                if (removal.getCause() != RemovalCause.REPLACED) {
                    logger.debug("Remove from cache: " + removal.getKey() + ". Cause: " + removal.getCause().toString());
                    removeVocab.put(removal.getKey(), removal.getValue());
                }
            }
        };
        vocab = CacheBuilder.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterWrite(wordExpiry, TimeUnit.MINUTES)
//                .maximumWeight(125000000)
//                .weigher(
//                        new Weigher<String, Long>() {
//                            public int weigh(String key, Long value) {
//                                return key.length();
//                            }
//                        })
                .removalListener(removalListener)
                .build(
                        new CacheLoader<String, Long>() {
                            public Long load(String key) {
                                return (long) 0;
                            }
                        });
    }

    //FIXME when remove words send also message to the learner/model
    @Override
    public boolean process(ContentEvent event) {
        if (event.isLastEvent()) {
            logger.info("IndexGenerator-{}: collected {} word types from a corpus of {} words and {} sentences",
                    id, vocab.size(), totalWords, totalSentences);
            outputStream.put(new IndexUpdateEvent(null, null, 0, true));
            return true;
        }
        OneContentEvent content = (OneContentEvent) event;
        List<String> sentence = (List<String>) content.getContent();
        totalWords += sentence.size();
        totalSentences++;
        long wordCount = 0;
        HashMap<String, Long> currVocab = sentenceVocab(sentence);
        HashMap<String, Long> outVocab = new HashMap<String, Long>(currVocab.size());
        Iterator<Map.Entry<String, Long>> vocabIter = currVocab.entrySet().iterator();
        while (vocabIter.hasNext()) {
            Map.Entry<String, Long> vocabWord = vocabIter.next();
            String word = vocabWord.getKey();
            long currCount = vocabWord.getValue();
            long newCount = 0;
            try {
                newCount = vocab.get(word) + currCount;
            } catch (ExecutionException e) {
                logger.error("Cache access error for word: " + word);
                e.printStackTrace();
            }
            vocab.put(word, newCount);
            if (newCount >= minCount) {
                outVocab.put(word, newCount);
                wordCount += currCount;
            }
        }
//        logger.info("total {} word types after removing those with count<{}", vocab.size(), min_count);
        if (totalSentences % 1000 == 0 && totalSentences > 0) {
            logger.info("IndexGenerator-{}: after {} sentences, processed {} words and {} word types",
                    id, totalSentences, totalWords, vocab.size());
        }
        // Do not remove just occurred words
        removeVocab.keySet().removeAll(outVocab.keySet());
        long removeCount = 0;
        for (String word: removeVocab.keySet()) {
            removeCount += removeVocab.get(word);
        }
        if (wordCount != 0 || removeCount != 0) {
            outputStream.put(new IndexUpdateEvent(outVocab, removeVocab.keySet(), wordCount - removeCount, event.isLastEvent()));
        }
        removeVocab = new HashMap<>();
        return true;
    }

    private HashMap<String, Long> sentenceVocab(List<String> sentence) {
        HashMap<String, Long> currVocab = new HashMap<String, Long>(sentence.size());
        for (String word:sentence) {
            if (currVocab.containsKey(word)) {
                currVocab.put(word, currVocab.get(word) + 1);
            } else {
                currVocab.put(word, (long) 1);
            }
        }
        return currVocab;
    }


    @Override
    public Processor newProcessor(Processor processor) {
        IndexGenerator p = (IndexGenerator) processor;
        IndexGenerator i = new IndexGenerator(p.cacheSize, p.wordExpiry, p.minCount);
        i.cacheSizeOption = p.cacheSizeOption;
        i.wordExpiryOption = p.wordExpiryOption;
        i.minCountOption = p.minCountOption;
        i.totalWords = p.totalWords;
        i.totalSentences = p.totalSentences;
        i.outputStream = p.outputStream;
        // FIXME passing the reference is not good if distributed?!
        i.vocab = p.vocab;
        i.removeVocab = p.removeVocab;
        return i;
    }

    public void setOutputStream(Stream outputStream) {
        this.outputStream = outputStream;
    }
}
