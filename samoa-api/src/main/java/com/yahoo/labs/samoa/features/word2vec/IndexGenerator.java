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

    private int id;
    private Stream outputStream;
    private long totalSentences = 0;
    //FIXME use Guava CacheBuilder?!
    private LoadingCache<String, Long> vocab;
    private long totalWords = 0;
    private Set<String> removeVocab;


    public IndexGenerator(long totalWords, long totalSentences) {
        this.totalWords = totalWords;
        this.totalSentences = totalSentences;
    }

    public IndexGenerator() {
        this(0, 0);
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
        removeVocab = new HashSet<>();
        RemovalListener<String, Long> removalListener = new RemovalListener<String, Long>() {
            public void onRemoval(RemovalNotification<String, Long> removal) {
                removeVocab.add(removal.getKey());
            }
        };
        // FIXME the map size has to be carefully chosen (look at also at vocabs in other classes)
        vocab = CacheBuilder.newBuilder()
                .maximumSize(100000000)
                .expireAfterWrite(24, TimeUnit.HOURS)
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

    //FIXME time decay should be managed here?
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
        // FIXME this assumes words are divided by a space
        String[] sentence = ((String) content.getContent()).split(" ");
        totalWords += sentence.length;
        totalSentences++;
        // TODO send only currVocab to the learner? if not work directly on vocab
        HashMap<String, Long> currVocab = sentenceVocab(sentence);
        HashMap<String, Long> outVocab = new HashMap<String, Long>(currVocab.size());
        Iterator<Map.Entry<String, Long>> vocabIter = currVocab.entrySet().iterator();
        // FIXME here communications with redis for old words
        while (vocabIter.hasNext()) {
            Map.Entry<String, Long> vocabWord = vocabIter.next();
            String word = vocabWord.getKey();
            long newCount = 0;
            try {
                newCount = vocab.get(word) + vocabWord.getValue();
            } catch (ExecutionException e) {
                logger.error("Cache access error for word: " + word);
                e.printStackTrace();
            }
            vocab.put(word, newCount);
            outVocab.put(word, newCount);
        }
//        logger.info("total {} word types after removing those with count<{}", vocab.size(), min_count);
        if (totalSentences % 1000 == 0 && totalSentences > 0) {
            logger.info("IndexGenerator-{}: after {} sentences, processed {} words and {} word types",
                    id, totalSentences, totalWords, vocab.size());
        }
        // Do not remove just occurred words
        removeVocab.removeAll(outVocab.keySet());
        outputStream.put(new IndexUpdateEvent(outVocab, removeVocab, sentence.length, event.isLastEvent()));
        removeVocab = new HashSet<>();
        return true;
    }

    private HashMap<String, Long> sentenceVocab(String[] sentence) {
        HashMap<String, Long> currVocab = new HashMap<String, Long>(sentence.length);
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
        IndexGenerator i = new IndexGenerator(p.totalWords, p.totalSentences);
        i.outputStream = p.outputStream;
        // FIXME not good passing the reference if distributed?!
        i.vocab = p.vocab;
        i.removeVocab = p.removeVocab;
        return i;
    }

    public void setOutputStream(Stream outputStream) {
        this.outputStream = outputStream;
    }
}
