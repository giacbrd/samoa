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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class IndexGenerator implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(IndexGenerator.class);

    private int id;
    private Stream outputStream;
    private long totalSentences = 0;
    //FIXME use Guava CacheBuilder?!
    private HashMap<String, Long> vocab;
    private long totalWords = 0;


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
        // FIXME the map size has to be carefully chosen (look at also at vocabs in other classes)
        vocab = new HashMap<String, Long>(1000000);
    }

    //FIXME time decay should be managed here?
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
            if (vocab.containsKey(word)) {
                // We use Vocab (and not a map of <String, Integer>) just for the speed of this operation
                vocab.put(word, vocab.get(word)+1);
            } else {
                vocab.put(word, vocabWord.getValue());
            }
            outVocab.put(word, vocab.get(word));
        }
//        logger.info("total {} word types after removing those with count<{}", vocab.size(), min_count);
        if (totalSentences % 1000 == 0 && totalSentences > 0) {
            logger.info("IndexGenerator-{}: after {} sentences, processed {} words and {} word types",
                    id, totalSentences, totalWords, vocab.size());
        }
        outputStream.put(new IndexUpdateEvent(vocab, null, sentence.length, event.isLastEvent()));
        return true;
    }

    private HashMap<String, Long> sentenceVocab(String[] sentence) {
        HashMap<String, Long> currVocab = new HashMap<String, Long>(sentence.length);
        for (String word:sentence) {
            if (currVocab.containsKey(word)) {
                currVocab.put(word, currVocab.get(word)+1);
            } else {
                currVocab.put(word, (long)1);
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
        return i;
    }

    public void setOutputStream(Stream outputStream) {
        this.outputStream = outputStream;
    }
}
