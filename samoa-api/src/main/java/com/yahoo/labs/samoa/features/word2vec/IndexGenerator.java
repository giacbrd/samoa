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
    private HashMap<String, Vocab> vocab;
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
        vocab = new HashMap<String, Vocab>();
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
        if (totalSentences % 10000 == 0) {
            logger.info("IndexGenerator-{}: after {} sentences, processed {} words and {} word types",
                    id, totalSentences, totalWords, vocab.size());
        }
        // FIXME this assumes words are divided by a space
        String[] sentence = ((String) content.getContent()).split(" ");
        totalWords += sentence.length;
        totalSentences++;
        // TODO send only currVocab to the learner? if not work directly on vocab
        HashMap<String, Vocab> currVocab = sentenceVocab(sentence);
        HashMap<String, Vocab> outVocab = new HashMap<String, Vocab>(currVocab.size());
        Iterator<Map.Entry<String, Vocab>> vocabIter = currVocab.entrySet().iterator();
        // FIXME here communications with redis for old words
        while (vocabIter.hasNext()) {
            Map.Entry<String, Vocab> vocabWord = vocabIter.next();
            String word = vocabWord.getKey();
            if (vocab.containsKey(word)) {
                vocab.get(word).count++;
            } else {
                vocab.put(word, vocabWord.getValue());
            }
            outVocab.put(word, vocab.get(word));
        }
//        logger.info("total {} word types after removing those with count<{}", vocab.size(), min_count);
        outputStream.put(new IndexUpdateEvent(vocab, null, sentence.length, event.isLastEvent()));
        return true;
    }

    private HashMap<String, Vocab> sentenceVocab(String[] sentence) {
        HashMap<String, Vocab> currVocab = new HashMap<String, Vocab>(sentence.length);
        for (String word:sentence) {
            if (currVocab.containsKey(word)) {
                currVocab.get(word).count += 1;
            } else {
                currVocab.put(word, new Vocab(1));
            }
        }
        return currVocab;
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
