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

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class IndexProcessor implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(IndexProcessor.class);

    private int id;
    private Stream toLearnerStream;
    private long totalSentences;
    private HashMap<String, Vocab> vocab;
    private long totalWords;

    @Override
    public boolean process(ContentEvent event) {
        if (event.isLastEvent()) {
            logger.info("IndexProcessor-{}: collected {} word types from a corpus of {} words and {} sentences",
                    id, vocab.size(), totalWords, totalSentences);
            return true;
        }
        OneContentEvent content = (OneContentEvent) event;
        if (totalSentences % 10000 == 0) {
            logger.info("IndexProcessor-{}: after {} sentences, processed {} words and {} word types",
                    id, totalSentences, totalWords, vocab.size());
        }
        // FIXME this assumes words are divided by a space
        String[] sentence = ((String) content.getContent()).split(" ");
        totalWords += sentence.length;
        totalSentences++;
        // TODO send only currVocab to the learner? if not work directly on vocab
        HashMap<String, Vocab> currVocab = updateVocab(sentence);
        return true;
    }

    private HashMap<String, Vocab> updateVocab(String[] sentence) {
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
    public void onCreate(int id) {
        this.id = id;
        vocab = new HashMap<String, Vocab>();
        totalSentences = 0;
        totalWords = 0;
    }

    @Override
    public Processor newProcessor(Processor processor) {
        //FIXME
        return null;
    }

    public void setOutputStream(Stream toLearnerStream) {
        this.toLearnerStream = toLearnerStream;
    }
}
