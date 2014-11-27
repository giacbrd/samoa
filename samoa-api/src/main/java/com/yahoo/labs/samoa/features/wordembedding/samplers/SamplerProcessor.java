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


import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.features.wordembedding.indexers.IndexUpdateEvent;
import com.yahoo.labs.samoa.features.wordembedding.tasks.OneContentEvent;
import com.yahoo.labs.samoa.topology.Stream;
import org.jblas.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public abstract class SamplerProcessor<T> implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(SamplerProcessor.class);

    private Sampler sampler;
    private long totalWords;
    private int id;
    private boolean firstSentenceReceived;
    private long totalSentences;
    private Stream learnerStream;
    private Stream modelStream;
    private short window;

    @Override
    public boolean process(ContentEvent event) {
        if (event instanceof IndexUpdateEvent) {
            if (event.isLastEvent()) {
                logger.info(this.getClass().getSimpleName()+"-{}: collected in vocabulary {} word types from a " +
                                "corpus of {} words.", id, sampler.size(), totalWords);
                return true;
            }
            IndexUpdateEvent update = (IndexUpdateEvent) event;
            totalWords += update.getWordCount();
            // Update local vocabulary
            Map<T, Long> vocabUpdate = update.getVocab();
            for(Map.Entry<T, Long> v: vocabUpdate.entrySet()) {
                sampler.put(v.getKey(), v.getValue());
            }
            Set<T> removeUpdate = update.getRemoveVocab();
            for(T word: removeUpdate) {
                sampler.remove(word);
            }
            // Update the noise distribution of negative sampling
//            if (wordUpdates % wordsPerUpdate == 0 && !(vocabUpdate.isEmpty() && removeUpdate.isEmpty()) && firstSentenceReceived) {
//                updateNegativeSampling();
//            }
            // When this type of events start to arrive, the vocabulary is already well filled
        } else if (event instanceof OneContentEvent) {
            if (event.isLastEvent()) {
                logger.info(this.getClass().getSimpleName()+"-{}: collected {} word types from a corpus of {} words " +
                                "and {} sentences", id, sampler.size(), totalWords, totalSentences);
                learnerStream.put(new SGNSItemEvent(null, null, null, true));
                return true;
            }
            if (!firstSentenceReceived) {
                firstSentenceReceived = true;
                logger.info(this.getClass().getSimpleName()+"-{}: starting sampling sentences, the vocabulary " +
                                "contains {} words and {} word types", id, totalWords, sampler.size());
//                if (totalWords > 0) {
//                    updateNegativeSampling();
//                }
            }
            totalSentences++;
            OneContentEvent content = (OneContentEvent) event;
            List<T> contentSentence = (List<T>) content.getContent();
            List<T> sentence = sampler.undersample(contentSentence);
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
                        generateTraining(word, wordC);
                    }
                }
            }
        }
        if (totalSentences % 1000 == 0 && totalSentences > 0) {
            logger.info(this.getClass().getSimpleName()+"-{}: after {} sentences, the vocabulary contains {} words " +
                            "and {} word types", id, totalSentences, totalWords, sampler.size());
        }
        return true;
    }

    protected abstract void generateTraining(T word, T wordC);
}
