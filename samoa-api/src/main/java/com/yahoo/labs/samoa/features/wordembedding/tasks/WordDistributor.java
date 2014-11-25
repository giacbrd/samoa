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

import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.topology.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * Distribute words and sentences.
 *
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class WordDistributor implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(WordDistributor.class);
    private int id;

    private Stream wordStream;
    private Stream sentenceStream;
    private int parallelism;
    private Stream sentenceAllStream;

    public WordDistributor(int parallelism) {
        this.parallelism = parallelism;
    }

    @Override
    public void onCreate(int id) {
        this.id = id;
    }

    @Override
    public boolean process(ContentEvent event) {
        if (event.isLastEvent()) {
            for (int i = 0; i < parallelism; i++) {
                wordStream.put(new OneContentEvent<List<String>>(null, true, Integer.toString(i)));
            }
            sentenceAllStream.put(new OneContentEvent<List<String>>(null, true));
            return true;
        }
        OneContentEvent<String> sentence = (OneContentEvent<String>) event;
        //FIXME assumes spaces in sentence splitting
        String[] words = sentence.getContent().trim().split(" ");
        List<List<String>> outWords = initOutWords();
        for(String word: words) {
            outWords.get(Math.abs(word.hashCode()) % parallelism).add(word);
        }
        //FIXME is distribution to all indexers guaranteed?!
        for (int i = 0; i < outWords.size(); i++) {
            wordStream.put(new OneContentEvent<List<String>>(outWords.get(i), event.isLastEvent(), Integer.toString(i)));
        }
        sentenceStream.put(new OneContentEvent<List<String>>(Arrays.asList(words), event.isLastEvent()));
        return true;
    }

    private ArrayList<List<String>> initOutWords() {
        ArrayList<List<String>> outWords = new ArrayList<List<String>>(this.parallelism);
        for (int i = 0; i < parallelism; i++) {
            outWords.add(new ArrayList<String>());
        }
        return outWords;
    }

    public void setSentenceStream(Stream sentenceStream) {
        this.sentenceStream = sentenceStream;
    }

    public void setWordStream(Stream wordStream) {
        this.wordStream = wordStream;
    }

    public void setSentenceAllStream(Stream sentenceAllStream) {
        this.sentenceAllStream = sentenceAllStream;
    }

    @Override
    public Processor newProcessor(Processor processor) {
        WordDistributor p = (WordDistributor) processor;
        WordDistributor m = new WordDistributor(p.parallelism);
        m.wordStream = p.wordStream;
        m.sentenceStream = p.sentenceStream;
        m.sentenceAllStream = p.sentenceAllStream;
        return m;
    }

}
