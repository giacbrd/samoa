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

import java.util.HashMap;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class IndexUpdateEvent implements ContentEvent {

    private final HashMap<String, Vocab> vocab;
    private final HashMap<Integer, String> index2word;
    private long wordCount;
    private String key;
    private boolean isLastEvent;

    public IndexUpdateEvent(HashMap<String, Vocab> vocab, HashMap<Integer, String> index2word, long wordCount, boolean isLastEvent) {
        this.vocab = vocab;
        this.index2word = index2word;
        this.wordCount = wordCount;
        this.isLastEvent = isLastEvent;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public void setKey(String key) {

        this.key = key;
    }

    @Override
    public boolean isLastEvent() {
        return isLastEvent;
    }

    public long getWordCount() {
        return wordCount;
    }
    public HashMap<String, Vocab> getVocab() {
        return vocab;
    }

    public HashMap<Integer, String> getIndex2word() {
        return index2word;
    }

}
