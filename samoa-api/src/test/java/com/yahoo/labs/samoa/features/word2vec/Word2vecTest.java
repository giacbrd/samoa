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

import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Word2vecTest extends TestCase {

    private static final Logger logger = LoggerFactory.getLogger(Word2vecTest.class);

    private ArrayList<List<String>> sentences;

    public void setUp() throws Exception {
        super.setUp();
        sentences = new ArrayList<List<String>>();
        sentences.add(new ArrayList<String>(Arrays.asList(new String[]{"anarchism", "originated", "as", "a", "term", "of", "abuse", "first", "used", "against", "early", "working", "class"})));
        sentences.add(new ArrayList<String>(Arrays.asList(new String[]{"radicals", "including", "the", "diggers", "of", "the", "english", "revolution", "and", "the", "sans", "culottes", "of", "the", "french", "revolution"})));
        sentences.add(new ArrayList<String>(Arrays.asList(new String[]{ "whilst", "the", "term", "is", "still", "used", "in", "a", "pejorative", "way", "to", "describe", "any", "act", "that", "used", "violent", "means", "to", "destroy", "the", "organization", "of", "society"})));
        sentences.add(new ArrayList<String>(Arrays.asList(new String[]{"it", "has", "also", "been", "taken", "up", "as", "a", "positive", "label", "by", "self", "defined", "anarchists", "the", "word", "anarchism", "is", "derived", "from", "the", "greek", "without", "archons", "ruler", "chief", "king"})));
        sentences.add(new ArrayList<String>(Arrays.asList(new String[]{"anarchism", "as", "a", "political", "philosophy", "is", "the", "belief", "that", "rulers", "are", "unnecessary", "and", "should", "be", "abolished", "although", "there", "are", "differing", "interpretations", "of", "what", "this", "means", "anarchism", "also", "refers", "to", "related", "social", "movements", "that", "advocate", "the", "elimination", "of", "authoritarian"})));
        sentences.add(new ArrayList<String>(Arrays.asList(new String[]{"institutions", "particularly", "the", "state", "the", "word", "anarchy", "as", "most", "anarchists", "use", "it", "does", "not", "imply", "chaos", "nihilism", "or", "anomie", "but", "rather", "a", "harmonious", "anti", "authoritarian", "society"})));
        sentences.add(new ArrayList<String>(Arrays.asList(new String[]{"in", "place", "of", "what", "are", "regarded", "as", "authoritarian", "political", "structures", "and", "coercive", "economic", "institutions", "anarchists", "advocate", "social", "relations", "based", "upon", "voluntary", "association", "of", "autonomous"})));
        sentences.add(new ArrayList<String>(Arrays.asList(new String[]{"individuals", "mutual", "aid", "and", "self", "governance", "while", "anarchism", "is", "most", "easily", "defined", "by", "what", "it", "is", "against", "anarchists", "also", "offer", "positive", "visions", "of", "what", "they", "believe"})));
        sentences.add(new ArrayList<String>(Arrays.asList(new String[]{"to", "be", "a", "truly", "free", "society", "however", "ideas", "about", "how", "an", "anarchist", "society", "might", "work", "vary", "considerably", "especially", "with", "respect", "to", "economics"})));
        sentences.add(new ArrayList<String>(Arrays.asList(new String[]{"there", "is", "also", "disagreement", "about", "how", "a", "free", "society", "might", "be", "brought", "about", "origins", "and", "predecessors"})));
        sentences.add(new ArrayList<String>(Arrays.asList(new String[]{"kropotkin", "and", "others", "argue", "that", "before", "recorded", "history", "human", "society", "was", "organized", "on", "anarchist", "principles", "most", "anthropologists", "follow", "kropotkin"})));
    }

    public void tearDown() throws Exception {
        sentences = null;
    }

    @Test
    public void test_initialization() {
//        SentencesIterable sit = new SentencesIterable(new File("/home/gberardi/samoa_data/text8.csv"));
//        logger.info("Learning...");
        Word2vec w2v = new Word2vec(sentences, 100, 0.025, (short)5, (short)0, 0.0, 1, 0.0, true, false, (short)10, false);
//        Word2vec w2v = new Word2vec(sit, 200, 0.025, (short)5, (short)5, 0.0, 1, 0.0001, true, false, (short)10, false);
//        ArrayList<ImmutablePair<String, Double>> results = w2v.most_similar(new ArrayList<String>(Arrays.asList(new String[]{"anarchism"})), new ArrayList<String>(), 10);
//        for (ImmutablePair<String, Double> p: results) {
//            logger.info(p.getLeft() + " " + Double.toString(p.getRight()));
//        }
    }

}