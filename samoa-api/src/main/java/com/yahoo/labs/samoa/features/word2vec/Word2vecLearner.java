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


import com.yahoo.labs.samoa.features.word2vec.batch.Vocab;
import org.jblas.DoubleMatrix;

/**
 * Learn from words in the same context and produce a gradient.
 *
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public interface Word2vecLearner {

    /**
     * Learn from a word pair and return the gradient.
     * @param word The word to learn
     * @param wordC A word of the context
     * @param alpha Learning rate
     * @param labels Precomputed true labels (first element is 1, relative to {@code word}, the others 0, relative to negative sampled words)
     * @return
     */
    //FIXME paramters will be changed for sure!
    public DoubleMatrix trainPair(Vocab word, Vocab wordC, double alpha, DoubleMatrix labels);
}
