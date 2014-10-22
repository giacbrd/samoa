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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.util.*;

import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jblas.DoubleMatrix;
import org.jblas.MatrixFunctions;
import org.jblas.util.*;
import org.jblas.util.Random;
import org.slf4j.*;
import org.slf4j.helpers.FormattingTuple;

import static org.jblas.Geometry.normalize;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class Word2vec {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Word2vec.class);

    ArrayList<String> index2word;
    Iterable<List<String>> sentences;
    int layer1_size = 100;
    double alpha = 0.025;
    short window = 5;
    short min_count = 5;
    double sample = 0;
    int seed = 1;
    double min_alpha = 0.0001;
    boolean sg = true;
    boolean hs = true;
    short negative = 0;
    boolean cbow_mean = false;
    HashMap<String, Vocab> vocab;
    int[] table;
    /** Matrix W of word representations **/
    DoubleMatrix syn0;
    /** Matrix W normalized by norm2 **/
    DoubleMatrix syn0norm;
    /** Matrix C of word contexts (each row is the representation of a word when it is in a context) **/
    DoubleMatrix syn1neg;

    public Word2vec(Iterable<List<String>> sentences) {
        this.sentences = sentences;
        this.vocab = new HashMap<String, Vocab>();
        this.index2word = new ArrayList<String>();
        build_vocab(sentences);
        train(sentences);
    }

    public Word2vec(Iterable<List<String>> sentences, int size, double alpha, short window, short min_count,
                    double sample, int seed, double min_alpha, boolean sg, boolean hs, short negative,
                    boolean cbow_mean) {
        this.sentences = sentences;
        this.layer1_size = size;
//        if layer1_size % 4 != 0:
//            logger.warning("consider setting layer layer1_size to a multiple of 4 for greater performance")
        this.alpha = alpha;
        this.window = window;
        this.min_count = min_count;
        this.sample = sample;
        this.seed = seed;
        this.min_alpha = min_alpha;
        this.sg = sg;
        this.hs = hs;
        this.negative = negative;
        this.cbow_mean = cbow_mean;
        this.vocab = new HashMap<String, Vocab>();
        this.index2word = new ArrayList<String>();
        build_vocab(sentences);
        train(sentences);
    }

    /**
     * Update the model's neural weights from a sequence of sentences (can be a once-only generator stream).
     * Each sentence must be a list of unicode strings.
     * @param sentences
     * @param word_count
     */
    private long train(Iterable<List<String>> sentences, long total_words, long word_count) {
        logger.info("training model on {} vocabulary and {} features, " +
                "using 'skipgram'={} 'hierarchical softmax'={} 'subsample'={} and 'negative sampling'={}",
                        vocab.size(), layer1_size, sg, hs, sample, negative);

        if (vocab == null || vocab.isEmpty()) {
            throw new RuntimeException("you must first build vocabulary before training the model");
        }

        double next_report = 5000;
        long start = System.currentTimeMillis();
        long elapsed = 0;
        if (total_words <= 0) {
            total_words = 0;
            for (Vocab v: vocab.values()) {
                total_words += (long) (v.sample_probability * v.count);
            }
        }
        // Learning
        for (List<String> temp_sentence: sentences) {
            // convert input strings to Vocab objects (eliding OOV/downsampled words), and start filling the jobs queue
            // avoid calling random_sample() where prob >= 1, to speed things up a little
            ArrayList<Vocab> sentence = new ArrayList<Vocab>();
            for (String word: temp_sentence) {
                if (vocab.containsKey(word) && (vocab.get(word).sample_probability >= 1.0 || vocab.get(word).sample_probability >= Random.nextDouble())) {
                    sentence.add(vocab.get(word));
                }
            }
            double alpha = Math.max(min_alpha, this.alpha * (1 - ((double) word_count / total_words)));
            word_count += train_sentence(sentence, alpha);
            elapsed = System.currentTimeMillis() - start;
            if (elapsed >= next_report) {
                logger.info("PROGRESS: at {} words ({} of total words), alpha {}",
                        word_count, (double)word_count / total_words, alpha);
                next_report = elapsed + 5000;
            }
        }
        logger.info("PROGRESS: at {} words ({} of total words), alpha {}",
                word_count, (double)word_count / total_words, alpha);
        return word_count;
    }

    private void train(Iterable<List<String>> sentences) {
        train(sentences, 0, 0);
    }

    private int train_sentence(List<Vocab> sentence, double alpha) {
        // Precompute labels used in the computation of the gradient (the true outputs)
        DoubleMatrix labels = null;
        if (negative > 0) {
            labels = DoubleMatrix.zeros(negative + 1);
            labels.put(0, 1.0);
        }
        // Iterate through sentence words. For each word predict a word2 which is in the range of |window - reduced_window|
        for (int pos = 0; pos < sentence.size(); pos++) {
            Vocab word = sentence.get(pos);
            if (word == null) {
                continue; // OOV word in the input sentence => skip
            }
            // Generate a random window for each word
            int reduced_window = Random.nextInt(window); // `b` in the original word2vec code
            // now go over all words from the (reduced) window, predicting each one in turn
            int start = Math.max(0, pos - window + reduced_window);
            int end = pos + window + 1 - reduced_window;
            List<Vocab> sentence2 = sentence.subList(start, end > sentence.size() ? sentence.size() : end);
            // Fixed a context word, iterate through words (word2) which have it in their context
            for (int pos2 = 0; pos2 < sentence2.size(); pos2++) {
                Vocab word2 = sentence2.get(pos2);
                // don't train on OOV words and on the `word` itself
                if (word2 != null && pos != pos2 + start) {
                    train_pair(word, word2, alpha, labels);
                }
            }
        }
        int result = 0;
        for (Vocab word: sentence) {
            if (word != null) {
                result++;
            }
        }
        return result;
    }

    private void train_pair(Vocab word, Vocab word2, double alpha, DoubleMatrix labels) {
        //FIXME add other methods
        // Get the word vector from matrix
        DoubleMatrix l1 = syn0.getRow(word2.index);
        // Get the indices of the contexts, the first is the "true" one
        int[] C_indices = new int[negative+1];
        // use this word (label = 1) + `negative` other random words not from this sentence (label = 0)
        C_indices[0] = word.index;
        for (int i = 1; i < C_indices.length; i++) {
            int neg_i = table[Random.nextInt(table.length)];
            if (neg_i != word.index) {
                C_indices[i] = neg_i;
            }
        }
        // Matrix of vectors of contexts, the first is the "true" one
        DoubleMatrix l2b = syn1neg.getRows(C_indices); //2d matrix, k+1 x layer1_size
        // Compute the outputs of the model, for the true context and the other negatives one (propagate hidden -> output)
        DoubleMatrix fb = MatrixFunctions.expi(l2b.mmul(l1.transpose()).negi());
        for (int i = 0; i < fb.length; i++) {
            fb.put(i, 1.0 / (1.0 + fb.get(i)));
        }
        // Partial computation of the gradient (it misses the multiplication by the input vector)
        DoubleMatrix gb = (labels.sub(fb)).muli(alpha); // vector of error gradients multiplied by the learning rate
        // Now update matrices X and C
        // Learn C
        for (int i = 0; i < C_indices.length; i++) {
            for (int j = 0; j < syn1neg.columns; j++) {
                syn1neg.put(C_indices[i], j, syn1neg.get(C_indices[i], j) + gb.get(i) * l1.get(j));
            }
        }
        // Gradient error for learning W
        DoubleMatrix neu1e = gb.transpose().mmul(l2b);
        for (int i = 0; i < syn0.columns; i++) {
            syn0.put(word2.index, i, syn0.get(word2.index, i) + neu1e.get(i));
        }
    }

    private void build_vocab(Iterable<List<String>> sentences) {
        logger.info("collecting all words and their counts");
        HashMap<String, Vocab> temp_vocab = vocab_from(sentences);
        this.vocab = new HashMap<String, Vocab>();
        this.index2word = new ArrayList<String>();
        Iterator<Map.Entry<String, Vocab>> vocab_iter = temp_vocab.entrySet().iterator();
        while (vocab_iter.hasNext()) {
            Map.Entry<String, Vocab> vocab_word = vocab_iter.next();
            String word = vocab_word.getKey();
            Vocab v = vocab_word.getValue();
            if (v.count >= min_count) {
                v.index = index2word.size();
                index2word.add(word);
                vocab.put(word, v);
            }
        }
        logger.info("total {} word types after removing those with count<{}", vocab.size(), min_count);
//
//            if self.hs:
//            # add info about each word's Huffman encoding
//            self.create_binary_tree()
        if (negative > 0) {
            make_table();
        }
        precalc_sampling();
        reset_weights();
    }

    private static HashMap<String, Vocab> vocab_from(Iterable<List<String>> sentences) {
        int sentence_no = 0;
        HashMap<String, Vocab> vocab = new HashMap<String, Vocab>();
        long total_words = 0;
        for (List<String> sentence: sentences) {
            if (sentence_no % 10000 == 0) {
                logger.info("PROGRESS: at sentence #{}, processed {} words and {} word types",
                        sentence_no, total_words, vocab.size());
            }
            for (String word:sentence) {
                total_words += 1;
                if (vocab.containsKey(word)) {
                    vocab.get(word).count += 1;
                } else {
                    vocab.put(word, new Vocab(1));
                }
            }
            sentence_no++;
        }
        logger.info("collected {} word types from a corpus of {} words and {} sentences",
                vocab.size(), total_words, sentence_no + 1);
        return vocab;
    }

    private void make_table() {
        make_table(100000000, 0.75);
    }

    private void make_table(int table_size, double power) {
        logger.info("constructing a table with noise distribution from {} words", vocab.size());
        int vocab_size = index2word.size();
        table = new int[table_size]; //table (= list of words) of noise distribution for negative sampling
        if (vocab_size <= 0) {
            logger.warn("empty vocabulary in word2vec, is this intended?");
            return;
        }
        //compute sum of all power (Z in paper)
        double train_words_pow = 0;
        for (Vocab v: vocab.values()) {
            train_words_pow += Math.pow(v.count, power);
        }
        //go through the whole table and fill it up with the word indexes proportional to a word's count**power
        int widx = 0;
        // normalize count^0.75 by Z
        double d1 = Math.pow(vocab.get(index2word.get(widx)).count, power) / train_words_pow;
        for (int tidx = 0; tidx < table_size; tidx++) {
            table[tidx] = widx;
            if ((double)tidx / table_size > d1) {
                widx++;
                d1 += Math.pow(vocab.get(index2word.get(widx)).count, power) / train_words_pow;
            }
            if (widx >= vocab_size) {
                widx = vocab_size - 1;
            }
        }
    }

    /**
     * Reset all projection weights to an initial (untrained) state, but keep the existing vocabulary.
     */
    private void reset_weights() {
        logger.info("resetting layer weights");
        this.syn0 = new DoubleMatrix(vocab.size(), layer1_size);
        // Python version hack: randomize weights vector by vector, rather than materializing a huge random matrix in RAM at once
        for (int i = 0; i < vocab.size(); i++) {
            // construct deterministic seed from word AND seed argument
            //FIXME: hashCode function must be equal to the hash function in Python
            // Note: Python's built in hash function can vary across versions of Python
            org.jblas.util.Random.seed((long) (index2word.get(i) + Integer.toString(seed)).hashCode());
            DoubleMatrix random_row = DoubleMatrix.rand(layer1_size);
            syn0.putRow(i, random_row.subi(0.5).divi(layer1_size));
        }
//        if self.hs:
//        self.syn1 = zeros((len(self.vocab), self.layer1_size), dtype=REAL)
        if (negative > 0) {
            syn1neg = DoubleMatrix.zeros(vocab.size(), layer1_size);
        }
        syn0norm = null;
    }

    /**
     * Precalculate each vocabulary item's threshold for sampling
     */
    private void precalc_sampling() {
        double threshold_count = 0.0;
        if (sample > 0) {
            logger.info("frequent-word downsampling, threshold {}; progress tallies will be approximate", this.sample);
            long total_words = 0;
            for (Vocab v : vocab.values()) {
                total_words += v.count;
            }
            threshold_count = sample * total_words;
        }
        for (Vocab v: vocab.values()) {
            double prob = sample > 0 ? (Math.sqrt(v.count / threshold_count) + 1) * (threshold_count / v.count) : 1.0;
            v.sample_probability = Math.min(prob, 1.0);
        }
    }

    /**
     * Precompute L2-normalized vectors.
     * If `replace` is set, forget the original vectors and only keep the normalized
     * ones = saves lots of memory!
     * Note that you **cannot continue training** after doing a replace. The model becomes
     * effectively read-only = you can call `most_similar`, `similarity` etc., but not `train`.
     */
    public void init_sims(boolean replace) {
        if (replace) {
            syn0norm = syn0;
            syn1neg = null;
        } else {
            syn0norm = new DoubleMatrix(syn0.rows, syn0.columns);
        }
        for (int i = 0; i < syn0.rows; i++) {
            DoubleMatrix row = syn0.getRow(i);
            syn0norm.putRow(i, normalize(row));
        }
    }

    public void init_sims() {init_sims(false);}

    /**
     * Find the top-N most similar words. Positive words contribute positively towards the
     * similarity, negative words negatively.
     *
     * This method computes cosine similarity between a simple mean of the projection
     * weight vectors of the given words, and corresponds to the `word-analogy` and
     * `distance` scripts in the original word2vec implementation.
     */
    public ArrayList<ImmutablePair<String, Double>> most_similar(List<String> positives, List<String> negatives, int topn) {

        init_sims(false);
        DoubleMatrix mean_matrix = new DoubleMatrix(positives.size() + negatives.size(), syn0norm.columns);
        HashSet<Integer> input_words = new HashSet<>();
        int i = 0;
        for (String word: positives) {
            mean_matrix.putRow(i, syn0norm.getRow(vocab.get(word).index));
            input_words.add(vocab.get(word).index);
            i++;
        }
        for (String word: negatives) {
            mean_matrix.putRow(i, syn0norm.getRow(vocab.get(word).index).negi());
            input_words.add(vocab.get(word).index);
            i++;
        }
        DoubleMatrix mean = mean_matrix.columnMeans();
        if (mean.norm2() > 0) {
            mean = normalize(mean);
        }
        DoubleMatrix sims = syn0norm.mmul(mean.transpose());
        int[] indexes = sims.sortingPermutation();
        ArrayList<ImmutablePair<String, Double>> result = new ArrayList<ImmutablePair<String, Double>>(topn);
        for (int j = indexes.length-1; j >= indexes.length-topn; j--) {
            if (!input_words.contains(indexes[j])) {
                result.add(new ImmutablePair<String, Double>(index2word.get(indexes[j]), sims.get(indexes[j])));
            }
        }
        return result;
    }

    public void save(File path) throws IOException {
        if (!path.isFile()) {
            path.mkdirs();
        }
        syn0norm.save(path.getAbsolutePath() + File.separator + "syn0norm");
        syn0.save(path.getAbsolutePath() + File.separator + "syn0");
        syn1neg.save(path.getAbsolutePath() + File.separator + "syn1neg");
        FileOutputStream fos = new FileOutputStream(path.getAbsolutePath() + File.separator + "index2word");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(index2word);
        oos.close();
        fos.close();
        fos = new FileOutputStream(path.getAbsolutePath() + File.separator + "vocab");
        oos = new ObjectOutputStream(fos);
        oos.writeObject(vocab);
        oos.close();
        fos.close();
    }

}
