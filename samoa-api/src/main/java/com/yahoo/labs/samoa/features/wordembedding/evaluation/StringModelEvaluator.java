package com.yahoo.labs.samoa.features.wordembedding.evaluation;

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


import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.jblas.DoubleMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

import static org.jblas.Geometry.normalize;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class StringModelEvaluator {

    private static final Logger logger = LoggerFactory.getLogger(StringModelEvaluator.class);
    private HashMap<String, MutablePair<DoubleMatrix, Long>> syn0norm;
    private int columns;

    public void load(File path) throws IOException, ClassNotFoundException {
        if ((new File(path.getAbsolutePath() + File.separator + "index2word")).exists()) {
            loadBatch(path);
        } else {
            FileInputStream fis = new FileInputStream(path.getAbsolutePath() + File.separator + "syn0norm");
            ObjectInputStream ois = new ObjectInputStream(fis);
            syn0norm = (HashMap<String, MutablePair<DoubleMatrix, Long>>) ois.readObject();
            ois.close();
            fis.close();
        }
        this.columns = syn0norm.values().iterator().next().getLeft().length;
    }

    /**
     * Load a model saved in the format of http://github...
     * @param path
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public void loadBatch(File path) throws IOException, ClassNotFoundException {
        //FIXME
        throw new UnsupportedOperationException("This feature is temporarily disabled.");
//        FileInputStream fis = new FileInputStream(path.getAbsolutePath() + File.separator + "index2word");
//        ObjectInputStream ois = new ObjectInputStream(fis);
//        ArrayList<String> index2word = (ArrayList<String>) ois.readObject();
//        ois.close();
//        fis.close();
//        fis = new FileInputStream(path.getAbsolutePath() + File.separator + "vocab");
//        ois = new ObjectInputStream(fis);
//        HashMap<String, Vocab> vocab = (HashMap<String, Vocab>) ois.readObject();
//        ois.close();
//        fis.close();
//        DoubleMatrix syn0normM = new DoubleMatrix();
//        syn0normM.load(path.getAbsolutePath() + File.separator + "syn0norm");
//        syn0norm = new HashMap<>(index2word.size());
//        for (int i = 0; i < index2word.size(); i++) {
//            String word = index2word.get(i);
//            syn0norm.put(word, new MutablePair<>(syn0normM.getRow(i).transpose(), (long) vocab.get(word).count));
//        }

    }

    /**
     * Find the top-N most similar words. Positive words contribute positively towards the
     * similarity, negative words negatively.
     *
     * This method computes cosine similarity between a simple mean of the projection
     * weight vectors of the given words, and corresponds to the `word-analogy` and
     * `distance` scripts in the original word2vec implementation.
     */
    public ArrayList<ImmutablePair<String, Double>> most_similar(List<String> positives, List<String> negatives, int topn) {

        HashSet<String> inputWords = new HashSet<>();
        for (String word: positives) {
            inputWords.add(word);
        }
        for (String word: negatives) {
            inputWords.add(word);
        }
        HashMap<String, Double> sims = similarity_vectors(positives, negatives);
        Ordering<Map.Entry<String, Double>> byValues = new Ordering<Map.Entry<String, Double>>() {
            @Override
            public int compare(Map.Entry<String, Double> left, Map.Entry<String, Double> right) {
                return Double.compare(right.getValue(), left.getValue());
            }
        };

        List<Map.Entry<String, Double>> simsOrdered = Lists.newArrayList(sims.entrySet());
        Collections.sort(simsOrdered, byValues);
        ArrayList<ImmutablePair<String, Double>> result = new ArrayList<ImmutablePair<String, Double>>(topn);
        int i = 0;
        int count = 0;
        while ((count < topn) && (i < simsOrdered.size())) {
            String word = simsOrdered.get(i).getKey();
            if (!inputWords.contains(word)) {
                result.add(new ImmutablePair<String, Double>(word, simsOrdered.get(i).getValue()));
                count++;
            }
            i++;
        }
        return result;
    }

    /**
     * Returns a vector of similarities between inputs and each word vector in the vocabulary
     * @param positives
     * @param negatives
     * @return
     */
    public HashMap<String, Double> similarity_vectors(List<String> positives, List<String> negatives) {

        DoubleMatrix meanMatrix = new DoubleMatrix(positives.size() + negatives.size(), this.columns);
        int i = 0;
        for (String word: positives) {
            if (syn0norm.containsKey(word)) {
                meanMatrix.putRow(i, syn0norm.get(word).getLeft());
                i++;
            } else {
                logger.debug("The model does not contain the positive word: " + word);
            }
        }
        for (String word: negatives) {
            if (syn0norm.containsKey(word)) {
                meanMatrix.putRow(i, syn0norm.get(word).getLeft().neg());
                i++;
            } else {
                logger.debug("The model does not contain the negative word: " + word);
            }
        }
        DoubleMatrix mean = meanMatrix.columnMeans();
        if (mean.norm2() > 0) {
            mean = normalize(mean);
        }
        HashMap<String, Double> result = new HashMap<String, Double>(syn0norm.size());
        for (String word: syn0norm.keySet()) {
            result.put(word, syn0norm.get(word).getLeft().dot(mean));
        }
        return result;
    }

    /**
     * Compute accuracy of the model. `questions` is a filename where lines are
     * 4-tuples of words, split into sections by ": SECTION NAME" lines.
     * See https://code.google.com/p/word2vec/source/browse/trunk/questions-words.txt for an example.
     * The accuracy is reported (=printed to log and returned as a list) for each
     * section separately, plus there's one aggregate summary at the end.
     * Use `restrict_vocab` to ignore all questions containing a word whose frequency
     * is not in the top-N most frequent words (default top 30,000).
     * This method corresponds to the `compute-accuracy` script of the original C word2vec.
     */
    public void accuracy(File questions, int restrict_vocab) throws IOException {

        class Section {
            String section;
            int correct = 0;
            int incorrect = 0;
            void log_accuracy() {
                if (correct + incorrect > 0) {
                    logger.info(String.format("%s: %.1f%% (%d/%d)", section, 100.0 * correct / (correct + incorrect),
                            correct, correct + incorrect));
                }
            }
        }
        Ordering<Map.Entry<String, MutablePair<DoubleMatrix, Long>>> by_values =
                new Ordering<Map.Entry<String, MutablePair<DoubleMatrix, Long>>>() {
            @Override
            public int compare(Map.Entry<String, MutablePair<DoubleMatrix, Long>> left, Map.Entry<String, MutablePair<DoubleMatrix, Long>> right) {
                return Long.compare(right.getValue().getRight(), left.getValue().getRight());
            }
        };

        List<Map.Entry<String, MutablePair<DoubleMatrix, Long>>> vocab_ordered = Lists.newArrayList(syn0norm.entrySet());
        Collections.sort(vocab_ordered, by_values);
        if (restrict_vocab < syn0norm.size()) {
            vocab_ordered = vocab_ordered.subList(0, restrict_vocab);
        } else {
            restrict_vocab = syn0norm.size();
        }
        //TODO optimize this creation
        HashSet<String> ok_vocab = new HashSet<String>(vocab_ordered.size());
        for (Map.Entry<String, MutablePair<DoubleMatrix, Long>> v: vocab_ordered) {
            ok_vocab.add(v.getKey());
        }
        Section section = null;
        ArrayList<Section> sections = new ArrayList<Section>();
        LineIterator questions_it = FileUtils.lineIterator(questions, "UTF-8");
        int line_no = 0;
        while (questions_it.hasNext()) {
            String line = questions_it.next().trim();
            String a, b, c, expected;
            if (line.startsWith(": ")) {
                // a new section starts => store the old section
                if (section != null) {
                    sections.add(section);
                    section.log_accuracy();
                }
                section = new Section();
                section.section = line.split(": ")[1].trim();
            } else {
                if (section.section.isEmpty() || section == null) {
                    throw new IOException(String.format("missing section header before line #%d in %s", line_no, questions));
                }
                try {
                    String[] words = line.split(" ");
                    // FIXME assumes training set is in lower case
                    a = words[0].toLowerCase();
                    b = words[1].toLowerCase();
                    c = words[2].toLowerCase();
                    expected = words[3].toLowerCase();
                } catch (Exception e) {
                    logger.info(String.format("skipping invalid line #%d in %s",line_no, questions));
                    continue;
                }
                if (!ok_vocab.contains(a) || !ok_vocab.contains(b) || !ok_vocab.contains(c) || !ok_vocab.contains(expected)) {
                    logger.debug(String.format("skipping line #%d with OOV words: %s", line_no, line));
                    continue;
                }
                HashSet<String> ignore = new HashSet<String>(Arrays.asList(a, b, c)); // indexes of words to ignore
                String predicted = "";
                // find the most likely prediction, ignoring OOV words and input words
                //FIXME set topn not arbitrarily
                ArrayList<ImmutablePair<String, Double>> similars = most_similar(Arrays.asList(b, c), Arrays.asList(a), 1000);
                for (ImmutablePair<String, Double> v: similars) {
                    predicted = v.getLeft();
                    if (ok_vocab.contains(predicted) && !ignore.contains(predicted)) {
                        if (!predicted.equals(expected)) {
                            logger.debug("{}: expected {}, predicted {}", line.trim(), expected, predicted);
                        }
                        break;
                    }
                }
                if (predicted.equals(expected)) {
                    section.correct++;
                } else {
                    section.incorrect++;
                }
            }
            line_no++;
        }
        if (section != null) {
            // store the last section, too
            sections.add(section);
            section.log_accuracy();
        }
        Section total = new Section();
        total.section = "TOTAL";
        for (Section s: sections) {
            total.correct += s.correct;
            total.incorrect += s.incorrect;
        }
        total.log_accuracy();
        sections.add(total);
        // FIXME return sections?
    }

    public static void main(String[] args) {
        StringModelEvaluator e = new StringModelEvaluator();
        try {
            e.load(new File(args[0]));
        } catch (IOException e1) {
            e1.printStackTrace();
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
        }

        logger.info("Number of word types: "+e.syn0norm.size());
        long wordOccurrences = 0;
        for (String word: e.syn0norm.keySet()) {
            wordOccurrences+=e.syn0norm.get(word).getRight();
        }
        logger.info("Sum of word counts: "+wordOccurrences);
//        for (String key: e.syn0norm.keySet()) {
//            if (e.syn0norm.get(key).getLeft() == null) {
//                logger.info(key);
//            }
//        }
        logger.info(e.most_similar(new ArrayList<String>(Arrays.asList(new String[]{"king"})), new ArrayList<String>(), 10).toString());
        logger.info(e.most_similar(new ArrayList<String>(Arrays.asList(new String[]{"anarchism"})), new ArrayList<String>(), 10).toString());
        //logger.info(e.similarity_vectors(new ArrayList<String>(Arrays.asList(new String[]{"king"})), new ArrayList<String>()).get("devote").toString());

        try {
            e.accuracy(new File(args[1]), 30000);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }
}
