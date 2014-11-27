package com.yahoo.labs.samoa.features.counters;
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

import java.util.Iterator;
import java.util.Map;

/**
 * Approximate element counts
 *
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public interface Counter<T> {

    int size();
    /**
     * The value is changed only if it is grater then the current count
     * @param key
     * @param value
     * @return
     */
    long put(T key, Long value);

    long remove(T word);

    long get(T word);

    boolean containsKey(T word);

    Iterator<Map.Entry<T,Long>> iterator();
}
//public interface Counter<T> extends Map<T, Long> {
//}
