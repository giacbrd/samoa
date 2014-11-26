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
import com.yahoo.labs.samoa.features.wordembedding.samplers.SGNSItemEvent;

/**
 * @author Giacomo Berardi <barnets@gmail.com>.
 */
public class TestProcessor implements Processor {
    @Override
    public boolean process(ContentEvent event) {
        SGNSItemEvent e = (SGNSItemEvent) event;
        //System.out.println(e.getItem() + " " + e.getContextItem());
        return true;
    }

    @Override
    public void onCreate(int id) {

    }

    @Override
    public Processor newProcessor(Processor processor) {
        return new TestProcessor();
    }
}
