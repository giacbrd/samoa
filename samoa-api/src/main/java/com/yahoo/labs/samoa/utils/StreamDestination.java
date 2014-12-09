package com.yahoo.labs.samoa.utils;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2013 Yahoo! Inc.
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

import com.yahoo.labs.samoa.topology.IProcessingItem;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Represents one destination for streams. It has the info of:
 * the ProcessingItem, parallelismHint, and partitioning scheme.
 * Usage:
 * - When ProcessingItem connects to a stream, it will pass 
 * a StreamDestination to the stream.
 * - Stream manages a set of StreamDestination.
 * - Used in single-threaded and multi-threaded local mode.
 * @author Anh Thu Vu
 *
 */
public class StreamDestination {

	private IProcessingItem pi;
	private int parallelism;
	private PartitioningScheme type;
	
	/*
	 * Constructor
	 */
	public StreamDestination(IProcessingItem pi, int parallelismHint, PartitioningScheme type) {
		this.pi = pi;
		this.parallelism = parallelismHint;
		this.type = type;
	}
	
	/*
	 * Getters
	 */
	public IProcessingItem getProcessingItem() {
		return this.pi;
	}
	
	public int getParallelism() {
		return this.parallelism;
	}
	
	public PartitioningScheme getPartitioningScheme() {
		return this.type;
	}

    /**
     * Helper function for key-based distribution of streams.
     * @param key
     * @param parallelism
     * @return
     */
    public static int getPIIndexForKey(String key, int parallelism) {
        // If key is null, return a default index: 0
        if (key == null) return 0;

        // HashCodeBuilder object does not have reset() method
        // So all objects that get appended will be included in the
        // computation of the hashcode.
        // To avoid initialize a HashCodeBuilder for each event,
        // here I use the static method with reflection on the event's key
        int index = HashCodeBuilder.reflectionHashCode(key, true) % parallelism;
        if (index < 0) {
            index += parallelism;
        }
        return index;
    }

}
