package com.yahoo.labs.samoa.features.counter;

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

import org.apache.commons.lang3.tuple.ImmutablePair;
import java.util.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Minimal implementation of the Space Saving algorithm, with item removal.
 * Code from https://github.com/addthis/stream-lib
 *
 * @author Giacomo Berardi <barnets@gmail.com>.
 */

public class StreamSummary<T> implements Counter<T> {

    protected class DoublyLinkedList<T> implements Iterable<T> {

        protected int size;
        protected ListNode2<T> tail;
        protected ListNode2<T> head;

        //FIXME use some external library structures for these inner classes
        /**
         * Append to head of list
         */
        public ListNode2<T> add(T value) {
            ListNode2<T> node = new ListNode2<T>(value);
            if (size++ == 0) {
                tail = node;
            } else {
                node.prev = head;
                head.next = node;
            }

            head = node;

            return node;
        }

        /**
         * Prepend to tail of list
         */
        public ListNode2<T> enqueue(T value) {
            ListNode2<T> node = new ListNode2<T>(value);
            if (size++ == 0) {
                head = node;
            } else {
                node.next = tail;
                tail.prev = node;
            }

            tail = node;

            return node;
        }

        public void add(ListNode2<T> node) {
            node.prev = head;
            node.next = null;

            if (size++ == 0) {
                tail = node;
            } else {
                head.next = node;
            }

            head = node;
        }

        public ListNode2<T> addAfter(ListNode2<T> node, T value) {
            ListNode2<T> newNode = new ListNode2<T>(value);
            addAfter(node, newNode);
            return newNode;
        }

        public void addAfter(ListNode2<T> node, ListNode2<T> newNode) {
            newNode.next = node.next;
            newNode.prev = node;
            node.next = newNode;
            if (newNode.next == null) {
                head = newNode;
            } else {
                newNode.next.prev = newNode;
            }
            size++;
        }

        public void remove(ListNode2<T> node) {
            if (node == tail) {
                tail = node.next;
            } else {
                node.prev.next = node.next;
            }

            if (node == head) {
                head = node.prev;
            } else {
                node.next.prev = node.prev;
            }
            size--;
        }

        public int size() {
            return size;
        }


        @Override
        public Iterator<T> iterator() {
            return new DoublyLinkedListIterator(this);
        }

        protected class DoublyLinkedListIterator implements Iterator<T> {

            protected DoublyLinkedList<T> list;
            protected ListNode2<T> itr;
            protected int length;

            public DoublyLinkedListIterator(DoublyLinkedList<T> list) {
                this.length = list.size;
                this.list = list;
                this.itr = list.tail;
            }

            @Override
            public boolean hasNext() {
                return itr != null;
            }

            @Override
            public T next() {
                if (length != list.size) {
                    throw new ConcurrentModificationException();
                }
                T next = itr.value;
                itr = itr.next;
                return next;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        }

        public T first() {
            return tail == null ? null : tail.getValue();
        }

        public T last() {
            return head == null ? null : head.getValue();
        }

        public ListNode2<T> head() {
            return head;
        }

        public ListNode2<T> tail() {
            return tail;
        }

        public boolean isEmpty() {
            return size == 0;
        }

        @SuppressWarnings("unchecked")
        public T[] toArray() {
            T[] a = (T[]) new Object[size];
            int i = 0;
            for (T v : this) {
                a[i++] = v;
            }
            return a;
        }
    }

    protected class ListNode2<T> {
        protected T value;
        protected ListNode2<T> prev;
        protected ListNode2<T> next;
        public ListNode2(T value) {
            this.value = value;
        }
        public ListNode2<T> getPrev() {
            return prev;
        }
        public ListNode2<T> getNext() {
            return next;
        }
        public T getValue() {
            return value;
        }
        public void setValue(T value) {
            this.value = value;
        }
    }

    protected class ItemCount<T>  {
        protected ListNode2<StreamSummary<T>.Bucket> bucketNode;
        protected T item;
        protected long count;
        protected long error;
        public ItemCount(ListNode2<StreamSummary<T>.Bucket> bucket, T item) {
            this.bucketNode = bucket;
            this.count = 0;
            this.error = 0;
            this.item = item;
        }
        public T getItem() {
            return item;
        }
        public long getCount() {
            return count;
        }
        public long getError() {
            return error;
        }
    }

    protected class Bucket {
        protected DoublyLinkedList<ItemCount<T>> counterList;
        private long count;
        public Bucket(long count) {
            this.count = count;
            this.counterList = new DoublyLinkedList<ItemCount<T>>();
        }
    }

    protected int capacity;
    private HashMap<T, ListNode2<ItemCount<T>>> counterMap;
    protected DoublyLinkedList<Bucket> bucketList;

    /**
     * @param capacity maximum size (larger capacities improve accuracy)
     */
    public StreamSummary(int capacity) {
        this.capacity = capacity;
        counterMap = new HashMap<T, ListNode2<ItemCount<T>>>();
        bucketList = new DoublyLinkedList<Bucket>();
    }

    public int getCapacity() {
        return capacity;
    }

    /**
     * Algorithm: <i>Space-Saving</i>
     *
     * @param item stream element (<i>e</i>)
     * @return false if item was already in the stream summary, true otherwise
     */
    public boolean offer(T item) {
        return offer(item, 1);
    }

    /**
     * Algorithm: <i>Space-Saving</i>
     *
     * @param item stream element (<i>e</i>)
     * @return false if item was already in the stream summary, true otherwise
     */
    public boolean offer(T item, long incrementCount) {
        return offerReturnAll(item, incrementCount).left;
    }

    /**
     * @param item stream element (<i>e</i>)
     * @return item dropped from summary if an item was dropped, null otherwise
     */
    public T offerReturnDropped(T item, long incrementCount) {
        return offerReturnAll(item, incrementCount).right;
    }

    /**
     * @param item stream element (<i>e</i>)
     * @return Pair<isNewItem, itemDropped> where isNewItem is the return value of offer() and itemDropped is null if no item was dropped
     */
    public ImmutablePair<Boolean, T> offerReturnAll(T item, long incrementCount) {
        ListNode2<ItemCount<T>> counterNode = counterMap.get(item);
        boolean isNewItem = (counterNode == null);
        T droppedItem = null;
        if (isNewItem) {

            if (size() < capacity) {
                counterNode = bucketList.enqueue(new Bucket(0)).getValue().counterList.add(new ItemCount<T>(bucketList.tail(), item));
            } else {
                Bucket min = bucketList.first();
                counterNode = min.counterList.tail();
                ItemCount<T> itemCount = counterNode.getValue();
                droppedItem = itemCount.item;
                counterMap.remove(droppedItem);
                itemCount.item = item;
                itemCount.error = min.count;
            }
            counterMap.put(item, counterNode);
        }

        incrementCounter(counterNode, incrementCount);

        return new ImmutablePair<Boolean, T>(isNewItem, droppedItem);
    }

    protected void incrementCounter(ListNode2<ItemCount<T>> counterNode, long incrementCount) {
        ItemCount<T> itemCount = counterNode.getValue();       // count_i
        ListNode2<Bucket> oldNode = itemCount.bucketNode;
        Bucket bucket = oldNode.getValue();         // Let Bucket_i be the bucket of count_i
        bucket.counterList.remove(counterNode);            // Detach count_i from Bucket_i's child-list
        itemCount.count = itemCount.count + incrementCount;

        // Finding the right bucket for count_i
        // Because we allow a single call to increment count more than once, this may not be the adjacent bucket.
        ListNode2<Bucket> bucketNodePrev = oldNode;
        ListNode2<Bucket> bucketNodeNext = bucketNodePrev.getNext();
        while (bucketNodeNext != null) {
            Bucket bucketNext = bucketNodeNext.getValue(); // Let Bucket_i^+ be Bucket_i's neighbor of larger value
            if (itemCount.count == bucketNext.count) {
                bucketNext.counterList.add(counterNode);    // Attach count_i to Bucket_i^+'s child-list
                break;
            } else if (itemCount.count > bucketNext.count) {
                bucketNodePrev = bucketNodeNext;
                bucketNodeNext = bucketNodePrev.getNext();  // Continue hunting for an appropriate bucket
            } else {
                // A new bucket has to be created
                bucketNodeNext = null;
            }
        }

        if (bucketNodeNext == null) {
            Bucket bucketNext = new Bucket(itemCount.count);
            bucketNext.counterList.add(counterNode);
            bucketNodeNext = bucketList.addAfter(bucketNodePrev, bucketNext);
        }
        itemCount.bucketNode = bucketNodeNext;

        //Cleaning up
        if (bucket.counterList.isEmpty())           // If Bucket_i's child-list is empty
        {
            bucketList.remove(oldNode);         // Detach Bucket_i from the Stream-Summary
        }
    }

    /**
     * Remove an element
     * @param item
     * @return The estimated count of the element, 0 if it is not present
     */
    public long removeItem(T item) {
        long count = count(item);
        if (count == 0) {
            return 0;
        }
        ListNode2<ItemCount<T>> counterNode = counterMap.get(item);
        ListNode2<Bucket> bucketNode = counterNode.getValue().bucketNode;
        Bucket bucket = bucketNode.getValue();
        bucket.counterList.remove(counterNode);
        counterMap.remove(item);
        if (bucket.counterList.isEmpty()) {
            bucketList.remove(bucketNode);
        }
        return count;
    }

    /**
     * Get the estimated count of an element
     * @param item
     * @return The estimated count of the element, 0 if it is not present
     */
    public long count(T item) {
        ListNode2<ItemCount<T>> c = counterMap.get(item);
        return (c == null) ? 0 : c.getValue().getCount();
    }

    public List<T> peek(int k) {
        List<T> topK = new ArrayList<T>(k);

        for (ListNode2<Bucket> bNode = bucketList.head(); bNode != null; bNode = bNode.getPrev()) {
            Bucket b = bNode.getValue();
            for (ItemCount<T> c : b.counterList) {
                if (topK.size() == k) {
                    return topK;
                }
                topK.add(c.item);
            }
        }

        return topK;
    }

    public List<ItemCount<T>> topK(int k) {
        List<ItemCount<T>> topK = new ArrayList<ItemCount<T>>(k);

        for (ListNode2<Bucket> bNode = bucketList.head(); bNode != null; bNode = bNode.getPrev()) {
            Bucket b = bNode.getValue();
            for (ItemCount<T> c : b.counterList) {
                if (topK.size() == k) {
                    return topK;
                }
                topK.add(c);
            }
        }

        return topK;
    }

    /**
     * @return number of items stored
     */
    public long size() {
        return counterMap.size();
    }

    public boolean isEmpty() {
        return counterMap.isEmpty();
    }

    public boolean containsKey(Object key) {
        return counterMap.containsKey(key);
    }

    public long get(Object key) {
        return count((T) key);
    }

    /**
     * The value is changed only if it is grater then the current count
     * @param key
     * @param value
     * @return
     */
    public long put(T key, Long value) {
        T item = (T) key;
        long increment = value - count(item);
        if (increment > 0) {
            offer(item, increment);
            return increment;
        }
        return (long) 0;
    }

    @Override
    public Iterator<Map.Entry<T, Long>> iterator() {
        return  new Iterator<Map.Entry<T, Long>>() {
            private final Iterator<Map.Entry<T, ListNode2<ItemCount<T>>>> iterator = counterMap.entrySet().iterator();
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }
            @Override
            public ImmutablePair<T, Long> next() {
                Map.Entry<T, ListNode2<ItemCount<T>>> itemNode = iterator.next();
                return new ImmutablePair<T, Long>(itemNode.getKey(), itemNode.getValue().getValue().getCount());
            }
            @Override
            public void remove() {
                throw new UnsupportedOperationException("Cannot remove from the iterator.");
            }
        };
    }

    public long remove(Object key) {
        return removeItem((T) key);
    }

    public Set<T> keySet() {
        return counterMap.keySet();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (ListNode2<Bucket> bNode = bucketList.head(); bNode != null; bNode = bNode.getPrev()) {
            Bucket b = bNode.getValue();
            sb.append('{');
            sb.append(b.count);
            sb.append(":[");
            for (ItemCount<T> c : b.counterList) {
                sb.append('{');
                sb.append(c.item);
                sb.append(':');
                sb.append(c.error);
                sb.append("},");
            }
            if (b.counterList.size() > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }
            sb.append("]},");
        }
        if (bucketList.size() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(']');
        return sb.toString();
    }
}
