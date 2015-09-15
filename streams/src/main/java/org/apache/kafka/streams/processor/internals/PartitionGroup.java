/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * A PartitionGroup is composed from a set of partitions. It also maintains the timestamp of this
 * group, hence the associated task as the min timestamp across all partitions in the group.
 */
public class PartitionGroup {

    private final Map<TopicPartition, RecordQueue> partitionQueues;

    private final PriorityQueue<RecordQueue> queuesByTime;

    private final TimestampExtractor timestampExtractor;

    public static class RecordInfo {
        public RecordQueue queue;

        public ProcessorNode node() {
            return queue.source();
        }

        public TopicPartition partition() {
            return queue.partition();
        }
    }

    // since task is thread-safe, we do not need to synchronize on local variables
    private int totalBuffered;

    public PartitionGroup(Map<TopicPartition, RecordQueue> partitionQueues, TimestampExtractor timestampExtractor) {
        this.queuesByTime = new PriorityQueue<>(partitionQueues.size(), new Comparator<RecordQueue>() {

            @Override
            public int compare(RecordQueue queue1, RecordQueue queue2) {
                long time1 = queue1.timestamp();
                long time2 = queue2.timestamp();

                if (time1 < time2) return -1;
                if (time1 > time2) return 1;
                return 0;
            }
        });

        this.partitionQueues = partitionQueues;

        this.timestampExtractor = timestampExtractor;

        this.totalBuffered = 0;
    }

    /**
     * Get the next record and queue
     *
     * @return StampedRecord
     */
    public StampedRecord nextRecord(RecordInfo info) {
        StampedRecord record = null;

        RecordQueue queue = queuesByTime.poll();
        if (queue != null) {
            // get the first record from this queue.
            record = queue.poll();

            if (queue.size() > 0) {
                queuesByTime.offer(queue);
            }
        }
        info.queue = queue;

        if (record != null) totalBuffered--;

        return record;
    }

    /**
     * Adds raw records to this partition group
     *
     * @param partition the partition
     * @param rawRecords  the raw records
     * @return the queue size for the partition
     */
    public int addRawRecords(TopicPartition partition, Iterable<ConsumerRecord<byte[], byte[]>> rawRecords) {
        RecordQueue recordQueue = partitionQueues.get(partition);

        int oldSize = recordQueue.size();
        int newSize = recordQueue.addRawRecords(rawRecords, timestampExtractor);

        // add this record queue to be considered for processing in the future if it was empty before
        if (oldSize == 0 && newSize > 0) {
            queuesByTime.offer(recordQueue);
        }

        totalBuffered += newSize - oldSize;

        return newSize;
    }

    public Set<TopicPartition> partitions() {
        return partitionQueues.keySet();
    }

    /**
     * Return the timestamp of this partition group as the smallest
     * partition timestamp among all its partitions
     */
    public long timestamp() {
        if (queuesByTime.isEmpty()) {
            // if there is no data in all partitions, return the smallest of their last known times
            long timestamp = Long.MAX_VALUE;
            for (RecordQueue queue : partitionQueues.values()) {
                if (timestamp > queue.timestamp())
                    timestamp = queue.timestamp();
            }
            return timestamp;
        } else {
            return queuesByTime.peek().timestamp();
        }
    }

    public int numBuffered(TopicPartition partition) {
        RecordQueue recordQueue = partitionQueues.get(partition);

        if (recordQueue == null)
            throw new KafkaException("Record's partition does not belong to this partition-group.");

        return recordQueue.size();
    }

    public int numBuffered() {
        return totalBuffered;
    }

    public void close() {
        queuesByTime.clear();
        partitionQueues.clear();
    }
}
