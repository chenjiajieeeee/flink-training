/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.examples.ridecount;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.GeoUtils;

/**
 * Example that counts the rides for each driver.
 *
 * <p>Note that this is implicitly keeping state for each driver. This sort of simple, non-windowed
 * aggregation on an unbounded set of keys will use an unbounded amount of state. When this is an
 * issue, look at the SQL/Table API, or ProcessFunction, or state TTL, all of which provide
 * mechanisms for expiring state for stale keys.
 */
public class RideCountExample {

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        // set up streaming execution environment
        //解释一下下面这行代码，这行代码是获取一个流处理的执行环境，这个执行环境是Flink的一个核心概念，它是Flink程序的主要入口点。
        // 通过这个执行环境，我们可以设置执行环境的各种参数，比如并行度，checkpoint的配置等等。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        //这行代码解释一下
        //这行代码是获取一个数据流，这个数据流是一个TaxiRide的数据流，这个数据流是通过TaxiRideGenerator这个类来生成的。
        DataStream<TaxiRide> rides = env.addSource(new TaxiRideGenerator());

        DataStream<TaxiFare> fares = env.addSource(new TaxiFareGenerator());

        // map each ride to a tuple of (driverId, 1)
        //这行代码解释一下
        //这行代码是将每一次的乘车记录转换成一个Tuple2<Long, Long>的数据流，这个数据流的第一个元素是driverId，第二个元素是1。
        DataStream<Tuple2<Long, Long>> tuples = rides.map(new MapFunction<TaxiRide, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(TaxiRide ride) throws Exception {
                if(GeoUtils.isInNYC(ride.startLon, ride.startLat) && GeoUtils.isInNYC(ride.endLon, ride.endLat)){
                    return new Tuple2<>(ride.driverId, 1L);
                } else {
                    return new Tuple2<>(ride.driverId, 0L);
                }
            }
        });
        //解释一下这行代码
        //这行代码是将上面的数据流按照driverId进行分区，然后对每一个driverId进行求和。
        // partition the stream by the driverId
        KeyedStream<Tuple2<Long, Long>, Long> keyedByDriverId = tuples.keyBy(t -> t.f0);

        //解释一下这行代码
        //这行代码是对每一个driverId的数据流进行求和。
        // count the rides for each driver
        DataStream<Tuple2<Long, Long>> rideCounts = keyedByDriverId.sum(1);

        //解释一下这行代码
        //这行代码是将每一个driverId的数据流打印出来。
        //我想设定打印的频率，怎么设置？
        env.setBufferTimeout(500);
        rideCounts.print();

        // run the cleansing pipeline
        env.execute("Ride Count");
    }
}
