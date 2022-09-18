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

package com.geeknarrator;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DataStreamJob {

  public static void main(String[] args) throws Exception {
    // Sets up the execution environment, which is the main entry point
    // to building Flink applications.
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<Data> source1 = createSource1(env);
    source1.print();

//    DataStream<Data> filterStep = source1
//        .filter(event -> event.userId.equals("74"));
//    filterStep.print();

//    DataStream<Tuple3<String, Long, TimeWindow>> windowing = source1
//        .keyBy(data -> data.page)
//            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//        .apply(new WindowFunction<Data, Tuple3<String, Long, TimeWindow>, String, TimeWindow>() {
//          @Override
//          public void apply(String page, TimeWindow window, Iterable<Data> input, Collector<Tuple3<String, Long, TimeWindow>> out) throws Exception {
//            long numActivities = Iterables.size(input);
//            out.collect(Tuple3.of(page, numActivities, window));
//          }
//        });
//    windowing.print();


//    DataStream<Purchase> source2 = createSource2(env);
//    source1.join(source2).where(data -> data.userId).equalTo(purchase -> purchase.userId)
//        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//            .apply(new JoinFunction<Data, Purchase, Tuple2<String, Integer>>() {
//              @Override
//              public Tuple2<String, Integer> join(Data data, Purchase purchase) throws Exception {
//                return Tuple2.of(data.userId, 1);
//              }
//            })
//        .keyBy(tuple2 -> tuple2.f0)
//        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//        .sum(1).print();



    env.execute("Flink Java API Skeleton");
  }

  private static SingleOutputStreamOperator<Data> createSource1(StreamExecutionEnvironment env) {
    return env.readTextFile("webserver_log.txt")
        .map(str -> {
          String[] line = str.split(", ");
          return new Data(line[0], line[1], line[2], line[3]);
        })
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<Data>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.timestamp));
  }

  private static DataStream<Purchase> createSource2(StreamExecutionEnvironment env) {
    return env.readTextFile("purchases.txt")
        .map(str -> {
          String[] line = str.split(", ");
          return new Purchase(line[0], line[1], line[2]);
        })
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<Purchase>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.timestamp));
  }
}
