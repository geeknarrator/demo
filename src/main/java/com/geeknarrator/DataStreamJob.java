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

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.Iterator;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction.Context;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import nl.basjes.parse.core.Parser;
import nl.basjes.parse.httpdlog.HttpdLoglineParser;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {
	static Pattern logPattern = Pattern.compile("(.*) - - \\[(.*)\\] \"(.*)\" (\\d+) (\\d+) \"(.*)\" \"(.*)\"");

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.fromSequence(1, 10);
		 *
		 * then, transform the resulting DataStream<Long> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.window()
		 * 	.process()
		 *
		 * and many more.
		 * Have a look at the programming guide:
		 *
		 * https://nightlies.apache.org/flink/flink-docs-stable/
		 *
		 */
		DataStream<String> sourceDataStream = env.readTextFile("apache_logs.txt");
    env.setParallelism(1);
		DataStream<AccessLog> parsedStream = sourceDataStream.flatMap(new FlatMapFunction<String, AccessLog>() {
			@Override
			public void flatMap(String input, Collector<AccessLog> collector) throws Exception {
				parse(input).ifPresent(collector::collect);
			}
		});
		DataStream<AccessLog> assignedTimestampsAndWatermarks = parsedStream.assignTimestampsAndWatermarks(WatermarkStrategy.<AccessLog>forBoundedOutOfOrderness(Duration.ofMinutes(1))
				.withTimestampAssigner((accessLog, timestamp) -> accessLog.getDateTime().getTime()));

		assignedTimestampsAndWatermarks.keyBy(AccessLog::getIp).window(TumblingEventTimeWindows.of(Time.minutes(60)))
						.process(new ProcessWindowFunction<AccessLog, Long, String, TimeWindow>() {

							@Override
							public void process(String s, ProcessWindowFunction<AccessLog, Long, String, TimeWindow>.Context context, Iterable<AccessLog> elements, Collector<Long> collector) throws Exception {
								long count = 0;
								for (AccessLog element : elements) {
									count++;
								}
								collector.collect(count);
							}
						}).print();

		env.execute("Flink Java API Skeleton");
	}

	/**
	 * 1) Print all events
	 * 2) Count them based on a tumbling window - We can talk a bit about how this works.
	 * 3) Join this stream with another.
	 */

	private static Optional<AccessLog> parse(String input) {
			Matcher matcher = logPattern.matcher(input);
			if(!matcher.matches()) {
				return Optional.empty();
			}
			String ip = matcher.group(1);
			String dateTime = matcher.group(2);
			// 17/May/2015:10:05:03 +0000
			Date parsedDate;
			try {
				parsedDate = DateUtils.parseDate(dateTime, "dd/MMM/yyyy:HH:mm:ss Z");
			} catch (ParseException e) {
				//System.out.println("Parsing failed");
				return Optional.empty();
			}
			//System.out.println(parsedDate);
			String api = matcher.group(3);
			String httpCode = matcher.group(4);
			String responseSize = matcher.group(5);
			String httpUrl = matcher.group(6);
			String userAgent = matcher.group(7);
			return Optional.of(new AccessLog(ip, parsedDate, api, httpCode, responseSize, httpUrl, userAgent));
	}
}
