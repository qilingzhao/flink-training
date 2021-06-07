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

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.util.Collector;


/**
 * The "Hourly Tips" exercise of the Flink training in the docs.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 */
public class HourlyTipsExercise extends ExerciseBase {

	/**
	 * Main method.
	 *
	 * @throws Exception which occurs during job execution.
	 */
	public static void main(String[] args) throws Exception {

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareGenerator()));

//		throw new MissingSolutionException();
		// my solution
//		DataStream<Tuple3<Long, Long, Float>> hourlyMax = fares
//				.keyBy(new KeySelector<TaxiFare, Long>() {
//					@Override
//					public Long getKey(TaxiFare value) throws Exception {
//						return value.driverId;
//					}
//				}).window(TumblingEventTimeWindows.of(Time.hours(1)))
//				.aggregate(new DriverTotalTip(), new ProcessWin1())
//					.keyBy(new KeySelector<Tuple3<Long, Long, Float>, Long>() {
//						@Override
//						public Long getKey(Tuple3<Long, Long, Float> value) throws Exception {
//							return value.f0;
//						}
//					}).window(TumblingEventTimeWindows.of(Time.hours(1))).reduce(new ReduceFunction<Tuple3<Long, Long, Float>>() {
//						@Override
//						public Tuple3<Long, Long, Float> reduce(Tuple3<Long, Long, Float> value1, Tuple3<Long, Long, Float> value2) throws Exception {
//							return value1.f2 > value2.f2 ? value1 : value2;
//						}
//					});

		// better solution
		DataStream<Tuple3<Long, Long, Float> > hourlyTip = fares.keyBy(fare -> fare.driverId)
				.window(TumblingEventTimeWindows.of(Time.hours(1))).process(new SumDriverTips());

		DataStream<Tuple3<Long, Long, Float> > hourlyMax = hourlyTip.keyBy(fare -> fare.f0)
				.window(TumblingEventTimeWindows.of(Time.hours(1))).maxBy(2);
		printOrTest(hourlyMax);

		// execute the transformation pipeline
		env.execute("Hourly Tips (java)");
	}


	public static class DriverTotalTip implements AggregateFunction<TaxiFare, Float, Float> {

		@Override
		public Float createAccumulator() {
			return 0f;
		}

		@Override
		public Float add(TaxiFare value, Float accumulator) {
			return value.tip + accumulator;
		}

		@Override
		public Float getResult(Float accumulator) {
			return accumulator;
		}

		@Override
		public Float merge(Float a, Float b) {
			return a + b;
		}
	}


	public static class ProcessWin1 extends ProcessWindowFunction<Float, Tuple3<Long, Long, Float>, Long, TimeWindow> {

		@Override
		public void process(Long aLong, Context context, Iterable<Float> elements, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			float maxTips = 0f;
			for (Float tip : elements) {
				if (tip > maxTips) {
					maxTips = tip;
				}
			}
			Long endTimeStamp = context.window().getEnd();
			out.collect(new Tuple3<>(endTimeStamp, aLong, maxTips));
		}
	}

	public static class SumDriverTips extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {

		@Override
		public void process(Long aLong, Context context, Iterable<TaxiFare> elements, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			Float sum = 0f;
			for (TaxiFare taxiFare : elements) {
				sum += taxiFare.tip;
			}
			out.collect(new Tuple3<>(context.window().getEnd(), aLong, sum));
		}
	}
}
