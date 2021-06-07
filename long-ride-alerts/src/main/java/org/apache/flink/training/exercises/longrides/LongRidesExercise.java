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

package org.apache.flink.training.exercises.longrides;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.util.Collector;

/**
 * The "Long Ride Alerts" exercise of the Flink training in the docs.
 *
 * <p>The goal for this exercise is to emit START events for taxi rides that have not been matched
 * by an END event during the first 2 hours of the ride.
 *
 */
public class LongRidesExercise extends ExerciseBase {

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
		DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()));

		DataStream<TaxiRide> longRides = rides
				.keyBy((TaxiRide ride) -> ride.rideId)
				.process(new MatchFunction());

		printOrTest(longRides);

		env.execute("Long Taxi Rides");
	}

	public static class MatchFunction extends KeyedProcessFunction<Long, TaxiRide, TaxiRide> {

		private final long durationMsec = Time.hours(2).toMilliseconds();

		private transient MapState<Long, Boolean> isRideEndMap;
		private transient MapState<Long, TaxiRide> startTaxiRideMap;

		@Override
		public void open(Configuration config) throws Exception {
			MapStateDescriptor<Long, Boolean> rideEndDesc =
					new MapStateDescriptor<Long, Boolean>("rideEndDesc", Long.class, Boolean.class);
			isRideEndMap = getRuntimeContext().getMapState(rideEndDesc);

			MapStateDescriptor<Long, TaxiRide> startTaxiRideDesc =
					new MapStateDescriptor<Long, TaxiRide>("startTaxiRideDesc", Long.class, TaxiRide.class);
			startTaxiRideMap = getRuntimeContext().getMapState(startTaxiRideDesc);
		}

		@Override
		public void processElement(TaxiRide ride, Context context, Collector<TaxiRide> out) throws Exception {
			TimerService timerService = context.timerService();
			long eventTime = ride.getEventTime();
			if (eventTime <= timerService.currentWatermark()) {

			} else {
				if (ride.isStart) {
					startTaxiRideMap.put(ride.rideId, ride);
					timerService.registerEventTimeTimer(eventTime + durationMsec);
				} else {
					isRideEndMap.put(ride.rideId, true);
				}
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext context, Collector<TaxiRide> out) throws Exception {
			long rideId = context.getCurrentKey();
			Boolean isEnd = isRideEndMap.get(rideId);
			if (isEnd == null || !isEnd) {
				out.collect(startTaxiRideMap.get(rideId));
			}
		}
	}
}
