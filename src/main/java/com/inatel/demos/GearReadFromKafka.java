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

package com.inatel.demos;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.util.Collector;
import java.util.Properties;

public class GearReadFromKafka {
	
  static final int CAR_NUM = 3;
  static final int INTERVAL = 3;

  public static void main(String[] args) throws Exception {
    // create execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "flink_consumer");

    DataStream stream = env.addSource(
            new FlinkKafkaConsumer09<>("telemetry", 
            new JSONDeserializationSchema(), 
            properties)
    );
 

    stream  .flatMap(new TelemetryJsonParser())
    		.keyBy(0)
    		.timeWindow(Time.seconds(20))
            .reduce(new GearReducer())
            .flatMap(new GearFlatMapper())
            .map(new GearPrinter())
            .print();

    env.execute();    
    }

    @SuppressWarnings("serial")
	static class TelemetryJsonParser implements FlatMapFunction<ObjectNode, Tuple3<Integer, Integer, Integer>> {
      @Override
      public void flatMap(ObjectNode jsonTelemetry, Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {
        JsonNode jsonNode = jsonTelemetry.get("Car");
        int carN = jsonNode.asInt();
        if (carN == CAR_NUM)
        {
        	float time = jsonTelemetry.get("time").floatValue();
        	int gear = jsonTelemetry.get("telemetry").get("Gear").intValue();
        	out.collect(new Tuple3<>((int) Math.floor(time/INTERVAL), gear, 1));
        }
      }
    }
    
    @SuppressWarnings("serial")
    static class GearReducer implements ReduceFunction<Tuple3<Integer, Integer, Integer>> {
      @Override
      public Tuple3<Integer, Integer, Integer> reduce(Tuple3<Integer, Integer, Integer> value1, Tuple3<Integer, Integer, Integer> value2) {
    	//System.out.println("Values - Time = "+value1.f0+" Gear = "+value1.f1+" Counter = "+value1.f2);
    	if (value1.f1 != value2.f1)
    	{
    		return new Tuple3<>(value2.f0, value2.f1, value1.f2 + 1);
    	}
    	return new Tuple3<>(value1.f0, value1.f1, value1.f2);
      }
    }
    
    @SuppressWarnings("serial")
	static class GearFlatMapper implements FlatMapFunction<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>> {
      @Override
      public void flatMap(Tuple3<Integer, Integer, Integer> carInfo, Collector<Tuple2<Integer, Integer>> out) throws Exception {    	  
      	out.collect(new Tuple2<>(carInfo.f0, carInfo.f2));      
      }
    }
    
    @SuppressWarnings("serial")
    static class CounterReducer implements ReduceFunction<Tuple2<Integer, Integer>> {
      @Override
      public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) {
    	//System.out.println("Values - Time = "+value1.f0+" Gear = "+value1.f1+" Counter = "+value1.f2);    	
    	return new Tuple2<>(value1.f0, value2.f1 > value1.f1 ? value2.f1 : value1.f1);
      }
    }
        
    @SuppressWarnings("serial")
    static class GearPrinter implements MapFunction<Tuple2<Integer, Integer>, String> {
      @Override
      public String map(Tuple2<Integer, Integer> carInfo) throws Exception {
    	//System.out.println("Values - Time = "+carInfo.f0+" Counter = "+carInfo.f1);  
        return  String.format("Car3 - %dº intervalo (de %ds) : nº de trocas - %d", carInfo.f0+1, INTERVAL,carInfo.f1 ) ;
      }
    }

  }
