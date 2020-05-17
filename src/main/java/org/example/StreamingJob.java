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

package org.example;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

		// Set up Twitter as the data source
		Properties props = new Properties();
		props.setProperty(TwitterSource.CONSUMER_KEY, "lDSDKAh07M5HAktpZIQkwi0Nn");
		props.setProperty(TwitterSource.CONSUMER_SECRET, "Zh3CldrxP8IbEDeTYTSlGghwgtoLwXCRqCLjWY6KvIqgvrJhFl");
		props.setProperty(TwitterSource.TOKEN, "468804467-L5O6Mu0PzETEIX8gXDSpvfcqcg07lNOpr413wGQO");
		props.setProperty(TwitterSource.TOKEN_SECRET, "zSISFJMu0NxMJMhXNVmNWxQhSSt1RmM2dHW3pMBlt5pYZ");
		DataStream<String> rawTweets = fsEnv.addSource(new TwitterSource(props)).name("Twitter Source");

		//Create a Table
		Table tblRawTweets = tableEnv.fromDataStream(rawTweets);
		tblRawTweets.printSchema();


		// execute program
		fsEnv.execute("Flink Streaming Java API Skeleton");
	}
}
