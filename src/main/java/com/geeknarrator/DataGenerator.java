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

import java.util.Random;

public class DataGenerator {

  public static void main(String[] args) {
    // user id
    // timestamp
    // activity
    // page
    String website = "geeknarrator.com";
    int numIds = 10000;
    long currentTime = 1663495628873L; //System.currentTimeMillis();
    Random random = new Random();
    for (int i = 0; i < numIds; i++) {
      int id = random.nextInt(1000);
      long timestamp  = currentTime + random.nextInt(10000);
      Data.Activity activity = Data.Activity.values()[random.nextInt(Data.Activity.values().length)];
      int page = random.nextInt(100);
//      System.out.printf("%s, %s, %s, %s/page%s\n", id, timestamp, activity, website, page);
      int purchaseId = random.nextInt(Integer.MAX_VALUE);
      System.out.printf("%s, %s, %s\n", id, timestamp, purchaseId);
    }
  }

}