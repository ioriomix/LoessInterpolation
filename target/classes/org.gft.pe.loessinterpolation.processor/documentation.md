<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  ~
  -->

## Loess Interpolation

<p align="center"> 
    <img src="icon.png" width="150px;" class="pe-image-documentation"/>
</p>

***

## Description
Calculates the loess interpolation of a monitored value over a given time interval if greater than a threshold value.

***

## Required input
There should be a field to monitor in the event at certain times, if the difference of the timestamps is greater than a certain threshold (provided in input), the interpolation is calculated

***

## Configuration
###Value to Observe
Specifies the value field that should be monitored.

###Timestamp
Specify the timestamp.

###Threshold value
Specifies the threshold value.

## Output
Outputs events with the tween value if the timestamp difference is greater than the defined threshold.