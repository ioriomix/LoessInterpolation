/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.gft.pe.loessinterpolation;

import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.DataProcessorType;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.model.schema.PropertyScope;
import org.apache.streampipes.sdk.builder.PrimitivePropertyBuilder;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.helpers.*;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.sdk.utils.Datatypes;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.routing.SpOutputCollector;
import org.apache.streampipes.wrapper.standalone.ProcessorParams;
import org.apache.streampipes.wrapper.standalone.StreamPipesDataProcessor;
import org.apache.commons.math3.analysis.interpolation.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;


public class LoessInterpolationDataProcessor extends StreamPipesDataProcessor {

  private String input_value;
  private String timestamp_value;

  private static final String INPUT_VALUE = "value";
  private static final String TIMESTAMP_VALUE = "timestamp_value";
  private static final String THRESHOLD = "threshold";

  private Double threshold;

  double[] array3X = {0.0,0.0,0.0,0.0,0.0};
  double[] array3Y = {0.0,0.0,0.0,0.0,0.0};


  @Override
  public DataProcessorDescription declareModel() {
    return ProcessingElementBuilder.create("org.gft.pe.loessinterpolation.processor")
            .withAssets(Assets.DOCUMENTATION, Assets.ICON)
            .withLocales(Locales.EN)
            .category(DataProcessorType.AGGREGATE)

            .requiredStream(StreamRequirementsBuilder.create()
                    .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                            Labels.withId(INPUT_VALUE), PropertyScope.NONE)
                    .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                            Labels.withId(TIMESTAMP_VALUE), PropertyScope.NONE)
                    .build())

            .requiredFloatParameter(Labels.withId(THRESHOLD))

            .outputStrategy(OutputStrategies.append(PrimitivePropertyBuilder.create(Datatypes.Double, "chosen_timestamp").build()
                    ,PrimitivePropertyBuilder.create(Datatypes.Double, "interpolation_value").build()))

            .build();
  }

  @Override
  public void onInvocation(ProcessorParams processorParams,
                            SpOutputCollector out,
                            EventProcessorRuntimeContext ctx) throws SpRuntimeException  {

    this.input_value = processorParams.extractor().mappingPropertyValue(INPUT_VALUE);
    this.timestamp_value = processorParams.extractor().mappingPropertyValue(TIMESTAMP_VALUE);
    this.threshold = processorParams.extractor().singleValueParameter(THRESHOLD,Double.class);

  }

  @Override
  public void onEvent(Event event,SpOutputCollector out) {

    Double xi = 0.0;
    Double yi = 0.0;

    //recovery input value
    Double value = event.getFieldBySelector(this.input_value).getAsPrimitive().getAsDouble();

    //recovery timestamp value
    String timestampStr = event.getFieldBySelector(this.timestamp_value).getAsPrimitive().getAsString();

    //convert timestamp to double
    Double timestamp = Double.parseDouble(timestampStr);

    //recover type of interpolation
    //if we are in the first event it sets the [0] values of the two arrays with the data arriving from SP
    if ((array3Y[0] == 0.0 && array3X[0] == 0.0)) {

      array3X[0] = timestamp;
      array3Y[0] = value;

      //if we are in the second event it sets the [1] values of the two arrays with the data arriving from SP
    } else if ((array3Y[1] == 0.0 && array3X[1] == 0.0)) {

      array3X[1] = timestamp;
      array3Y[1] = value;

    } else if ((array3Y[2] == 0.0 && array3X[2] == 0.0)) {

      array3X[2] = timestamp;
      array3Y[2] = value;

    } else if ((array3Y[3] == 0.0 && array3X[3] == 0.0)) {

      array3X[3] = timestamp;
      array3Y[3] = value;

    } else if ((array3Y[4] == 0.0 && array3X[4] == 0.0)) {

      array3X[4] = timestamp;
      array3Y[4] = value;


      //if the new timestamp is equal than the timestamp previously or the difference is more low to the threshold,
      //do not perform an interpolation
    } else if ((array3X[0] == timestamp) || (array3X[1] == timestamp) ||
            (array3X[2] == timestamp) || (array3X[3] == timestamp) || (array3X[4] == timestamp) ||
            (timestamp - array3X[0] < this.threshold) || (timestamp - array3X[1] < this.threshold) ||
            (timestamp - array3X[2] < this.threshold) || (timestamp - array3X[3] < this.threshold) ||
            (timestamp - array3X[4] < this.threshold)) {
      System.out.println("--------- Timestamp Values not accepted ------- ");

      //perform an interpolation
    } else {
      array3X[4] = timestamp;
      array3Y[4] = value;

      //perform a mathematical median for an array of two timestamp values
      BigDecimal bd = new BigDecimal((array3X[0] + array3X[1] + array3X[2] + array3X[3] + array3X[4]) / 5).setScale(2, RoundingMode.HALF_UP);
      xi = bd.doubleValue();

      System.out.println("array3X: " + Arrays.toString(array3X));
      System.out.println("array3Y: " + Arrays.toString(array3Y));

      yi = loessInterp(array3X, array3Y, xi);
      System.out.println("***** yi *****: " + yi);

      for (int i = 0; i < 4; ++i) {
        //move the second value of the array to the first position
        array3X[i] = array3X[i+1];
        array3Y[i] = array3Y[i+1];

      }

      //set the values resulting from the interpolation, in the fields of the event output
      event.addField("chosen_timestamp", xi);
      event.addField("interpolation_value", yi);

      out.collect(event);
    }
  }

  @Override
  public void onDetach(){
  }


  public double loessInterp(double[] x, double[] y, double xi) {

    LoessInterpolator li = new LoessInterpolator(0.6, 2, 1e-12);
    double[] res = li.smooth(x, y);
    PolynomialSplineFunction psf = li.interpolate(x, y);
    double yi = psf.value(xi);
    return yi;

  }


}
