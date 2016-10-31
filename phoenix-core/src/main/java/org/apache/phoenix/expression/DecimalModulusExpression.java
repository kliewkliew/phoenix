/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.expression;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.FloorDecimalExpression;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;

import java.util.Arrays;
import java.util.List;

public class DecimalModulusExpression extends ModulusExpression {

    public DecimalModulusExpression() {
    }

    public DecimalModulusExpression(List<Expression> children) {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        final Expression quotientExpression = new DecimalDivideExpression(children);
        final Expression quotientFloorExpression = new FloorDecimalExpression(Arrays.asList(
                quotientExpression,
                LiteralExpression.newConstant(0)));
        final Expression minuend = getDividendExpression();
        final Expression subtrahend = new DecimalMultiplyExpression(Arrays.asList(
                quotientFloorExpression, getDivisorExpression()));
        final Expression remainderExpression = new DecimalSubtractExpression(Arrays.asList(
                minuend, subtrahend));

        return remainderExpression.evaluate(tuple, ptr);
    }

    @Override
    public PDataType getDataType() {
        return PDecimal.INSTANCE;
    }

    @Override
    public ArithmeticExpression clone(List<Expression> children) {
        return new DecimalModulusExpression(children);
    }

}
