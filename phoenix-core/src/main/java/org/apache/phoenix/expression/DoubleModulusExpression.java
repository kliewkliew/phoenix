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
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;

import java.util.List;

public class DoubleModulusExpression extends ModulusExpression {

    public DoubleModulusExpression() {
    }

    public DoubleModulusExpression(List<Expression> children) {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        // get the dividend
        if (!getDividendExpression().evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return true;
        }
        final double dividend = getDividendExpression().getDataType().getCodec()
                .decodeDouble(ptr, getDividendExpression().getSortOrder());

        // get the divisor
        if (!getDivisorExpression().evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return true;
        }
        final double divisor = getDivisorExpression().getDataType().getCodec()
                .decodeDouble(ptr, getDivisorExpression().getSortOrder());

        // actually perform modulus
        final double remainder = dividend % divisor;

        // return the result, use encodeDouble to avoid extra Double allocation
        final byte[] resultPtr = new byte[PDouble.INSTANCE.getByteSize()];
        getDataType().getCodec().encodeDouble(remainder, resultPtr, 0);
        ptr.set(resultPtr);
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDouble.INSTANCE;
    }

    @Override
    public ArithmeticExpression clone(List<Expression> children) {
        return new DoubleModulusExpression(children);
    }

}
