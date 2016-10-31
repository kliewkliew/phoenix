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

import java.util.Arrays;
import java.util.List;

import org.apache.phoenix.expression.function.FloorDecimalExpression;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;


/**
 *
 * Modulus expression implementation
 *
 * @since 0.1
 */
public abstract class ModulusExpression extends ArithmeticExpression {

    public ModulusExpression() { }

    public ModulusExpression(List<Expression> children) {
        super(children);
    }

    protected Expression getDividendExpression() {
        return children.get(0);
    }

    protected Expression getDivisorExpression() {
        return children.get(1);
    }

    @Override
    protected String getOperatorString() {
        return " % ";
    }

    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        List<T> l = acceptChildren(visitor, visitor.visitEnter(this));
        T t = visitor.visitLeave(this, l);
        if (t == null) {
            t = visitor.defaultReturn(this, l);
        }
        return t;
    }

}
