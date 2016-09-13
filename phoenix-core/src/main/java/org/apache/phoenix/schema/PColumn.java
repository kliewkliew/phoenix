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
package org.apache.phoenix.schema;


/**
 * Definition of a Phoenix column
 *
 * 
 * @since 0.1
 */
public interface PColumn extends PDatum {

    /**
     * @return the name of the column qualifier
     */
    PName getName();

    /**
     * @return the name of the column family
     */
    PName getFamilyName();

    /**
     * @return the zero-based ordinal position of the column
     */
    int getPosition();
    
    /**
     * @return the declared array size or zero if this is not an array
     */
    Integer getArraySize();
    
    byte[] getViewConstant();
    
    boolean isViewReferenced();
    
    int getEstimatedSize();

    String getExpressionStr();
    
    /**
     * @return whether this column represents/stores the hbase cell timestamp.
     */
    boolean isRowTimestamp();
    
    boolean isDynamic();
}
