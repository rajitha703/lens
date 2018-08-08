/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.server.api;

import lombok.*;

@AllArgsConstructor
public class LensErrorInfo {

  @Getter
  private int errorCode;
  @Getter
  private int errorWeight;
  @Getter
  private String errorName;

  @Override
  public boolean equals(final Object o) {

    if (this == o) {
      return true;
    }

    if (!(o instanceof LensErrorInfo)) {
      return false;
    }

    LensErrorInfo e = (LensErrorInfo) o;
    return errorCode == e.errorCode && errorWeight == e.errorWeight && errorName.equals(e.errorName);
  }


  @Override
  public int hashCode() {

    final int PRIME = 59;
    int result = 1;

    result = result * PRIME + errorCode;
    result = result * PRIME + errorWeight;
    result = result * PRIME + errorName.hashCode();
    return result;
  }

}
