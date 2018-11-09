/*
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
package org.apache.druid.query.lookup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.linkedin.paldb.api.StoreReader;
import org.apache.druid.common.config.NullHandling;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@JsonTypeName("pal")
public class PaldbLookupExtractor extends LookupExtractor
{

  private final StoreReader reader;

  @JsonCreator
  public PaldbLookupExtractor(
      @JsonProperty("reader") StoreReader reader
  )
  {
    this.reader = reader;
  }

  @JsonProperty
  public StoreReader getReader()
  {
    return reader;
  }

  @Nullable
  @Override
  public String apply(@Nullable String key)
  {
    String keyEquivalent = NullHandling.nullToEmptyIfNeeded(key);
    if (keyEquivalent == null) {
      return null;
    }
    return NullHandling.emptyToNullIfNeeded(reader.get(keyEquivalent));
  }

  @Override
  public List<String> unapply(@Nullable String value)
  {
    String valueToLookup = NullHandling.nullToEmptyIfNeeded(value);
    if (valueToLookup == null) {
      return Collections.emptyList();
    }
    Iterable<Map.Entry<String, String>> list = reader.iterable();
    List<String> keys = StreamSupport.stream(list.spliterator(), false)
                                     .filter(t -> t.getValue().equals(valueToLookup))
                                     .map(entry -> entry.getKey())
                                     .collect(Collectors.toList());
    return keys;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new byte[0];
  }

}


