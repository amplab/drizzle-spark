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
 */

package org.apache.spark.network.shuffle.protocol;

import java.util.Arrays;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encoders;

/** Used to notify a remote BlockManager that a map task has finished computing its shuffle data. */
public class MapOutputReady extends BlockTransferMessage {
  public final int shuffleId;
  public final int mapId;
  public final int numReduces;
  public final byte[] serializedMapStatus;

    public MapOutputReady(int shuffleId, int mapId, int numReduces,  byte[] serializedMapStatus) {
      this.shuffleId = shuffleId;
      this.mapId = mapId;
      this.numReduces = numReduces;
      this.serializedMapStatus = serializedMapStatus;
    }

    @Override
    protected Type type() { return Type.MAP_OUTPUT_READY; }

    @Override
    public int hashCode() {
      return (Objects.hashCode(shuffleId, mapId, numReduces) * 41 +
        Arrays.hashCode(serializedMapStatus));
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("shuffleId", shuffleId)
        .add("mapId", mapId)
        .add("numReduces", numReduces)
        .add("map statuses size", serializedMapStatus.length)
        .toString();
    }

    @Override
    public boolean equals(Object other) {
      if (other != null && other instanceof MapOutputReady) {
        MapOutputReady o = (MapOutputReady) other;
        return Objects.equal(shuffleId, o.shuffleId)
          && Objects.equal(mapId, o.mapId)
          && Objects.equal(numReduces, o.numReduces)
          && Arrays.equals(serializedMapStatus, o.serializedMapStatus);
      }
      return false;
    }

    @Override
    public int encodedLength() {
      return 4 + 4 + 4 + Encoders.ByteArrays.encodedLength(serializedMapStatus);
    }

    @Override
    public void encode(ByteBuf buf) {
      buf.writeInt(shuffleId);
      buf.writeInt(mapId);
      buf.writeInt(numReduces);
      Encoders.ByteArrays.encode(buf, serializedMapStatus);
    }

    public static MapOutputReady decode(ByteBuf buf) {
      int shuffleId = buf.readInt();
      int mapId = buf.readInt();
      int numReduces = buf.readInt();
      byte[] serializedMapStatus = Encoders.ByteArrays.decode(buf);
      return new MapOutputReady(shuffleId, mapId, numReduces, serializedMapStatus);
    }
}
