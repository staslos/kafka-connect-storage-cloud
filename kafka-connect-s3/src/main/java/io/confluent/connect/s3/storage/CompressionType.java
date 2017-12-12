/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.s3.storage;

import org.apache.kafka.connect.errors.ConnectException;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Supported compression types.
 *
 * <p>Closely modeled on {@link org.apache.kafka.common.record.CompressionType}.</p>
 */
public enum CompressionType {
  NONE(0, "none", ""),

  GZIP(1, "gzip", ".gz") {
    @Override
    public OutputStream wrapForOutput(OutputStream out) {
      try {
        return new GZIPOutputStream(out, 8 * 1024);
      } catch (Exception e) {
        throw new ConnectException(e);
      }
    }

    @Override
    public InputStream wrapForInput(InputStream in) {
      try {
        return new GZIPInputStream(in);
      } catch (Exception e) {
        throw new ConnectException(e);
      }
    }

    @Override
    public void finalize(OutputStream compressionFilter) {
      if (compressionFilter instanceof DeflaterOutputStream) {
        try {
          ((DeflaterOutputStream) compressionFilter).finish();
        } catch (Exception e) {
          throw new ConnectException(e);
        }
      } else {
        throw new ConnectException("Expected compressionFilter to be a DeflatorOutputStream, "
            + "but was passed an instance that does not match that type.");
      }
    }
  };

  public final int id;
  public final String name;
  public final String extension;

  CompressionType(int id, String name, String extension) {
    this.id = id;
    this.name = name;
    this.extension = extension;
  }

  public static CompressionType forName(String name) {
    if (NONE.name.equals(name)) {
      return NONE;
    } else if (GZIP.name.equals(name)) {
      return GZIP;
    } else {
      throw new IllegalArgumentException("Unknown compression name: " + name);
    }
  }

  /**
   * Wrap OUT with a filter that will compress data with this CompressionType.
   */
  public OutputStream wrapForOutput(OutputStream out) {
    return out;
  }

  /**
   * Wrap IN with a filter that will decompress data with this CompressionType.
   */
  public InputStream wrapForInput(InputStream in) {
    return in;
  }

  /**
   * Take any action necessary to finalize filter before the underlying
   * S3OutputStream is committed.
   */
  public void finalize(OutputStream compressionFilter) {}

}
