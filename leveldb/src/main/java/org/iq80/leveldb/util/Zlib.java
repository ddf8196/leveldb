/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

public class Zlib
{
    private Zlib()
    {
    }

    public static void uncompress(InputStream input, OutputStream output, boolean raw)
            throws IOException
    {
        Inflater inflater = new Inflater(raw);
        InflaterInputStream inflaterInputStream = new InflaterInputStream(input, inflater);
        byte[] buffer = new byte[1024];
        int read;
        while ((read = inflaterInputStream.read(buffer)) > 0) {
            output.write(buffer, 0, read);
        }
        inflater.end();
    }

    public static int compress(InputStream input, OutputStream output, boolean raw)
            throws IOException
    {
        Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, raw);
        DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(output, deflater);
        byte[] buffer = new byte[1024];
        int read;
        while ((read = input.read(buffer)) > 0) {
            deflaterOutputStream.write(buffer, 0, read);
        }
        deflaterOutputStream.finish();
        int compressedSize = deflater.getTotalOut();
        deflater.end();
        return compressedSize;
    }
}
