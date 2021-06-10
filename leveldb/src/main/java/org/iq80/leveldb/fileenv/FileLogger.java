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
package org.iq80.leveldb.fileenv;

import com.google.common.base.Supplier;
import org.iq80.leveldb.Logger;
import org.iq80.leveldb.util.LogMessageFormatter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Date;

class FileLogger
        implements Logger
{
    private final PrintStream ps;
    private final LogMessageFormatter formatter;

    private FileLogger(PrintStream ps, LogMessageFormatter formatter)
    {
        this.ps = ps;
        this.formatter = formatter;
    }

    public static Logger createLogger(OutputStream outputStream, Supplier<Date> clock)
    {
        return new FileLogger(new PrintStream(outputStream), new LogMessageFormatter(clock));
    }

    public static Logger createFileLogger(File loggerFile) throws IOException
    {
        return createLogger(new FileOutputStream(loggerFile),
                new Supplier<Date>()
                {
                    @Override
                    public Date get()
                    {
                        return new Date();
                    }
                });
    }

    @Override
    public void log(String template, Object... args)
    {
        log2(formatter.format(template, args));
    }

    @Override
    public void log(String message)
    {
        log2(formatter.format(message));
    }

    private void log2(String message)
    {
        ps.println(message);
    }

    @Override
    public void close() throws IOException
    {
        ps.close();
    }
}
