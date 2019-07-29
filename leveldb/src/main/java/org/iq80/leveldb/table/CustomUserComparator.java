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
package org.iq80.leveldb.table;

import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.util.Slice;

import static java.util.Objects.requireNonNull;

public class CustomUserComparator
        implements UserComparator
{
    private final DBComparator comparator;

    public CustomUserComparator(DBComparator comparator)
    {
        requireNonNull(comparator.name(), "User Comparator name can't be null");
        this.comparator = comparator;
    }

    @Override
    public String name()
    {
        return comparator.name();
    }

    @Override
    public Slice findShortestSeparator(Slice start, Slice limit)
    {
        byte[] shortestSeparator = comparator.findShortestSeparator(start.getBytes(), limit.getBytes());
        requireNonNull(shortestSeparator, "User comparator returned null from findShortestSeparator()");
        return new Slice(shortestSeparator);
    }

    @Override
    public Slice findShortSuccessor(Slice key)
    {
        byte[] shortSuccessor = comparator.findShortSuccessor(key.getBytes());
        requireNonNull(comparator, "User comparator returned null from findShortSuccessor()");
        return new Slice(shortSuccessor);
    }

    @Override
    public boolean startWith(Slice key, Slice prefix)
    {
        return comparator.startWith(key.getBytes(), prefix.getBytes());
    }

    @Override
    public int compare(Slice o1, Slice o2)
    {
        return comparator.compare(o1.getBytes(), o2.getBytes());
    }
}
