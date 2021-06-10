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
package org.iq80.leveldb.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.iterator.SeekingIterator;
import org.iq80.leveldb.iterator.SeekingIterators;
import org.iq80.leveldb.table.UserComparator;
import org.iq80.leveldb.iterator.InternalIterator;
import org.iq80.leveldb.iterator.MergingIterator;
import org.iq80.leveldb.util.SafeListBuilder;
import org.iq80.leveldb.util.Slice;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.iq80.leveldb.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;
import static org.iq80.leveldb.impl.ValueType.VALUE;

// todo this class should be immutable
public class Level
{
    public static final Comparator<FileMetaData> NEWEST_FIRST = new Comparator<FileMetaData>()
    {
        @Override
        public int compare(FileMetaData fileMetaData, FileMetaData fileMetaData1)
        {
            return (int) (fileMetaData1.getNumber() - fileMetaData.getNumber());
        }
    };
    private final int levelNumber;
    private final TableCache tableCache;
    private final InternalKeyComparator internalKeyComparator;
    private final List<FileMetaData> files;

    public Level(int levelNumber, Collection<FileMetaData> files, TableCache tableCache, InternalKeyComparator internalKeyComparator)
    {
        checkArgument(levelNumber >= 0, "levelNumber is negative");
        requireNonNull(files, "files is null");
        requireNonNull(tableCache, "tableCache is null");
        requireNonNull(internalKeyComparator, "internalKeyComparator is null");

        this.files = new ArrayList<>(files);
        this.tableCache = tableCache;
        this.internalKeyComparator = internalKeyComparator;
        this.levelNumber = levelNumber;
    }

    public int getLevelNumber()
    {
        return levelNumber;
    }

    public List<FileMetaData> getFiles()
    {
        return files;
    }

    public InternalIterator iterator(ReadOptions options) throws IOException
    {
        if (levelNumber == 0) {
            try (SafeListBuilder<InternalIterator> builder = SafeListBuilder.builder()) {
                for (FileMetaData file : files) {
                    builder.add(tableCache.newIterator(file, options));
                }
                return new MergingIterator(builder.build(), internalKeyComparator);
            }
        }
        else {
            return createLevelConcatIterator(tableCache, files, internalKeyComparator, options);
        }
    }

    public static InternalIterator createLevelConcatIterator(final TableCache tableCache, List<FileMetaData> files, InternalKeyComparator internalKeyComparator, final ReadOptions options)
    {
        SeekingIterator<InternalKey, FileMetaData> iterator = SeekingIterators.fromSortedList(files,
                new Function<FileMetaData, InternalKey>()
                {
                    @Override
                    public InternalKey apply(FileMetaData fileMetaData)
                    {
                        return fileMetaData.getLargest();
                    }
                },
                new Function<FileMetaData, FileMetaData>()
                {
                    @Override
                    public FileMetaData apply(FileMetaData f)
                    {
                        return f;
                    }
                }, internalKeyComparator);

        return SeekingIterators.twoLevelInternalIterator(iterator,
                new Function<FileMetaData, SeekingIterator<InternalKey, Slice>>()
                {
                    @Override
                    public SeekingIterator<InternalKey, Slice> apply(FileMetaData fileMetaData)
                    {
                        try {
                            return tableCache.newIterator(fileMetaData, options);
                        }
                        catch (IOException e) {
                            throw new DBException(e);
                        }
                    }
                },
                new Closeable()
                {
                    @Override
                    public void close()
                            throws IOException
                    {
                    }
                });
    }

    public LookupResult get(ReadOptions options, LookupKey key, ReadStats readStats, ReadStats lasReadFile)
    {
        if (files.isEmpty()) {
            return null;
        }

        List<FileMetaData> fileMetaDataList = getFilesForKey(key.getUserKey(), key.getInternalKey());
        if (fileMetaDataList.isEmpty()) {
            return null;
        }

        for (FileMetaData fileMetaData : fileMetaDataList) {
            if (lasReadFile.getSeekFile() != null && readStats.getSeekFile() == null) {
                // We have had more than one seek for this read.  Charge the first file.
                readStats.setSeekFile(lasReadFile.getSeekFile());
                readStats.setSeekFileLevel(lasReadFile.getSeekFileLevel());
            }

            lasReadFile.setSeekFile(fileMetaData);
            lasReadFile.setSeekFileLevel(levelNumber);

            final LookupResult lookupResult = tableCache.get(options, key.getInternalKey().encode(), fileMetaData, new KeyMatchingLookup(key));
            if (lookupResult != null) {
                return lookupResult;
            }
        }

        return null;
    }

    public List<FileMetaData> getFilesForKey(Slice userKey, InternalKey internalKey)
    {
        final UserComparator userComparator = internalKeyComparator.getUserComparator();
        if (levelNumber == 0) {
            final List<FileMetaData> fileMetaDataList = new ArrayList<>(files.size());
            for (FileMetaData fileMetaData : files) {
                if (userComparator.compare(userKey, fileMetaData.getSmallest().getUserKey()) >= 0 &&
                        userComparator.compare(userKey, fileMetaData.getLargest().getUserKey()) <= 0) {
                    fileMetaDataList.add(fileMetaData);
                }
            }
            if (fileMetaDataList.isEmpty()) {
                return Collections.emptyList();
            }
            Collections.sort(fileMetaDataList, NEWEST_FIRST);
            return fileMetaDataList;
        }
        else {
            // Binary search to find earliest index whose largest key >= ikey.
            int index = findFile(internalKey);

            // did we find any files that could contain the key?
            if (index >= files.size()) {
                return Collections.emptyList();
            }

            // check if the smallest user key in the file is less than the target user key
            FileMetaData fileMetaData = files.get(index);
            if (userComparator.compare(userKey, fileMetaData.getSmallest().getUserKey()) < 0) {
                return Collections.emptyList();
            }

            // search this file
            return Collections.singletonList(fileMetaData);
        }
    }

    public boolean someFileOverlapsRange(boolean disjointSortedFiles, Slice smallestUserKey, Slice largestUserKey)
    {
        UserComparator userComparator = internalKeyComparator.getUserComparator();
        if (!disjointSortedFiles) {
            // Need to check against all files
            for (FileMetaData file : files) {
                if (afterFile(userComparator, smallestUserKey, file) ||
                        beforeFile(userComparator, largestUserKey, file)) {
                    // No overlap
                }
                else {
                    return true;  // Overlap
                }
            }
            return false;
        }
        int index = 0;
        if (smallestUserKey != null) {
            InternalKey smallestInternalKey = new InternalKey(smallestUserKey, MAX_SEQUENCE_NUMBER, VALUE);
            index = findFile(smallestInternalKey);
        }

        if (index >= files.size()) {
            // beginning of range is after all files, so no overlap.
            return false;
        }

        return !beforeFile(userComparator, largestUserKey, files.get(index));
    }

    private boolean beforeFile(UserComparator userComparator, Slice userKey, FileMetaData file)
    {
        // null userKey occurs after all keys and is therefore never before *f
        return (userKey != null &&
                userComparator.compare(userKey, file.getSmallest().getUserKey()) < 0);
    }

    private boolean afterFile(UserComparator userComparator, Slice userKey, FileMetaData file)
    {
        // NULL user_key occurs before all keys and is therefore never after *f
        return (userKey != null &&
                userComparator.compare(userKey, file.getLargest().getUserKey()) > 0);
    }

    @VisibleForTesting
    int findFile(InternalKey targetKey)
    {
        // todo replace with Collections.binarySearch
        int left = 0;
        int right = files.size();

        // binary search restart positions to find the restart position immediately before the targetKey
        while (left < right) {
            int mid = (left + right) / 2;

            if (internalKeyComparator.compare(files.get(mid).getLargest(), targetKey) < 0) {
                // Key at "mid.largest" is < "target".  Therefore all
                // files at or before "mid" are uninteresting.
                left = mid + 1;
            }
            else {
                // Key at "mid.largest" is >= "target".  Therefore all files
                // after "mid" are uninteresting.
                right = mid;
            }
        }
        return right;
    }

    public void addFile(FileMetaData fileMetaData)
    {
        // todo remove mutation
        files.add(fileMetaData);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Level");
        sb.append("{levelNumber=").append(levelNumber);
        sb.append(", files=").append(files);
        sb.append('}');
        return sb.toString();
    }
}
