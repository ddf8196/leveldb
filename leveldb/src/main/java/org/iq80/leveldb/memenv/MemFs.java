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
package org.iq80.leveldb.memenv;

import com.google.common.base.Optional;
import org.iq80.leveldb.env.DbLock;
import org.iq80.leveldb.env.File;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

class MemFs
{
    public static final char SEPARATOR_CHAR = '/';
    public static final String SEPARATOR = "/";
    private final Object lock = new Object();
    private final Set<MemFile> dirs = new HashSet<>();
    private final Map<MemFile, FileState> maps = new HashMap<>();

    public File createTempDir(String prefix)
    {
        synchronized (lock) {
            MemFile e;
            do {
                String baseName = "/";
                if (prefix != null) {
                    baseName += prefix + "-";
                }

                baseName += System.currentTimeMillis() + "-";
                e = MemFile.createMemFile(this, baseName);
            } while (maps.containsKey(e) || !dirs.add(e));
            e.mkdirs();
            return e;
        }
    }

    public FileState requireFile(MemFile file) throws FileNotFoundException
    {
        FileState fileState;
        synchronized (lock) {
            fileState = maps.get(file);
            if (fileState == null) {
                throw new FileNotFoundException(file.getPath());
            }
        }
        return fileState;
    }

    public FileState getOrCreateFile(MemFile file) throws IOException
    {
        FileState fileState;
        synchronized (lock) {
            if (dirs.contains(file)) {
                throw new IOException(file + " is a directory");
            }
            if (!dirs.contains(file.getParentFile())) {
                throw new IOException("Unable to create file " + file + ", parent directory does not exist");
            }
            fileState = maps.get(file);
            if (fileState == null) {
                fileState = new FileState();
                maps.put(file, fileState);
            }
        }
        return fileState;
    }

    public boolean mkdirs(MemFile memFile)
    {
        synchronized (lock) {
            if (maps.containsKey(memFile)) {
                return false;
            }
            dirs.add(memFile);
            return true;
        }
    }

    public boolean canRead(MemFile memFile)
    {
        synchronized (lock) {
            return maps.containsKey(memFile);
        }
    }

    public boolean isFile(MemFile memFile)
    {
        return canRead(memFile);
    }

    public boolean isDirectory(MemFile memFile)
    {
        synchronized (lock) {
            return dirs.contains(memFile);
        }
    }

    public Optional<FileState> getFileState(MemFile file)
    {
        synchronized (lock) {
            return Optional.fromNullable(maps.get(file));
        }
    }

    public boolean delete(MemFile memFile)
    {
        synchronized (lock) {
            return maps.remove(memFile) != null || dirs.remove(memFile);
        }
    }

    public List<File> listFiles(MemFile memFile)
    {
        synchronized (lock) {
            //return children(memFile).collect(Collectors.toList());
            String s = memFile.getPath() + SEPARATOR;
            Set<MemFile> concat = new HashSet<>(maps.size() + dirs.size());
            concat.addAll(maps.keySet());
            concat.addAll(dirs);
            Iterator<MemFile> iter = concat.iterator();
            while (iter.hasNext()) {
                if (!iter.next().getPath().startsWith(s)) {
                    iter.remove();
                }
            }
            List<File> result = new ArrayList<>();
            for (MemFile e : concat) {
                int i = e.getPath().indexOf(SEPARATOR, s.length());
                File f = i >= 0 ? MemFile.createMemFile(this, e.getPath().substring(0, i)) : e;
                if (!result.contains(f)) {
                    result.add(f);
                }
            }
            return result;
        }
    }
    /*
    private Stream<MemFile> children(MemFile memFile)
    {
        String s = memFile.getPath() + SEPARATOR;
        return Stream.concat(maps.keySet().stream(), dirs.stream())
            .filter(e -> e.getPath().startsWith(s))
            .map(e -> {
                int i = e.getPath().indexOf(SEPARATOR, s.length());
                return i >= 0 ? MemFile.createMemFile(this, e.getPath().substring(0, i)) : e;
            })
            .distinct();
    }
    */

    public boolean renameTo(MemFile from, MemFile dest)
    {
        synchronized (lock) {
            if (isDirectory(from)) {
                //not supported, not required by DB
                return false;
            }
            if (maps.containsKey(from) && !maps.containsKey(dest)) {
                maps.put(dest, maps.get(from));
                maps.remove(from);
                return true;
            }
        }
        return false;
    }

    public boolean deleteRecursively(MemFile memFile)
    {
        String prefix = memFile.getPath() + SEPARATOR;
        synchronized (lock) {
            boolean r = false;
            for (Iterator<MemFile> iterator = dirs.iterator(); iterator.hasNext(); ) {
                MemFile dir = iterator.next();
                if (dir.equals(memFile) || dir.getPath().startsWith(prefix)) {
                    iterator.remove();
                    r = true;
                }
            }
            for (Iterator<Map.Entry<MemFile, FileState>> iterator = maps.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<MemFile, FileState> entry = iterator.next();
                if (entry.getKey().equals(memFile) || entry.getKey().getPath().startsWith(prefix)) {
                    iterator.remove();
                    r = true;
                }
            }
            return r;
        }
    }

    public boolean exists(MemFile memFile)
    {
        synchronized (lock) {
            return dirs.contains(memFile) || maps.containsKey(memFile);
        }
    }

    public DbLock doLock(MemFile file) throws IOException
    {
        final FileState orCreateFile;
        synchronized (lock) {
            orCreateFile = getOrCreateFile(file);
            if (orCreateFile.isLocked()) {
                throw new IOException("lock on " + file + " already owned");
            }
            orCreateFile.setLocked(true);
        }
        return new MemDbLock(orCreateFile);
    }

    private class MemDbLock implements DbLock
    {
        private final FileState file;
        private boolean released;

        public MemDbLock(FileState file)
        {
            this.file = file;
        }

        @Override
        public boolean isValid()
        {
            synchronized (lock) {
                return !released;
            }
        }

        @Override
        public void release()
        {
            synchronized (lock) {
                if (!released) {
                    released = true;
                    file.setLocked(false);
                }
            }
        }
    }
}
