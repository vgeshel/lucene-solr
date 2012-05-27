package org.apache.lucene.util;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

/**
 * Common util methods for dealing with {@link IndexReader}s.
 *
 * @lucene.internal
 */
public final class ReaderUtil {
  private static final ForkJoinPool fjPool = new ForkJoinPool();

  private ReaderUtil() {} // no instance

  /**
   * Gathers sub-readers from reader into a List.
   * 
   * @param allSubReaders
   * @param reader
   */
  public static void gatherSubReaders(List<IndexReader> allSubReaders, IndexReader reader) {
    IndexReader[] subReaders = reader.getSequentialSubReaders();
    if (subReaders == null) {
      // Add the reader itself, and do not recurse
      allSubReaders.add(reader);
    } else {
      for (int i = 0; i < subReaders.length; i++) {
        gatherSubReaders(allSubReaders, subReaders[i]);
      }
    }
  }

  public static interface ReaderVisitor<T> {
    T process(IndexReader r) throws IOException;

    T add(T t1, T t2);

    T initial();
  }

  private static class ReaderTask<T> extends RecursiveTask<T> {
    /**
     * 
     */
    private static final long serialVersionUID = -5820240087339249330L;
    private ReaderVisitor<T> v;
    private IndexReader r;

    private ReaderTask(ReaderVisitor<T> visitor, IndexReader r) {
      this.v = visitor;
      this.r = r;
    }

    @Override
    protected T compute() {
      IndexReader[] subs = r.getSequentialSubReaders();

      if (subs == null || subs.length == 0) {
        try {
          return v.process(r);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        ReaderTask<T>[] tasks = new ReaderTask[subs.length];

        for (int i = 0; i < subs.length; i ++) {
          tasks[i] = new ReaderTask<T>(v, subs[i]);
          tasks[i].fork();
        }

        T ret = v.initial();

        for (int i = 0; i < tasks.length; i ++) {
          ret = v.add(ret, tasks[i].join());
        }

        return ret;
      }
    }

  }

  public static <T> T visitReader(IndexReader reader, ReaderVisitor<T> v) {
    return fjPool.invoke(new ReaderTask<T>(v, reader));
  }

  public static <T> T visitReaders(IndexReader[] readers, ReaderVisitor<T> v) {
    if (readers.length == 0) {
      return v.initial();
    } else if (readers.length == 1) {
      try {
        return v.process(readers[0]);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      ReaderTask<T>[] tasks = new ReaderTask[readers.length];

      for (int i = 0; i < readers.length; i ++) {
        tasks[i] = new ReaderTask<T>(v, readers[i]);
        fjPool.submit(tasks[i]);
      }

      T ret = v.initial();

      for (int i = 0; i < tasks.length; i ++) {
        ret = v.add(ret, tasks[i].join());
      }

      return ret;
    }
  }

  private static class DocFreqCollector implements ReaderVisitor<Integer> {
    private final Term t;


    public DocFreqCollector(Term t) {
      super();
      this.t = t;
    }

    @Override
    public Integer process(IndexReader r) throws IOException {
      return r.docFreq(t);
    }

    @Override
    public Integer add(Integer t1, Integer t2) {
      return t1 + t2;
    }

    @Override
    public Integer initial() {
      return 0;
    }

  }

  public static int computeDocFreq(IndexReader reader, final Term t) throws IOException {
    return visitReader(reader, new DocFreqCollector(t));
  }

  public static int computeDocFreq(IndexReader[] readers, final Term t) throws IOException {
    return visitReaders(readers, new DocFreqCollector(t));
  }

  /**
   * Returns sub IndexReader that contains the given document id.
   *    
   * @param doc id of document
   * @param reader parent reader
   * @return sub reader of parent which contains the specified doc id
   */
  public static IndexReader subReader(int doc, IndexReader reader) {
    List<IndexReader> subReadersList = new ArrayList<IndexReader>();
    ReaderUtil.gatherSubReaders(subReadersList, reader);
    IndexReader[] subReaders = subReadersList
        .toArray(new IndexReader[subReadersList.size()]);
    int[] docStarts = new int[subReaders.length];
    int maxDoc = 0;
    for (int i = 0; i < subReaders.length; i++) {
      docStarts[i] = maxDoc;
      maxDoc += subReaders[i].maxDoc();
    }
    return subReaders[ReaderUtil.subIndex(doc, docStarts)];
  }

  /**
   * Returns sub-reader subIndex from reader.
   * 
   * @param reader parent reader
   * @param subIndex index of desired sub reader
   * @return the subreader at subIndex
   */
  public static IndexReader subReader(IndexReader reader, int subIndex) {
    List<IndexReader> subReadersList = new ArrayList<IndexReader>();
    ReaderUtil.gatherSubReaders(subReadersList, reader);
    IndexReader[] subReaders = subReadersList
        .toArray(new IndexReader[subReadersList.size()]);
    return subReaders[subIndex];
  }


  /**
   * Returns index of the searcher/reader for document <code>n</code> in the
   * array used to construct this searcher/reader.
   */
  public static int subIndex(int n, int[] docStarts) { // find
    // searcher/reader for doc n:
    int size = docStarts.length;
    int lo = 0; // search starts array
    int hi = size - 1; // for first element less than n, return its index
    while (hi >= lo) {
      int mid = (lo + hi) >>> 1;
      int midValue = docStarts[mid];
      if (n < midValue)
        hi = mid - 1;
      else if (n > midValue)
        lo = mid + 1;
      else { // found a match
        while (mid + 1 < size && docStarts[mid + 1] == midValue) {
          mid++; // scan to last match
        }
        return mid;
      }
    }
    return hi;
  }
}
