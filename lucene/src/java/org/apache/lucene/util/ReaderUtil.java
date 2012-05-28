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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.atomic.AtomicInteger;

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

  public static interface ReaderVisitor {
    void visit(IndexReader r);
  }

  private static class ReaderAction extends RecursiveAction {
    /**
     * 
     */
    private static final long serialVersionUID = -5820240087339249330L;
    private ReaderVisitor v;
    private IndexReader r;

    private ReaderAction(ReaderVisitor visitor, IndexReader r) {
      this.v = visitor;
      this.r = r;
    }

    @Override
    protected void compute() {
      IndexReader[] subs = r.getSequentialSubReaders();

      if (subs == null || subs.length == 0) {
        v.visit(r);
      } else {
        List<ReaderAction> tasks = new ArrayList<ReaderAction>(subs.length);

        for (int i = 0; i < subs.length; i ++) {
          tasks.add(new ReaderAction(v, subs[i]));
        }

        invokeAll(tasks);
      }
    }

  }

  public static void visitReader(IndexReader reader, ReaderVisitor v) {
    visitReaders(Collections.singletonList(reader), v);
  }

  public static void visitReaders(IndexReader[] readers, ReaderVisitor v) {
    List<IndexReader> l = new ArrayList<IndexReader>(readers.length);
    Collections.addAll(l, readers);
    visitReaders(l, v);
  }

  @SuppressWarnings("serial")
  public static void visitReaders(Collection<IndexReader> readers, ReaderVisitor v) {
    if (readers.size() == 0) {
      return;
    } else if (readers.size() == 1) {
      v.visit(readers.iterator().next());
    } else {
      final List<IndexReader> subs = new ArrayList<IndexReader>(readers.size() * 2);

      for (IndexReader r: readers) {
        gatherSubReaders(subs, r);
      }

      final List<ReaderAction> tasks = new ArrayList<ReaderAction>(subs.size());

      for (IndexReader r: subs) {
        tasks.add(new ReaderAction(v, r));
      }

      fjPool.invoke(new RecursiveAction() {

        @Override
        protected void compute() {
          invokeAll(tasks);
        }
      });
    }
  }

  private static class DocFreqCollector implements ReaderVisitor {
    private final Term t;
    private final AtomicInteger sum = new AtomicInteger(0); 

    public DocFreqCollector(Term t) {
      super();
      this.t = t;
    }

    @Override
    public void visit(IndexReader r) {
      try {
        sum.addAndGet(r.docFreq(t));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public int docFreq() {
      return sum.get();
    }

  }

  public static int computeDocFreq(IndexReader reader, final Term t) throws IOException {
    DocFreqCollector c = new DocFreqCollector(t);
    visitReader(reader, c);
    return c.docFreq();
  }

  public static int computeDocFreq(IndexReader[] readers, final Term t) throws IOException {
    DocFreqCollector c = new DocFreqCollector(t);
    visitReaders(readers, c);
    return c.docFreq();
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
