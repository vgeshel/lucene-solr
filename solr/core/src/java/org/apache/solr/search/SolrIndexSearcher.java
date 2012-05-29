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

package org.apache.solr.search;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.OpenBitSet;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.request.UnInvertedField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SolrIndexSearcher adds schema awareness and caching functionality
 * over the lucene IndexSearcher.
 *
 * @version $Id$
 * @since solr 0.9
 */
public class SolrIndexSearcher extends IndexSearcher implements SolrInfoMBean {

  // These should *only* be used for debugging or monitoring purposes
  public static final AtomicLong numOpens = new AtomicLong();
  public static final AtomicLong numCloses = new AtomicLong();


  private static Logger log = LoggerFactory.getLogger(SolrIndexSearcher.class);
  private final SolrCore core;
  private final IndexSchema schema;
  private String indexDir;

  private final String name;
  private long openTime = System.currentTimeMillis();
  private long registerTime = 0;
  private long warmupTime = 0;
  private final SolrIndexReader reader;
  private final boolean closeReader;

  private final int queryResultWindowSize;
  private final int queryResultMaxDocsCached;
  private final boolean useFilterForSortedQuery;
  public final boolean enableLazyFieldLoading;

  private final boolean cachingEnabled;
  private final SolrCache<Query,DocSet> filterCache;
  private final SolrCache<QueryResultKey,DocList> queryResultCache;
  private final SolrCache<Integer,Document> documentCache;
  private final SolrCache<String,Object> fieldValueCache;

  private final LuceneQueryOptimizer optimizer;

  // map of generic caches - not synchronized since it's read-only after the constructor.
  private final HashMap<String, SolrCache> cacheMap;
  private static final HashMap<String, SolrCache> noGenericCaches=new HashMap<String,SolrCache>(0);

  // list of all caches associated with this searcher.
  private final SolrCache[] cacheList;
  private static final SolrCache[] noCaches = new SolrCache[0];

  private final Collection<String> fieldNames;
  private Collection<String> storedHighlightFieldNames;

  /** Creates a searcher searching the index in the named directory.
   * 
   * @deprecated use alternate constructor
   */
  @Deprecated
  public SolrIndexSearcher(SolrCore core, IndexSchema schema, String name, String path, boolean enableCache) throws IOException {
    this(core, schema,name, core.getIndexReaderFactory().newReader(core.getDirectoryFactory().open(path), false), true, enableCache);
  }

  /*
   * Creates a searcher searching the index in the provided directory. Note:
   * uses the main IndexReaderFactory for the specified SolrCore.
   * 
   * @see SolrCore#getMainIndexReaderFactory
   */
  public SolrIndexSearcher(SolrCore core, IndexSchema schema, String name,
      Directory directory, boolean enableCache) throws IOException {
    this(core, schema,name, core.getIndexReaderFactory().newReader(directory, false), true, enableCache);
  }

  /** Creates a searcher searching the index in the provided directory. */
  public SolrIndexSearcher(SolrCore core, IndexSchema schema, String name, Directory directory, boolean readOnly, boolean enableCache) throws IOException {
    this(core, schema,name, core.getIndexReaderFactory().newReader(directory, readOnly), true, enableCache);
  }

  /** Creates a searcher searching the provided index. */
  public SolrIndexSearcher(SolrCore core, IndexSchema schema, String name, IndexReader r, boolean enableCache) {
    this(core, schema,name,r, false, enableCache);
  }

  private static SolrIndexReader wrap(IndexReader r) {
    SolrIndexReader sir;
    // wrap the reader
    if (!(r instanceof SolrIndexReader)) {
      sir = new SolrIndexReader(r, null, 0);
      sir.associateInfo(null);
    } else {
      sir = (SolrIndexReader)r;
    }
    return sir;
  }

  public SolrIndexSearcher(SolrCore core, IndexSchema schema, String name, IndexReader r, boolean closeReader, boolean enableCache) {
    super(wrap(r));
    this.reader = (SolrIndexReader)super.getIndexReader();
    this.core = core;
    this.schema = schema;
    this.name = "Searcher@" + Integer.toHexString(hashCode()) + (name!=null ? " "+name : "");
    log.info("Opening " + this.name);

    SolrIndexReader.setSearcher(reader, this);

    if (r.directory() instanceof FSDirectory) {
      FSDirectory fsDirectory = (FSDirectory) r.directory();
      indexDir = fsDirectory.getDirectory().getAbsolutePath();
    }

    this.closeReader = closeReader;
    setSimilarity(schema.getSimilarity());

    SolrConfig solrConfig = core.getSolrConfig();
    queryResultWindowSize = solrConfig.queryResultWindowSize;
    queryResultMaxDocsCached = solrConfig.queryResultMaxDocsCached;
    useFilterForSortedQuery = solrConfig.useFilterForSortedQuery;
    enableLazyFieldLoading = solrConfig.enableLazyFieldLoading;

    cachingEnabled=enableCache;
    if (cachingEnabled) {
      ArrayList<SolrCache> clist = new ArrayList<SolrCache>();
      fieldValueCache = solrConfig.fieldValueCacheConfig==null ? null : solrConfig.fieldValueCacheConfig.newInstance();
      if (fieldValueCache!=null) clist.add(fieldValueCache);
      filterCache= solrConfig.filterCacheConfig==null ? null : solrConfig.filterCacheConfig.newInstance();
      if (filterCache!=null) clist.add(filterCache);
      queryResultCache = solrConfig.queryResultCacheConfig==null ? null : solrConfig.queryResultCacheConfig.newInstance();
      if (queryResultCache!=null) clist.add(queryResultCache);
      documentCache = solrConfig.documentCacheConfig==null ? null : solrConfig.documentCacheConfig.newInstance();
      if (documentCache!=null) clist.add(documentCache);

      if (solrConfig.userCacheConfigs == null) {
        cacheMap = noGenericCaches;
      } else {
        cacheMap = new HashMap<String,SolrCache>(solrConfig.userCacheConfigs.length);
        for (CacheConfig userCacheConfig : solrConfig.userCacheConfigs) {
          SolrCache cache = null;
          if (userCacheConfig != null) cache = userCacheConfig.newInstance();
          if (cache != null) {
            cacheMap.put(cache.name(), cache);
            clist.add(cache);
          }
        }
      }

      cacheList = clist.toArray(new SolrCache[clist.size()]);
    } else {
      filterCache=null;
      queryResultCache=null;
      documentCache=null;
      fieldValueCache=null;
      cacheMap = noGenericCaches;
      cacheList= noCaches;
    }
    optimizer = solrConfig.filtOptEnabled ? new LuceneQueryOptimizer(solrConfig.filtOptCacheSize,solrConfig.filtOptThreshold) : null;

    fieldNames = r.getFieldNames(IndexReader.FieldOption.ALL);

    // do this at the end since an exception in the constructor means we won't close    
    numOpens.incrementAndGet();

  }


  @Override
  public String toString() {
    return name;
  }

  public SolrCore getCore() {
    return core;
  }


  /** Register sub-objects such as caches
   */
  public void register() {
    // register self
    core.getInfoRegistry().put("searcher", this);
    core.getInfoRegistry().put(name, this);
    for (SolrCache cache : cacheList) {
      cache.setState(SolrCache.State.LIVE);
      core.getInfoRegistry().put(cache.name(), cache);
    }
    registerTime=System.currentTimeMillis();
  }

  /**
   * Free's resources associated with this searcher.
   *
   * In particular, the underlying reader and any cache's in use are closed.
   */
  @Override
  public void close() throws IOException {
    if (cachingEnabled) {
      StringBuilder sb = new StringBuilder();
      sb.append("Closing ").append(name);
      for (SolrCache cache : cacheList) {
        sb.append("\n\t");
        sb.append(cache);
      }
      log.info(sb.toString());
    } else {
      log.debug("Closing " + name);
    }
    core.getInfoRegistry().remove(name);

    // super.close();
    // can't use super.close() since it just calls reader.close() and that may only be called once
    // per reader (even if incRef() was previously called).
    if (closeReader) reader.decRef();

    for (SolrCache cache : cacheList) {
      cache.close();
    }

    // do this at the end so it only gets done if there are no exceptions
    numCloses.incrementAndGet();
  }

  /** Direct access to the IndexReader used by this searcher */
  public SolrIndexReader getReader() { return reader; }
  /** Direct access to the IndexSchema for use with this searcher */
  public IndexSchema getSchema() { return schema; }

  /**
   * Returns a collection of all field names the index reader knows about.
   */
  public Collection<String> getFieldNames() {
    return fieldNames;
  }

  /**
   * Returns a collection of the names of all stored fields which can be
   * highlighted the index reader knows about.
   */
  public Collection<String> getStoredHighlightFieldNames() {
    if (storedHighlightFieldNames == null) {
      storedHighlightFieldNames = new LinkedList<String>();
      for (String fieldName : fieldNames) {
        try {
          SchemaField field = schema.getField(fieldName);
          if (field.stored() &&
              ((field.getType() instanceof org.apache.solr.schema.TextField) ||
                  (field.getType() instanceof org.apache.solr.schema.StrField))) {
            storedHighlightFieldNames.add(fieldName);
          }
        } catch (RuntimeException e) { // getField() throws a SolrException, but it arrives as a RuntimeException
          log.warn("Field \"" + fieldName + "\" found in index, but not defined in schema.");
        }
      }
    }
    return storedHighlightFieldNames;
  }
  //
  // Set default regenerators on filter and query caches if they don't have any
  //
  public static void initRegenerators(SolrConfig solrConfig) {
    if (solrConfig.fieldValueCacheConfig != null && solrConfig.fieldValueCacheConfig.getRegenerator() == null) {
      solrConfig.fieldValueCacheConfig.setRegenerator(
          new CacheRegenerator() {
            public boolean regenerateItem(SolrIndexSearcher newSearcher, SolrCache newCache, SolrCache oldCache, Object oldKey, Object oldVal) throws IOException {
              if (oldVal instanceof UnInvertedField) {
                UnInvertedField.getUnInvertedField((String)oldKey, newSearcher);
              }
              return true;
            }
          }
          );
    }

    if (solrConfig.filterCacheConfig != null && solrConfig.filterCacheConfig.getRegenerator() == null) {
      solrConfig.filterCacheConfig.setRegenerator(
          new CacheRegenerator() {
            public boolean regenerateItem(SolrIndexSearcher newSearcher, SolrCache newCache, SolrCache oldCache, Object oldKey, Object oldVal) throws IOException {
              newSearcher.cacheDocSet((Query)oldKey, null, false);
              return true;
            }
          }
          );
    }

    if (solrConfig.queryResultCacheConfig != null && solrConfig.queryResultCacheConfig.getRegenerator() == null) {
      final int queryResultWindowSize = solrConfig.queryResultWindowSize;
      solrConfig.queryResultCacheConfig.setRegenerator(
          new CacheRegenerator() {
            public boolean regenerateItem(SolrIndexSearcher newSearcher, SolrCache newCache, SolrCache oldCache, Object oldKey, Object oldVal) throws IOException {
              QueryResultKey key = (QueryResultKey)oldKey;
              int nDocs=1;
              // request 1 doc and let caching round up to the next window size...
              // unless the window size is <=1, in which case we will pick
              // the minimum of the number of documents requested last time and
              // a reasonable number such as 40.
              // TODO: make more configurable later...

              if (queryResultWindowSize<=1) {
                DocList oldList = (DocList)oldVal;
                int oldnDocs = oldList.offset() + oldList.size();
                // 40 has factors of 2,4,5,10,20
                nDocs = Math.min(oldnDocs,40);
              }

              int flags=NO_CHECK_QCACHE | key.nc_flags;
              QueryCommand qc = new QueryCommand();
              qc.setQuery(key.query)
              .setFilterList(key.filters)
              .setSort(key.sort)
              .setLen(nDocs)
              .setSupersetMaxDoc(nDocs)
              .setFlags(flags);
              QueryResult qr = new QueryResult();
              newSearcher.getDocListC(qr,qc);
              return true;
            }
          }
          );
    }
  }

  public QueryResult search(QueryResult qr, QueryCommand cmd) throws IOException {
    getDocListC(qr,cmd);
    return qr;
  }

  //  public Hits search(Query query, Filter filter, Sort sort) throws IOException {
  //    // todo - when Solr starts accepting filters, need to
  //    // change this conditional check (filter!=null) and create a new filter
  //    // that ANDs them together if it already exists.
  //
  //    if (optimizer==null || filter!=null || !(query instanceof BooleanQuery)
  //    ) {
  //      return super.search(query,filter,sort);
  //    } else {
  //      Query[] newQuery = new Query[1];
  //      Filter[] newFilter = new Filter[1];
  //      optimizer.optimize((BooleanQuery)query, this, 0, newQuery, newFilter);
  //
  //      return super.search(newQuery[0], newFilter[0], sort);
  //    }
  //  }

  /**
   * @return the indexDir on which this searcher is opened
   * @see #SolrIndexSearcher(SolrCore, IndexSchema, String, Directory, boolean)
   */
  public String getIndexDir() {
    return indexDir;
  }

  /* ********************** Document retrieval *************************/

  /* Future optimizations (yonik)
   *
   * If no cache is present:
   *   - use NO_LOAD instead of LAZY_LOAD
   *   - use LOAD_AND_BREAK if a single field is begin retrieved
   */

  /**
   * FieldSelector which loads the specified fields, and load all other
   * field lazily.
   */
  static class SetNonLazyFieldSelector implements FieldSelector {
    private Set<String> fieldsToLoad;
    SetNonLazyFieldSelector(Set<String> toLoad) {
      fieldsToLoad = toLoad;
    }
    public FieldSelectorResult accept(String fieldName) { 
      if(fieldsToLoad.contains(fieldName))
        return FieldSelectorResult.LOAD; 
      else
        return FieldSelectorResult.LAZY_LOAD;
    }
  }

  /**
   * Retrieve the {@link Document} instance corresponding to the document id.
   */
  @Override
  public Document doc(int i) throws IOException {
    return doc(i, (Set<String>)null);
  }

  /** Retrieve a {@link Document} using a {@link org.apache.lucene.document.FieldSelector}
   * This method does not currently use the Solr document cache.
   * 
   * @see IndexReader#document(int, FieldSelector) */
  @Override
  public Document doc(int n, FieldSelector fieldSelector) throws IOException {
    return getIndexReader().document(n, fieldSelector);
  }

  /**
   * Retrieve the {@link Document} instance corresponding to the document id.
   *
   * Note: The document will have all fields accessable, but if a field
   * filter is provided, only the provided fields will be loaded (the 
   * remainder will be available lazily).
   */
  public Document doc(int i, Set<String> fields) throws IOException {

    Document d;
    if (documentCache != null) {
      d = documentCache.get(i);
      if (d!=null) return d;
    }

    if(!enableLazyFieldLoading || fields == null) {
      d = getIndexReader().document(i);
    } else {
      d = getIndexReader().document(i, 
          new SetNonLazyFieldSelector(fields));
    }

    if (documentCache != null) {
      documentCache.put(i, d);
    }

    return d;
  }

  /**
   * Takes a list of docs (the doc ids actually), and reads them into an array 
   * of Documents.
   */
  public void readDocs(Document[] docs, DocList ids) throws IOException {
    readDocs(docs, ids, null);
  }
  /**
   * Takes a list of docs (the doc ids actually) and a set of fields to load,
   * and reads them into an array of Documents.
   */
  public void readDocs(Document[] docs, DocList ids, Set<String> fields) throws IOException {
    DocIterator iter = ids.iterator();
    for (int i=0; i<docs.length; i++) {
      docs[i] = doc(iter.nextDoc(), fields);
    }
  }

  /* ********************** end document retrieval *************************/

  ////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////

  /** expert: internal API, subject to change */
  public SolrCache getFieldValueCache() {
    return fieldValueCache;
  }

  /** Returns a weighted sort according to this searcher */
  public Sort weightSort(Sort sort) throws IOException {
    if (sort == null) return null;
    SortField[] sorts = sort.getSort();

    boolean needsWeighting = false;
    for (SortField sf : sorts) {
      if (sf instanceof SolrSortField) {
        needsWeighting = true;
        break;
      }
    }
    if (!needsWeighting) return sort;

    SortField[] newSorts = sorts.clone();
    for (int i=0; i<newSorts.length; i++) {
      if (newSorts[i] instanceof SolrSortField) {
        newSorts[i] = ((SolrSortField)newSorts[i]).weight(this);
      }
    }

    return new Sort(newSorts);
  }


  /**
   * Returns the first document number containing the term <code>t</code>
   * Returns -1 if no document was found.
   * This method is primarily intended for clients that want to fetch
   * documents using a unique identifier."
   * @param t
   * @return the first document number containing the term
   */
  public int getFirstMatch(Term t) throws IOException {
    TermDocs tdocs = null;
    try {
      tdocs = reader.termDocs(t);
      if (!tdocs.next()) return -1;
      return tdocs.doc();
    } finally {
      if (tdocs!=null) tdocs.close();
    }
  }


  /**
   * Compute and cache the DocSet that matches a query.
   * The normal usage is expected to be cacheDocSet(myQuery, null,false)
   * meaning that Solr will determine if the Query warrants caching, and
   * if so, will compute the DocSet that matches the Query and cache it.
   * If the answer to the query is already cached, nothing further will be done.
   * <p>
   * If the optionalAnswer DocSet is provided, it should *not* be modified
   * after this call.
   *
   * @param query           the lucene query that will act as the key
   * @param optionalAnswer   the DocSet to be cached - if null, it will be computed.
   * @param mustCache        if true, a best effort will be made to cache this entry.
   *                         if false, heuristics may be used to determine if it should be cached.
   */
  public void cacheDocSet(Query query, DocSet optionalAnswer, boolean mustCache) throws IOException {
    // Even if the cache is null, still compute the DocSet as it may serve to warm the Lucene
    // or OS disk cache.
    if (optionalAnswer != null) {
      if (filterCache!=null) {
        filterCache.put(query,optionalAnswer);
      }
      return;
    }

    // Throw away the result, relying on the fact that getDocSet
    // will currently always cache what it found.  If getDocSet() starts
    // using heuristics about what to cache, and mustCache==true, (or if we
    // want this method to start using heuristics too) then
    // this needs to change.
    getDocSet(query);
  }

  /**
   * Returns the set of document ids matching a query.
   * This method is cache-aware and attempts to retrieve the answer from the cache if possible.
   * If the answer was not cached, it may have been inserted into the cache as a result of this call.
   * This method can handle negative queries.
   * <p>
   * The DocSet returned should <b>not</b> be modified.
   */
  public DocSet getDocSet(Query query) throws IOException {
    if (query instanceof ExtendedQuery) {
      ExtendedQuery eq = (ExtendedQuery)query;
      if (!eq.getCache()) {
        if (query instanceof WrappedQuery) {
          query = ((WrappedQuery)query).getWrappedQuery();
        }
        query = QueryUtils.makeQueryable(query);
        return getDocSetNC(query, null);
      }
    }

    // Get the absolute value (positive version) of this query.  If we
    // get back the same reference, we know it's positive.
    Query absQ = QueryUtils.getAbs(query);
    boolean positive = query==absQ;

    if (filterCache != null) {
      DocSet absAnswer = filterCache.get(absQ);
      if (absAnswer!=null) {
        if (positive) return absAnswer;
        else return getPositiveDocSet(matchAllDocsQuery).andNot(absAnswer);
      }
    }

    DocSet absAnswer = getDocSetNC(absQ, null);
    DocSet answer = positive ? absAnswer : getPositiveDocSet(matchAllDocsQuery).andNot(absAnswer);

    if (filterCache != null) {
      // cache negative queries as positive
      filterCache.put(absQ, absAnswer);
    }

    return answer;
  }

  // only handle positive (non negative) queries
  DocSet getPositiveDocSet(Query q) throws IOException {
    DocSet answer;
    if (filterCache != null) {
      answer = filterCache.get(q);
      if (answer!=null) return answer;
    }
    answer = getDocSetNC(q,null);
    if (filterCache != null) filterCache.put(
        q,answer);
    return answer;
  }

  // only handle positive (non negative) queries
  /** @lucene.internal */
  public DocSet getPositiveDocSet(Query q, TermDocsState tdState) throws IOException {
    DocSet answer;
    if (filterCache != null) {
      answer = filterCache.get(q);
      if (answer!=null) return answer;
    }
    answer = getDocSetNC(q, null, tdState);
    if (filterCache != null) filterCache.put(q,answer);
    return answer;
  }

  private static Query matchAllDocsQuery = new MatchAllDocsQuery();


  public static class ProcessedFilter {
    public DocSet answer;  // the answer, if non-null
    public Filter filter;
    public DelegatingCollector postFilter;
  }


  private static Comparator<Query> sortByCost = new Comparator<Query>() {
    public int compare(Query q1, Query q2) {
      return ((ExtendedQuery)q1).getCost() - ((ExtendedQuery)q2).getCost();
    }
  };


  /**
   * Returns the set of document ids matching all queries.
   * This method is cache-aware and attempts to retrieve the answer from the cache if possible.
   * If the answer was not cached, it may have been inserted into the cache as a result of this call.
   * This method can handle negative queries.
   * <p>
   * The DocSet returned should <b>not</b> be modified.
   */
  public DocSet getDocSet(List<Query> queries) throws IOException {
    ProcessedFilter pf = getProcessedFilter(null, queries);
    if (pf.answer != null) return pf.answer;


    DocSetCollector setCollector = new DocSetCollector(maxDoc()>>6, maxDoc());
    Collector collector = setCollector;
    if (pf.postFilter != null) {
      pf.postFilter.setLastDelegate(collector);
      collector = pf.postFilter;
    }

    final SolrIndexReader[] leaves = reader.getLeafReaders();
    final int offsets[] = reader.getLeafOffsets();

    for (int i=0; i<leaves.length; i++) {
      SolrIndexReader subReader = leaves[i];
      int baseDoc = offsets[i];
      DocIdSet idSet = null;
      if (pf.filter != null) {
        idSet = pf.filter.getDocIdSet(subReader);
        if (idSet == null) continue;
      }
      DocIdSetIterator idIter = null;
      if (idSet != null) {
        idIter = idSet.iterator();
        if (idIter == null) continue;
      }

      collector.setNextReader(subReader, baseDoc);
      int max = subReader.maxDoc();

      if (idIter == null) {
        TermDocs tdocs = subReader.termDocs(null);
        while (tdocs.next()) {
          int docid = tdocs.doc();
          collector.collect(docid);
        }
      } else {
        for (int docid = -1; (docid = idIter.advance(docid+1)) < max; ) {
          if (subReader.isDeleted(docid)) continue;
          collector.collect(docid);
        }
      }
    }

    return setCollector.getDocSet();
  }


  public ProcessedFilter getProcessedFilter(DocSet setFilter, List<Query> queries) throws IOException {
    ProcessedFilter pf = new ProcessedFilter();
    if (queries==null || queries.size()==0) {
      if (setFilter != null)
        pf.filter = setFilter.getTopFilter();
      return pf;
    }

    DocSet answer=null;

    boolean[] neg = new boolean[queries.size()+1];
    DocSet[] sets = new DocSet[queries.size()+1];
    List<Query> notCached = null;
    List<Query> postFilters = null;

    int end = 0;
    int smallestIndex = -1;

    if (setFilter != null) {
      answer = sets[end++] = setFilter;
      smallestIndex = end;
    }

    int smallestCount = Integer.MAX_VALUE;
    for (Query q : queries) {
      if (q instanceof ExtendedQuery) {
        ExtendedQuery eq = (ExtendedQuery)q;
        if (!eq.getCache()) {
          if (eq.getCost() >= 100 && eq instanceof PostFilter) {
            if (postFilters == null) postFilters = new ArrayList<Query>(sets.length-end);
            postFilters.add(q);
          } else {
            if (notCached == null) notCached = new ArrayList<Query>(sets.length-end);
            notCached.add(q);
          }
          continue;
        }
      }

      Query posQuery = QueryUtils.getAbs(q);
      sets[end] = getPositiveDocSet(posQuery);
      // Negative query if absolute value different from original
      if (q==posQuery) {
        neg[end] = false;
        // keep track of the smallest positive set.
        // This optimization is only worth it if size() is cached, which it would
        // be if we don't do any set operations.
        int sz = sets[end].size();
        if (sz<smallestCount) {
          smallestCount=sz;
          smallestIndex=end;
          answer = sets[end];
        }
      } else {
        neg[end] = true;
      }

      end++;
    }

    // Are all of our normal cached filters negative?
    if (end > 0 && answer==null) {
      answer = getPositiveDocSet(matchAllDocsQuery);
    }

    // do negative queries first to shrink set size
    for (int i=0; i<end; i++) {
      if (neg[i]) answer = answer.andNot(sets[i]);
    }

    for (int i=0; i<end; i++) {
      if (!neg[i] && i!=smallestIndex) answer = answer.intersection(sets[i]);
    }

    if (notCached != null) {
      Collections.sort(notCached, sortByCost);
      List<Weight> weights = new ArrayList<Weight>(notCached.size());
      for (Query q : notCached) {
        Query qq = QueryUtils.makeQueryable(q);
        weights.add(createNormalizedWeight(qq));
      }
      pf.filter = new FilterImpl(answer, weights);
    } else {
      if (postFilters == null) {
        if (answer == null) {
          answer = getPositiveDocSet(matchAllDocsQuery);
        }
        // "answer" is the only part of the filter, so set it.
        pf.answer = answer;
      }

      if (answer != null) {
        pf.filter = answer.getTopFilter();
      }
    }

    if (postFilters != null) {
      Collections.sort(postFilters, sortByCost);
      for (int i=postFilters.size()-1; i>=0; i--) {
        DelegatingCollector prev = pf.postFilter;
        pf.postFilter = ((PostFilter)postFilters.get(i)).getFilterCollector(this);
        if (prev != null) pf.postFilter.setDelegate(prev);
      }
    }

    return pf;
  }

  // query must be positive
  protected DocSet getDocSetNC(Query query, DocSet filter) throws IOException {
    DocSetCollector collector = new DocSetCollector(maxDoc()>>6, maxDoc());

    if (filter==null) {
      if (query instanceof TermQuery) {
        Term t = ((TermQuery)query).getTerm();
        SolrIndexReader[] readers = reader.getLeafReaders();
        int[] offsets = reader.getLeafOffsets();
        int[] arr = new int[256];
        int[] freq = new int[256];
        for (int i=0; i<readers.length; i++) {
          SolrIndexReader sir = readers[i];
          int offset = offsets[i];
          collector.setNextReader(sir, offset);
          TermDocs tdocs = sir.termDocs(t);
          for(;;) {
            int num = tdocs.read(arr, freq);
            if (num==0) break;
            for (int j=0; j<num; j++) {
              collector.collect(arr[j]);
            }
          }
          tdocs.close();
        }
      } else {
        super.search(query,null,collector);
      }
      return collector.getDocSet();

    } else {
      Filter luceneFilter = filter.getTopFilter();
      super.search(query, luceneFilter, collector);
      return collector.getDocSet();
    }
  }

  /** @lucene.internal */
  public static class TermDocsState {
    public TermEnum tenum;
    public TermDocs tdocs;
  }

  // query must be positive
  protected DocSet getDocSetNC(Query query, DocSet filter, TermDocsState tdState) throws IOException {
    int smallSetSize = maxDoc()>>6;
            int largestPossible = tdState.tenum.docFreq();
            int[] docs = new int[Math.min(smallSetSize, largestPossible)];
            OpenBitSet obs = null;
            int upto=0;
            int numBits = 0;

            if (tdState.tdocs == null) {
              tdState.tdocs = reader.termDocs();
            }

            tdState.tdocs.seek(tdState.tenum);

            int[] arr = new int[Math.min(largestPossible, 256)];
            int[] freq = new int[arr.length];

            for(;;) {
              int num = tdState.tdocs.read(arr, freq);
              if (num==0) break;
              if (upto + num > docs.length) {
                if (obs == null) obs = new OpenBitSet(maxDoc());
                for (int i = 0; i<num; i++) {
                  obs.fastSet(arr[i]);
                }
                numBits += num;
              } else {
                System.arraycopy(arr, 0, docs, upto, num);
                upto += num;
              }
            }

            if (obs != null) {
              for (int i=0; i<upto; i++) {
                obs.fastSet(docs[i]);
              }
              numBits += upto;
              return new BitDocSet(obs, numBits);
            }

            return new SortedIntDocSet(docs, upto);
  }


  /**
   * Returns the set of document ids matching both the query and the filter.
   * This method is cache-aware and attempts to retrieve the answer from the cache if possible.
   * If the answer was not cached, it may have been inserted into the cache as a result of this call.
   * <p>
   *
   * @param query
   * @param filter may be null
   * @return DocSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   */
  public DocSet getDocSet(Query query, DocSet filter) throws IOException {
    if (filter==null) return getDocSet(query);

    if (query instanceof ExtendedQuery) {
      ExtendedQuery eq = (ExtendedQuery)query;
      if (!eq.getCache()) {
        if (query instanceof WrappedQuery) {
          query = ((WrappedQuery)query).getWrappedQuery();
        }
        query = QueryUtils.makeQueryable(query);
        return getDocSetNC(query, filter);
      }
    }

    // Negative query if absolute value different from original
    Query absQ = QueryUtils.getAbs(query);
    boolean positive = absQ==query;

    DocSet first;
    if (filterCache != null) {
      first = filterCache.get(absQ);
      if (first==null) {
        first = getDocSetNC(absQ,null);
        filterCache.put(absQ,first);
      }
      return positive ? first.intersection(filter) : filter.andNot(first);
    }

    // If there isn't a cache, then do a single filtered query if positive.
    return positive ? getDocSetNC(absQ,filter) : filter.andNot(getPositiveDocSet(absQ));
  }


  /**
   * Converts a filter into a DocSet.
   * This method is not cache-aware and no caches are checked.
   */
  public DocSet convertFilter(Filter lfilter) throws IOException {
    DocIdSet docSet = lfilter.getDocIdSet(this.reader);
    OpenBitSet obs = new OpenBitSet();
    DocIdSetIterator it = docSet.iterator();
    int doc;
    while((doc = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      obs.fastSet(doc);
    }
    return new BitDocSet(obs);
  }

  /**
   * Returns documents matching both <code>query</code> and <code>filter</code>
   * and sorted by <code>sort</code>.
   * <p>
   * This method is cache aware and may retrieve <code>filter</code> from
   * the cache or make an insertion into the cache as a result of this call.
   * <p>
   * FUTURE: The returned DocList may be retrieved from a cache.
   *
   * @param query
   * @param filter   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @return DocList meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException
   */
  public DocList getDocList(Query query, Query filter, Sort lsort, int offset, int len) throws IOException {
    QueryCommand qc = new QueryCommand();
    qc.setQuery(query)
    .setFilterList(filter)
    .setSort(lsort)
    .setOffset(offset)
    .setLen(len);
    QueryResult qr = new QueryResult();
    search(qr,qc);
    return qr.getDocList();
  }


  /**
   * Returns documents matching both <code>query</code> and the 
   * intersection of the <code>filterList</code>, sorted by <code>sort</code>.
   * <p>
   * This method is cache aware and may retrieve <code>filter</code> from
   * the cache or make an insertion into the cache as a result of this call.
   * <p>
   * FUTURE: The returned DocList may be retrieved from a cache.
   *
   * @param query
   * @param filterList may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @return DocList meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException
   */
  public DocList getDocList(Query query, List<Query> filterList, Sort lsort, int offset, int len, int flags) throws IOException {
    QueryCommand qc = new QueryCommand();
    qc.setQuery(query)
    .setFilterList(filterList)
    .setSort(lsort)
    .setOffset(offset)
    .setLen(len)
    .setFlags(flags);
    QueryResult qr = new QueryResult();
    search(qr,qc);
    return qr.getDocList();
  }

  private static final int NO_CHECK_QCACHE       = 0x80000000;
  public static final int GET_DOCSET             = 0x40000000;
  private static final int NO_CHECK_FILTERCACHE  = 0x20000000;
  private static final int NO_SET_QCACHE         = 0x10000000;

  public static final int GET_DOCLIST           =        0x02;
  public static final int GET_SCORES             =       0x01;

  /**
   * getDocList version that uses+populates query and filter caches.
   * In the event of a timeout, the cache is not populated.
   */
  private void getDocListC(QueryResult qr, QueryCommand cmd) throws IOException {
    // old parameters: DocListAndSet out, Query query, List<Query> filterList, DocSet filter, Sort lsort, int offset, int len, int flags, long timeAllowed, NamedList<Object> responseHeader
    DocListAndSet out = new DocListAndSet();
    qr.setDocListAndSet(out);
    QueryResultKey key=null;
    int maxDocRequested = cmd.getOffset() + cmd.getLen();
    // check for overflow, and check for # docs in index
    if (maxDocRequested < 0 || maxDocRequested > maxDoc()) maxDocRequested = maxDoc();
    int supersetMaxDoc= maxDocRequested;
    DocList superset = null;

    int flags = cmd.getFlags();
    Query q = cmd.getQuery();
    if (q instanceof ExtendedQuery) {
      ExtendedQuery eq = (ExtendedQuery)q;
      if (!eq.getCache()) {
        flags |= (NO_CHECK_QCACHE | NO_SET_QCACHE | NO_CHECK_FILTERCACHE);
      }
    }


    // we can try and look up the complete query in the cache.
    // we can't do that if filter!=null though (we don't want to
    // do hashCode() and equals() for a big DocSet).
    if (queryResultCache != null && cmd.getFilter()==null
        && (flags & (NO_CHECK_QCACHE|NO_SET_QCACHE)) != ((NO_CHECK_QCACHE|NO_SET_QCACHE)))
    {
      // all of the current flags can be reused during warming,
      // so set all of them on the cache key.
      key = new QueryResultKey(q, cmd.getFilterList(), cmd.getSort(), flags);
      if ((flags & NO_CHECK_QCACHE)==0) {
        superset = queryResultCache.get(key);

        if (superset != null) {
          // check that the cache entry has scores recorded if we need them
          if ((flags & GET_SCORES)==0 || superset.hasScores()) {
            // NOTE: subset() returns null if the DocList has fewer docs than
            // requested
            out.docList = superset.subset(cmd.getOffset(),cmd.getLen());
          }
        }
        if (out.docList != null) {
          // found the docList in the cache... now check if we need the docset too.
          // OPT: possible future optimization - if the doclist contains all the matches,
          // use it to make the docset instead of rerunning the query.
          if (out.docSet==null && ((flags & GET_DOCSET)!=0) ) {
            if (cmd.getFilterList()==null) {
              out.docSet = getDocSet(cmd.getQuery());
            } else {
              List<Query> newList = new ArrayList<Query>(cmd.getFilterList().size()+1);
              newList.add(cmd.getQuery());
              newList.addAll(cmd.getFilterList());
              out.docSet = getDocSet(newList);
            }
          }
          return;
        }
      }

      // If we are going to generate the result, bump up to the
      // next resultWindowSize for better caching.

      if ((flags & NO_SET_QCACHE) == 0) {
        // handle 0 special case as well as avoid idiv in the common case.
        if (maxDocRequested < queryResultWindowSize) {
          supersetMaxDoc=queryResultWindowSize;
        } else {
          supersetMaxDoc = ((maxDocRequested -1)/queryResultWindowSize + 1)*queryResultWindowSize;
          if (supersetMaxDoc < 0) supersetMaxDoc=maxDocRequested;
        }
      } else {
        key = null;  // we won't be caching the result
      }
    }


    // OK, so now we need to generate an answer.
    // One way to do that would be to check if we have an unordered list
    // of results for the base query.  If so, we can apply the filters and then
    // sort by the resulting set.  This can only be used if:
    // - the sort doesn't contain score
    // - we don't want score returned.

    // check if we should try and use the filter cache
    boolean useFilterCache=false;
    if ((flags & (GET_SCORES|NO_CHECK_FILTERCACHE))==0 && useFilterForSortedQuery && cmd.getSort() != null && filterCache != null) {
      useFilterCache=true;
      SortField[] sfields = cmd.getSort().getSort();
      for (SortField sf : sfields) {
        if (sf.getType() == SortField.SCORE) {
          useFilterCache=false;
          break;
        }
      }
    }

    // disable useFilterCache optimization temporarily
    if (useFilterCache) {
      // now actually use the filter cache.
      // for large filters that match few documents, this may be
      // slower than simply re-executing the query.
      if (out.docSet == null) {
        out.docSet = getDocSet(cmd.getQuery(),cmd.getFilter());
        DocSet bigFilt = getDocSet(cmd.getFilterList());
        if (bigFilt != null) out.docSet = out.docSet.intersection(bigFilt);
      }
      // todo: there could be a sortDocSet that could take a list of
      // the filters instead of anding them first...
      // perhaps there should be a multi-docset-iterator
      superset = sortDocSet(out.docSet,cmd.getSort(),supersetMaxDoc);
      out.docList = superset.subset(cmd.getOffset(),cmd.getLen());
    } else {
      // do it the normal way...
      cmd.setSupersetMaxDoc(supersetMaxDoc);
      if ((flags & GET_DOCSET)!=0) {
        // this currently conflates returning the docset for the base query vs
        // the base query and all filters.
        DocSet qDocSet = getDocListAndSetNC(qr,cmd);
        // cache the docSet matching the query w/o filtering
        if (qDocSet!=null && filterCache!=null && !qr.isPartialResults()) filterCache.put(cmd.getQuery(),qDocSet);
      } else {
        getDocListNC(qr,cmd);
        //Parameters: cmd.getQuery(),theFilt,cmd.getSort(),0,supersetMaxDoc,cmd.getFlags(),cmd.getTimeAllowed(),responseHeader);
      }

      superset = out.docList;
      out.docList = superset.subset(cmd.getOffset(),cmd.getLen());
    }

    // lastly, put the superset in the cache if the size is less than or equal
    // to queryResultMaxDocsCached
    if (key != null && superset.size() <= queryResultMaxDocsCached && !qr.isPartialResults()) {
      queryResultCache.put(key, superset);
    }
  }

  private static class AtomicFloat {
    private AtomicInteger i;

    public AtomicFloat(float f) {
      i = new AtomicInteger(Float.floatToIntBits(f));
    }

    public void setIfGreater(final float f) {
      boolean done  = false;
      final int newI = Float.floatToIntBits(f);

      while (! done) {
        final int currentI = i.get();
        final float currentF = Float.intBitsToFloat(currentI);

        if (f > currentF) {
          done = i.compareAndSet(currentI, newI);
        } else {
          done = true;
        }
      }
    }

    public float get() {
      return Float.intBitsToFloat(i.get());
    }
  }

  private void getDocListNC(QueryResult qr,final QueryCommand cmd) throws IOException {
    final long timeAllowed = cmd.getTimeAllowed();
    final int len = cmd.getSupersetMaxDoc();
    int last = len;
    if (last < 0 || last > maxDoc()) last=maxDoc();
    final int lastDocRequested = last;
    int nDocsReturned = 0;
    int totalHits = 0;
    float maxScore = Float.NEGATIVE_INFINITY;
    int[] ids;
    float[] scores;

    final boolean needScores = (cmd.getFlags() & GET_SCORES) != 0;

    Query query = QueryUtils.makeQueryable(cmd.getQuery());

    final ProcessedFilter pf = getProcessedFilter(cmd.getFilter(), cmd.getFilterList());
    final Filter luceneFilter = pf.filter;

    log.debug("running query {} ({}) with filter {} in parallel over {} readers", 
        new Object[]{query, cmd.getQuery(), luceneFilter, getSubReaders().length});

    // must create normalized weight using the top-level searcher
    Weight weight = createNormalizedWeight(query);
    final Sort sort = weightSort(cmd.getSort());

    // handle zero case...
    if (lastDocRequested<=0) {
      // TODO convert this branch to fjPool too
      final AtomicFloat topscore = new AtomicFloat(Float.NEGATIVE_INFINITY);
      final AtomicInteger numHits = new AtomicInteger(0);

      final Callable<Collector> collectorFactory = new Callable<Collector>() {

        @Override
        public Collector call() throws Exception {
          Collector collector;

          if (!needScores) {
            collector = new Collector () {
              @Override
              public void setScorer(Scorer scorer) throws IOException {
              }
              @Override
              public void collect(int doc) throws IOException {
                numHits.incrementAndGet();
              }
              @Override
              public void setNextReader(IndexReader reader, int docBase) throws IOException {
              }
              @Override
              public boolean acceptsDocsOutOfOrder() {
                return true;
              }
            };
          } else {
            collector = new Collector() {
              Scorer scorer;
              @Override
              public void setScorer(Scorer scorer) throws IOException {
                this.scorer = scorer;
              }
              @Override
              public void collect(int doc) throws IOException {
                numHits.incrementAndGet();
                float score = scorer.score();
                topscore.setIfGreater(score);            
              }
              @Override
              public void setNextReader(IndexReader reader, int docBase) throws IOException {
              }
              @Override
              public boolean acceptsDocsOutOfOrder() {
                return true;
              }
            };
          }

          if( timeAllowed > 0 ) {
            collector = new TimeLimitingCollector(collector, TimeLimitingCollector.getGlobalCounter(), timeAllowed);
          }
          if (pf.postFilter != null) {
            // we have to create a separate post filter here because it's mutable  
            final ProcessedFilter localpf = getProcessedFilter(cmd.getFilter(), cmd.getFilterList());
            localpf.postFilter.setLastDelegate(collector);
            collector = localpf.postFilter;
          }

          return collector;
        }
      };

      try {
        super.searchParallel(weight, luceneFilter, collectorFactory);
      }
      catch( TimeLimitingCollector.TimeExceededException x ) {
        log.warn( "Query: " + query + "; " + x.getMessage() );
        qr.setPartialResults(true);
      }

      nDocsReturned=0;
      ids = new int[nDocsReturned];
      scores = new float[nDocsReturned];
      totalHits = numHits.get();
      maxScore = totalHits>0 ? topscore.get() : 0.0f;
    } else {
      final ConcurrentLinkedQueue<TopDocsCollector> tdCollectors = new ConcurrentLinkedQueue<TopDocsCollector>();

      Callable<Collector> collectorFactory = new Callable<Collector>() {

        @Override
        public Collector call() throws Exception {
          TopDocsCollector topCollector;
          if (cmd.getSort() == null) {
            topCollector = TopScoreDocCollector.create(len, true);
          } else {
            topCollector = TopFieldCollector.create(sort, len, true, needScores, needScores, true);
          }
          Collector collector = topCollector;
          if( timeAllowed > 0 ) {
            collector = new TimeLimitingCollector(collector, TimeLimitingCollector.getGlobalCounter(), timeAllowed);
          }
          if (pf.postFilter != null) {
            // we have to create a separate post filter here because it's mutable  
            final ProcessedFilter localpf = getProcessedFilter(cmd.getFilter(), cmd.getFilterList());
            localpf.postFilter.setLastDelegate(collector);
            collector = localpf.postFilter;
          }

          tdCollectors.add(topCollector);
          return collector;
        }
      };

      try {
        // use a subsearcher here instead of super
        super.searchParallel(weight, luceneFilter, collectorFactory);
      }
      catch( TimeLimitingCollector.TimeExceededException x ) {
        log.warn( "Query: " + query + "; " + x.getMessage() );
        qr.setPartialResults(true);
      }

      // invokeAll tasks and wait for up to timeAllowed

      // aggregate results. use TopDocs.merge
      assert tdCollectors.size() == getSubReaders().length;

      TopDocs[] topDocsArr = new TopDocs[tdCollectors.size()];

      for (int i = 0; ! tdCollectors.isEmpty(); i ++) {
        TopDocsCollector topCollector = tdCollectors.remove();
        totalHits += topCollector.getTotalHits();
        TopDocs topDocs = topCollector.topDocs(0, len);
        topDocsArr[i] = topDocs;

        if (topCollector.getTotalHits() > 0) {
          maxScore = Math.max(maxScore, topDocs.getMaxScore());
        }

        log.debug("collector {} of {} found docs: {}",
            new Object[]{i, getSubReaders().length, topDocs.scoreDocs});
      }

      TopDocs topDocs = TopDocs.merge(sort, len, topDocsArr);

      nDocsReturned = topDocs.scoreDocs.length;

      ids = new int[nDocsReturned];
      scores = (cmd.getFlags()&GET_SCORES)!=0 ? new float[nDocsReturned] : null;
      for (int i=0; i<nDocsReturned; i++) {
        ScoreDoc scoreDoc = topDocs.scoreDocs[i];
        ids[i] = scoreDoc.doc;
        if (scores != null) scores[i] = scoreDoc.score;
      }
    }


    int sliceLen = Math.min(lastDocRequested,nDocsReturned);
    if (sliceLen < 0) sliceLen=0;
    qr.setDocList(new DocSlice(0,sliceLen,ids,scores,totalHits,maxScore));
  }

  // any DocSet returned is for the query only, without any filtering... that way it may
  // be cached if desired.
  private DocSet getDocListAndSetNC(QueryResult qr,final QueryCommand cmd) throws IOException {
    final int len = cmd.getSupersetMaxDoc();
    int last = len;
    if (last < 0 || last > maxDoc()) last=maxDoc();
    final int lastDocRequested = last;
    int nDocsReturned;
    int totalHits = 0;
    float maxScore = Float.NEGATIVE_INFINITY;
    int[] ids;
    float[] scores;
    final DocSet set;

    final boolean needScores = (cmd.getFlags() & GET_SCORES) != 0;
    final int maxDoc = maxDoc();
    final int smallSetSize = maxDoc>>6;

    final ProcessedFilter pf = getProcessedFilter(cmd.getFilter(), cmd.getFilterList());
    final Filter luceneFilter = pf.filter;

    Query query = QueryUtils.makeQueryable(cmd.getQuery());
    final long timeAllowed = cmd.getTimeAllowed();

    log.debug("running query {} ({}) with filter {} in parallel over {} readers", 
        new Object[]{query, cmd.getQuery(), luceneFilter, getSubReaders().length});

    // must create normalized weight using the top-level searcher
    Weight weight = createNormalizedWeight(query);
    final Sort sort = weightSort(cmd.getSort());

    // handle zero case...
    if (lastDocRequested<=0) {
      final AtomicFloat topscore = new AtomicFloat(Float.NEGATIVE_INFINITY);
      final AtomicReference<DocSetCollector> setCollectorRef = new AtomicReference<DocSetCollector>(null);

      final Callable<Collector> collectorFactory = new Callable<Collector>() {

        @Override
        public Collector call() throws Exception {
          Collector collector;
          DocSetCollector setCollector;

          if (!needScores) {
            collector = setCollector = new DocSetCollector(smallSetSize, maxDoc);
          } else {
            collector = setCollector = new DocSetDelegateCollector(smallSetSize, maxDoc, new Collector() {
              Scorer scorer;
              @Override
              public void setScorer(Scorer scorer) throws IOException {
                this.scorer = scorer;
              }
              @Override
              public void collect(int doc) throws IOException {
                float score = scorer.score();
                topscore.setIfGreater(score);
              }
              @Override
              public void setNextReader(IndexReader reader, int docBase) throws IOException {
              }
              @Override
              public boolean acceptsDocsOutOfOrder() {
                return false;
              }
            });

            if( timeAllowed > 0 ) {
              collector = new TimeLimitingCollector(collector, TimeLimitingCollector.getGlobalCounter(), timeAllowed);
            }
            if (pf.postFilter != null) {
              // we have to create a separate post filter here because it's mutable  
              final ProcessedFilter localpf = getProcessedFilter(cmd.getFilter(), cmd.getFilterList());
              localpf.postFilter.setLastDelegate(collector);
              collector = localpf.postFilter;
            }
          }

          setCollectorRef.set(setCollector);

          return collector;
        }
      };

      try {
        super.searchParallel(weight, luceneFilter, collectorFactory);
      }
      catch( TimeLimitingCollector.TimeExceededException x ) {
        log.warn( "Query: " + query + "; " + x.getMessage() );
        qr.setPartialResults(true);
      }

      set = setCollectorRef.get().getDocSet();

      nDocsReturned = 0;
      ids = new int[nDocsReturned];
      scores = new float[nDocsReturned];
      totalHits = set.size();
      maxScore = totalHits>0 ? topscore.get() : 0.0f;
    } else {
      final ConcurrentLinkedQueue<TopDocsCollector> tdCollectors = new ConcurrentLinkedQueue<TopDocsCollector>();
      set = new BitDocSet();
      final Lock setCollectorLock = new ReentrantLock();

      Callable<Collector> collectorFactory = new Callable<Collector>() {

        @Override
        public Collector call() throws Exception {
          TopDocsCollector topCollector;

          if (cmd.getSort() == null) {
            topCollector = TopScoreDocCollector.create(len, true);
          } else {
            topCollector = TopFieldCollector.create(weightSort(cmd.getSort()), len, false, needScores, needScores, true);
          }
          
          tdCollectors.add(topCollector);

          Collector collector = topCollector;

          if( timeAllowed > 0 ) {
            collector = new TimeLimitingCollector(collector, TimeLimitingCollector.getGlobalCounter(), timeAllowed );
          }

          final Collector finalCollector = collector;
          
          // hook in a collector to update the docset. this has to be after the postfilter (see below), otherwise we'll get false positives
          collector = new Collector() {
            IndexReader reader;
            int docBase;
            
            @Override
            public void setScorer(Scorer scorer) throws IOException {
              finalCollector.setScorer(scorer);
            }
            
            @Override
            public void setNextReader(IndexReader reader, int docBase) throws IOException {
              this.reader = reader;
              this.docBase = docBase;
              
              finalCollector.setNextReader(reader, docBase);
            }
            
            @Override
            public void collect(int doc) throws IOException {
              finalCollector.collect(doc);
              
              setCollectorLock.lock();
              
              try {
                set.add(docBase + doc);
              } finally {
                setCollectorLock.unlock();
              }
            }
            
            @Override
            public boolean acceptsDocsOutOfOrder() {
              return finalCollector.acceptsDocsOutOfOrder();
            }
          };

          if (pf.postFilter != null) {
            // we have to create a separate post filter here because it's mutable  
            final ProcessedFilter localpf = getProcessedFilter(cmd.getFilter(), cmd.getFilterList());
            localpf.postFilter.setLastDelegate(collector);
            collector = localpf.postFilter;
          }

          return collector;
        }
      };
      
      try {
        super.searchParallel(weight, luceneFilter, collectorFactory);
      }
      catch( TimeLimitingCollector.TimeExceededException x ) {
        log.warn( "Query: " + query + "; " + x.getMessage() );
        qr.setPartialResults(true);
      }

      // aggregate results. use TopDocs.merge
      assert tdCollectors.size() == getSubReaders().length;

      TopDocs[] topDocsArr = new TopDocs[tdCollectors.size()];

      for (int i = 0; ! tdCollectors.isEmpty(); i ++) {
        TopDocsCollector topCollector = tdCollectors.remove();
        totalHits += topCollector.getTotalHits();
        TopDocs topDocs = topCollector.topDocs(0, len);
        topDocsArr[i] = topDocs;

        if (topCollector.getTotalHits() > 0) {
          maxScore = Math.max(maxScore, topDocs.getMaxScore());
        }

        log.debug("collector {} of {} found docs: {}",
            new Object[]{i, getSubReaders().length, topDocs.scoreDocs});

      }

      TopDocs topDocs = TopDocs.merge(sort, len, topDocsArr);

      nDocsReturned = topDocs.scoreDocs.length;

      assert(totalHits == set.size());
      
      nDocsReturned = topDocs.scoreDocs.length;

      ids = new int[nDocsReturned];
      scores = (cmd.getFlags()&GET_SCORES)!=0 ? new float[nDocsReturned] : null;
      for (int i=0; i<nDocsReturned; i++) {
        ScoreDoc scoreDoc = topDocs.scoreDocs[i];
        ids[i] = scoreDoc.doc;
        if (scores != null) scores[i] = scoreDoc.score;
      }
    }

    int sliceLen = Math.min(lastDocRequested,nDocsReturned);
    if (sliceLen < 0) sliceLen=0;

    qr.setDocList(new DocSlice(0,sliceLen,ids,scores,totalHits,maxScore));
    // TODO: if we collect results before the filter, we just need to intersect with
    // that filter to generate the DocSet for qr.setDocSet()
    qr.setDocSet(set);

    // TODO: currently we don't generate the DocSet for the base query,
    // but the QueryDocSet == CompleteDocSet if filter==null.
    return pf.filter==null && pf.postFilter==null ? qr.getDocSet() : null;
  }


  /**
   * Returns documents matching both <code>query</code> and <code>filter</code>
   * and sorted by <code>sort</code>.
   * FUTURE: The returned DocList may be retrieved from a cache.
   *
   * @param query
   * @param filter   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @return DocList meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException
   */
  public DocList getDocList(Query query, DocSet filter, Sort lsort, int offset, int len) throws IOException {
    QueryCommand qc = new QueryCommand();
    qc.setQuery(query)
    .setFilter(filter)
    .setSort(lsort)
    .setOffset(offset)
    .setLen(len);
    QueryResult qr = new QueryResult();
    search(qr,qc);
    return qr.getDocList();
  }

  /**
   * Returns documents matching both <code>query</code> and <code>filter</code>
   * and sorted by <code>sort</code>.  Also returns the complete set of documents
   * matching <code>query</code> and <code>filter</code> (regardless of <code>offset</code> and <code>len</code>).
   * <p>
   * This method is cache aware and may retrieve <code>filter</code> from
   * the cache or make an insertion into the cache as a result of this call.
   * <p>
   * FUTURE: The returned DocList may be retrieved from a cache.
   * <p>
   * The DocList and DocSet returned should <b>not</b> be modified.
   *
   * @param query
   * @param filter   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException
   */
  public DocListAndSet getDocListAndSet(Query query, Query filter, Sort lsort, int offset, int len) throws IOException {
    QueryCommand qc = new QueryCommand();
    qc.setQuery(query)
    .setFilterList(filter)
    .setSort(lsort)
    .setOffset(offset)
    .setLen(len)
    .setNeedDocSet(true);
    QueryResult qr = new QueryResult();
    search(qr,qc);
    return qr.getDocListAndSet();
  }

  /**
   * Returns documents matching both <code>query</code> and <code>filter</code>
   * and sorted by <code>sort</code>.  Also returns the compete set of documents
   * matching <code>query</code> and <code>filter</code> (regardless of <code>offset</code> and <code>len</code>).
   * <p>
   * This method is cache aware and may retrieve <code>filter</code> from
   * the cache or make an insertion into the cache as a result of this call.
   * <p>
   * FUTURE: The returned DocList may be retrieved from a cache.
   * <p>
   * The DocList and DocSet returned should <b>not</b> be modified.
   *
   * @param query
   * @param filter   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @param flags    user supplied flags for the result set
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException
   */
  public DocListAndSet getDocListAndSet(Query query, Query filter, Sort lsort, int offset, int len, int flags) throws IOException {
    QueryCommand qc = new QueryCommand();
    qc.setQuery(query)
    .setFilterList(filter)
    .setSort(lsort)
    .setOffset(offset)
    .setLen(len)
    .setFlags(flags)
    .setNeedDocSet(true);
    QueryResult qr = new QueryResult();
    search(qr,qc);
    return qr.getDocListAndSet();
  }


  /**
   * Returns documents matching both <code>query</code> and the intersection 
   * of <code>filterList</code>, sorted by <code>sort</code>.  
   * Also returns the compete set of documents
   * matching <code>query</code> and <code>filter</code> 
   * (regardless of <code>offset</code> and <code>len</code>).
   * <p>
   * This method is cache aware and may retrieve <code>filter</code> from
   * the cache or make an insertion into the cache as a result of this call.
   * <p>
   * FUTURE: The returned DocList may be retrieved from a cache.
   * <p>
   * The DocList and DocSet returned should <b>not</b> be modified.
   *
   * @param query
   * @param filterList   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException
   */
  public DocListAndSet getDocListAndSet(Query query, List<Query> filterList, Sort lsort, int offset, int len) throws IOException {
    QueryCommand qc = new QueryCommand();
    qc.setQuery(query)
    .setFilterList(filterList)
    .setSort(lsort)
    .setOffset(offset)
    .setLen(len)
    .setNeedDocSet(true);
    QueryResult qr = new QueryResult();
    search(qr,qc);
    return qr.getDocListAndSet();
  }

  /**
   * Returns documents matching both <code>query</code> and the intersection 
   * of <code>filterList</code>, sorted by <code>sort</code>.  
   * Also returns the compete set of documents
   * matching <code>query</code> and <code>filter</code> 
   * (regardless of <code>offset</code> and <code>len</code>).
   * <p>
   * This method is cache aware and may retrieve <code>filter</code> from
   * the cache or make an insertion into the cache as a result of this call.
   * <p>
   * FUTURE: The returned DocList may be retrieved from a cache.
   * <p>
   * The DocList and DocSet returned should <b>not</b> be modified.
   *
   * @param query
   * @param filterList   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @param flags    user supplied flags for the result set
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException
   */
  public DocListAndSet getDocListAndSet(Query query, List<Query> filterList, Sort lsort, int offset, int len, int flags) throws IOException {
    QueryCommand qc = new QueryCommand();
    qc.setQuery(query)
    .setFilterList(filterList)
    .setSort(lsort)
    .setOffset(offset)
    .setLen(len)
    .setFlags(flags)
    .setNeedDocSet(true);
    QueryResult qr = new QueryResult();
    search(qr,qc);
    return qr.getDocListAndSet();
  }

  /**
   * Returns documents matching both <code>query</code> and <code>filter</code>
   * and sorted by <code>sort</code>. Also returns the compete set of documents
   * matching <code>query</code> and <code>filter</code> (regardless of <code>offset</code> and <code>len</code>).
   * <p>
   * FUTURE: The returned DocList may be retrieved from a cache.
   *
   * @param query
   * @param filter   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException
   */
  public DocListAndSet getDocListAndSet(Query query, DocSet filter, Sort lsort, int offset, int len) throws IOException {
    QueryCommand qc = new QueryCommand();
    qc.setQuery(query)
    .setFilter(filter)
    .setSort(lsort)
    .setOffset(offset)
    .setLen(len)
    .setNeedDocSet(true);
    QueryResult qr = new QueryResult();
    search(qr,qc);
    return qr.getDocListAndSet();
  }

  /**
   * Returns documents matching both <code>query</code> and <code>filter</code>
   * and sorted by <code>sort</code>.  Also returns the compete set of documents
   * matching <code>query</code> and <code>filter</code> (regardless of <code>offset</code> and <code>len</code>).
   * <p>
   * This method is cache aware and may make an insertion into the cache 
   * as a result of this call.
   * <p>
   * FUTURE: The returned DocList may be retrieved from a cache.
   * <p>
   * The DocList and DocSet returned should <b>not</b> be modified.
   *
   * @param query
   * @param filter   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @param flags    user supplied flags for the result set
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException
   */
  public DocListAndSet getDocListAndSet(Query query, DocSet filter, Sort lsort, int offset, int len, int flags) throws IOException {
    QueryCommand qc = new QueryCommand();
    qc.setQuery(query)
    .setFilter(filter)
    .setSort(lsort)
    .setOffset(offset)
    .setLen(len)
    .setFlags(flags)
    .setNeedDocSet(true);
    QueryResult qr = new QueryResult();
    search(qr,qc);
    return qr.getDocListAndSet();
  }

  protected DocList sortDocSet(DocSet set, Sort sort, int nDocs) throws IOException {
    // bit of a hack to tell if a set is sorted - do it better in the futute.
    boolean inOrder = set instanceof BitDocSet || set instanceof SortedIntDocSet;

    TopDocsCollector topCollector = TopFieldCollector.create(weightSort(sort), nDocs, false, false, false, inOrder);

    DocIterator iter = set.iterator();
    int base=0;
    int end=0;
    int readerIndex = -1;
    SolrIndexReader r=null;


    while(iter.hasNext()) {
      int doc = iter.nextDoc();
      while (doc>=end) {
        r = reader.getLeafReaders()[++readerIndex];
        base = reader.getLeafOffsets()[readerIndex];
        end = base + r.maxDoc();
        topCollector.setNextReader(r, base);
        // we should never need to set the scorer given the settings for the collector
      }
      topCollector.collect(doc-base);
    }

    TopDocs topDocs = topCollector.topDocs(0, nDocs);

    int nDocsReturned = topDocs.scoreDocs.length;
    int[] ids = new int[nDocsReturned];

    for (int i=0; i<nDocsReturned; i++) {
      ScoreDoc scoreDoc = topDocs.scoreDocs[i];
      ids[i] = scoreDoc.doc;
    }

    return new DocSlice(0,nDocsReturned,ids,null,topDocs.totalHits,0.0f);
  }



  /**
   * Returns the number of documents that match both <code>a</code> and <code>b</code>.
   * <p>
   * This method is cache-aware and may check as well as modify the cache.
   *
   * @param a
   * @param b
   * @return the numer of documents in the intersection between <code>a</code> and <code>b</code>.
   * @throws IOException
   */
  public int numDocs(Query a, DocSet b) throws IOException {
    // Negative query if absolute value different from original
    Query absQ = QueryUtils.getAbs(a);
    DocSet positiveA = getPositiveDocSet(absQ);
    return a==absQ ? b.intersectionSize(positiveA) : b.andNotSize(positiveA);
  }

  /**
   * Returns the number of documents that match both <code>a</code> and <code>b</code>.
   * <p>
   * This method is cache-aware and may check as well as modify the cache.
   *
   * @param a
   * @param b
   * @return the numer of documents in the intersection between <code>a</code> and <code>b</code>.
   * @throws IOException
   */
  public int numDocs(Query a, Query b) throws IOException {
    Query absA = QueryUtils.getAbs(a);
    Query absB = QueryUtils.getAbs(b);     
    DocSet positiveA = getPositiveDocSet(absA);
    DocSet positiveB = getPositiveDocSet(absB);

    // Negative query if absolute value different from original
    if (a==absA) {
      if (b==absB) return positiveA.intersectionSize(positiveB);
      return positiveA.andNotSize(positiveB);
    }
    if (b==absB) return positiveB.andNotSize(positiveA);

    // if both negative, we need to create a temp DocSet since we
    // don't have a counting method that takes three.
    DocSet all = getPositiveDocSet(matchAllDocsQuery);

    // -a -b == *:*.andNot(a).andNotSize(b) == *.*.andNotSize(a.union(b))
    // we use the last form since the intermediate DocSet should normally be smaller.
    return all.andNotSize(positiveA.union(positiveB));
  }


  /**
   * Takes a list of docs (the doc ids actually), and returns an array 
   * of Documents containing all of the stored fields.
   */
  public Document[] readDocs(DocList ids) throws IOException {
    Document[] docs = new Document[ids.size()];
    readDocs(docs,ids);
    return docs;
  }



  /**
   * Warm this searcher based on an old one (primarily for auto-cache warming).
   */
  public void warm(SolrIndexSearcher old) throws IOException {
    // Make sure this is first!  filters can help queryResults execute!
    boolean logme = log.isInfoEnabled();
    long warmingStartTime = System.currentTimeMillis();
    // warm the caches in order...
    for (int i=0; i<cacheList.length; i++) {
      if (logme) log.info("autowarming " + this + " from " + old + "\n\t" + old.cacheList[i]);
      this.cacheList[i].warm(this, old.cacheList[i]);
      if (logme) log.info("autowarming result for " + this + "\n\t" + this.cacheList[i]);
    }
    warmupTime = System.currentTimeMillis() - warmingStartTime;
  }

  /**
   * return the named generic cache
   */
  public SolrCache getCache(String cacheName) {
    return cacheMap.get(cacheName);
  }

  /**
   * lookup an entry in a generic cache
   */
  public Object cacheLookup(String cacheName, Object key) {
    SolrCache cache = cacheMap.get(cacheName);
    return cache==null ? null : cache.get(key);
  }

  /**
   * insert an entry in a generic cache
   */
  public Object cacheInsert(String cacheName, Object key, Object val) {
    SolrCache cache = cacheMap.get(cacheName);
    return cache==null ? null : cache.put(key,val);
  }

  public long getOpenTime() {
    return openTime;
  }

  @Override
  public Explanation explain(Query query, int doc) throws IOException {
    return super.explain(QueryUtils.makeQueryable(query), doc);
  }

  /////////////////////////////////////////////////////////////////////
  // SolrInfoMBean stuff: Statistics and Module Info
  /////////////////////////////////////////////////////////////////////

  public String getName() {
    return SolrIndexSearcher.class.getName();
  }

  public String getVersion() {
    return SolrCore.version;
  }

  public String getDescription() {
    return "index searcher";
  }

  public Category getCategory() {
    return Category.CORE;
  }

  public String getSourceId() {
    return "$Id$";
  }

  public String getSource() {
    return "$URL$";
  }

  public URL[] getDocs() {
    return null;
  }

  public NamedList getStatistics() {
    NamedList lst = new SimpleOrderedMap();
    lst.add("searcherName", name);
    lst.add("caching", cachingEnabled);
    lst.add("numDocs", reader.numDocs());
    lst.add("maxDoc", reader.maxDoc());
    lst.add("reader", reader.toString());
    lst.add("readerDir", reader.directory());
    lst.add("indexVersion", reader.getVersion());
    lst.add("openedAt", new Date(openTime));
    if (registerTime!=0) lst.add("registeredAt", new Date(registerTime));
    lst.add("warmupTime", warmupTime);
    return lst;
  }

  /**
   * A query request command to avoid having to change the method signatures
   * if we want to pass additional information to the searcher.
   */
  public static class QueryCommand {
    private Query query;
    private List<Query> filterList;
    private DocSet filter;
    private Sort sort;
    private int offset;
    private int len;
    private int supersetMaxDoc;
    private int flags;
    private long timeAllowed = -1;

    public Query getQuery() { return query; }
    public QueryCommand setQuery(Query query) {
      this.query = query;
      return this;
    }

    public List<Query> getFilterList() { return filterList; }
    /**
     * @throws IllegalArgumentException if filter is not null.
     */
    public QueryCommand setFilterList(List<Query> filterList) {
      if( filter != null ) {
        throw new IllegalArgumentException( "Either filter or filterList may be set in the QueryCommand, but not both." );
      }
      this.filterList = filterList;
      return this;
    }
    /**
     * A simple setter to build a filterList from a query
     * @throws IllegalArgumentException if filter is not null.
     */
    public QueryCommand setFilterList(Query f) {
      if( filter != null ) {
        throw new IllegalArgumentException( "Either filter or filterList may be set in the QueryCommand, but not both." );
      }
      filterList = null;
      if (f != null) {
        filterList = new ArrayList<Query>(2);
        filterList.add(f);
      }
      return this;
    }

    public DocSet getFilter() { return filter; }
    /**
     * @throws IllegalArgumentException if filterList is not null.
     */
    public QueryCommand setFilter(DocSet filter) {
      if( filterList != null ) {
        throw new IllegalArgumentException( "Either filter or filterList may be set in the QueryCommand, but not both." );
      }
      this.filter = filter;
      return this;
    }

    public Sort getSort() { return sort; }
    public QueryCommand setSort(Sort sort) {
      this.sort = sort;
      return this;
    }

    public int getOffset() { return offset; }
    public QueryCommand setOffset(int offset) {
      this.offset = offset;
      return this;
    }

    public int getLen() { return len; }
    public QueryCommand setLen(int len) {
      this.len = len;
      return this;
    }

    public int getSupersetMaxDoc() { return supersetMaxDoc; }
    public QueryCommand setSupersetMaxDoc(int supersetMaxDoc) {
      this.supersetMaxDoc = supersetMaxDoc;
      return this;
    }

    public int getFlags() {
      return flags;
    }

    public QueryCommand replaceFlags(int flags) {
      this.flags = flags;
      return this;
    }

    public QueryCommand setFlags(int flags) {
      this.flags |= flags;
      return this;
    }

    public QueryCommand clearFlags(int flags) {
      this.flags &= ~flags;
      return this;
    }

    public long getTimeAllowed() { return timeAllowed; }
    public QueryCommand setTimeAllowed(long timeAllowed) {
      this.timeAllowed = timeAllowed;
      return this;
    }

    public boolean isNeedDocSet() { return (flags & GET_DOCSET) != 0; }
    public QueryCommand setNeedDocSet(boolean needDocSet) {
      return needDocSet ? setFlags(GET_DOCSET) : clearFlags(GET_DOCSET);
    }
  }

  /**
   * The result of a search.
   */
  public static class QueryResult {
    private boolean partialResults;
    private DocListAndSet docListAndSet;
    public Object groupedResults; // Todo: Refactor. At least getter setter and different type.

    public DocList getDocList() { return docListAndSet.docList; }
    public void setDocList(DocList list) {
      if( docListAndSet == null ) {
        docListAndSet = new DocListAndSet();
      }
      docListAndSet.docList = list;
    }

    public DocSet getDocSet() { return docListAndSet.docSet; }
    public void setDocSet(DocSet set) {
      if( docListAndSet == null ) {
        docListAndSet = new DocListAndSet();
      }
      docListAndSet.docSet = set;
    }

    public boolean isPartialResults() { return partialResults; }
    public void setPartialResults(boolean partialResults) { this.partialResults = partialResults; }

    public void setDocListAndSet( DocListAndSet listSet ) { docListAndSet = listSet; }
    public DocListAndSet getDocListAndSet() { return docListAndSet; }
  }

}



class FilterImpl extends Filter {
  final DocSet filter;
  final Filter topFilter;
  final List<Weight> weights;

  public FilterImpl(DocSet filter, List<Weight> weights) {
    this.filter = filter;
    this.weights = weights;
    this.topFilter = filter == null ? null : filter.getTopFilter();
  }

  @Override
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    DocIdSet sub = topFilter == null ? null : topFilter.getDocIdSet(reader);
    if (weights.size() == 0) return sub;
    return new FilterSet(sub, reader);
  }

  private class FilterSet extends DocIdSet {
    DocIdSet docIdSet;
    IndexReader reader;

    public FilterSet(DocIdSet docIdSet, IndexReader reader) {
      this.docIdSet = docIdSet;
      this.reader = reader;
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      List<DocIdSetIterator> iterators = new ArrayList<DocIdSetIterator>(weights.size()+1);
      if (docIdSet != null) {
        DocIdSetIterator iter = docIdSet.iterator();
        if (iter == null) return null;
        iterators.add(iter);
      }
      for (Weight w : weights) {
        Scorer scorer = w.scorer(reader, true, false);
        if (scorer == null) return null;
        iterators.add(scorer);
      }
      if (iterators.size()==0) return null;
      if (iterators.size()==1) return iterators.get(0);
      if (iterators.size()==2) return new DualFilterIterator(iterators.get(0), iterators.get(1));
      return new FilterIterator(iterators.toArray(new DocIdSetIterator[iterators.size()]));
    }
  }

  private static class FilterIterator extends DocIdSetIterator {
    final DocIdSetIterator[] iterators;
    final DocIdSetIterator first;

    public FilterIterator(DocIdSetIterator[] iterators) {
      this.iterators = iterators;
      this.first = iterators[0];
    }

    @Override
    public int docID() {
      return first.docID();
    }

    private int doNext(int doc) throws IOException {
      int which=0;  // index of the iterator with the highest id
      int i=1;
      outer: for(;;) {
        for (; i<iterators.length; i++) {
          if (i == which) continue;
          DocIdSetIterator iter = iterators[i];
          int next = iter.advance(doc);
          if (next != doc) {
            doc = next;
            which = i;
            i = 0;
            continue outer;
          }
        }
        return doc;
      }
    }


    @Override
    public int nextDoc() throws IOException {
      return doNext(first.nextDoc());
    }

    @Override
    public int advance(int target) throws IOException {
      return doNext(first.advance(target));
    }
  }

  private static class DualFilterIterator extends DocIdSetIterator {
    final DocIdSetIterator a;
    final DocIdSetIterator b;

    public DualFilterIterator(DocIdSetIterator a, DocIdSetIterator b) {
      this.a = a;
      this.b = b;
    }

    @Override
    public int docID() {
      return a.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int doc = a.nextDoc();
      for(;;) {
        int other = b.advance(doc);
        if (other == doc) return doc;
        doc = a.advance(other);
        if (other == doc) return doc;
      }
    }

    @Override
    public int advance(int target) throws IOException {
      int doc = a.advance(target);
      for(;;) {
        int other = b.advance(doc);
        if (other == doc) return doc;
        doc = a.advance(other);
        if (other == doc) return doc;
      }
    }
  }

}

