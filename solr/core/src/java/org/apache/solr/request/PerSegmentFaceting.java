/*
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

package org.apache.solr.request;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.BoundedTreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class PerSegmentFaceting {

  private static Logger log = LoggerFactory.getLogger(PerSegmentFaceting.class);

  // input params
  SolrIndexSearcher searcher;
  DocSet docs;
  String fieldName;
  int offset;
  int limit;
  int mincount;
  boolean missing;
  String sort;
  String prefix;

  Filter baseSet;

  int nThreads;

  public PerSegmentFaceting(SolrIndexSearcher searcher, DocSet docs, String fieldName, int offset, int limit, int mincount, boolean missing, String sort, String prefix) {
    this.searcher = searcher;
    this.docs = docs;
    this.fieldName = fieldName;
    this.offset = offset;
    this.limit = limit;
    this.mincount = mincount;
    this.missing = missing;
    this.sort = sort;
    this.prefix = prefix;
  }

  public void setNumThreads(int threads) {
    nThreads = threads;
  }


  NamedList<Integer> getFacetCounts(Executor executor) throws IOException {

    CompletionService<SegFacet> completionService = new ExecutorCompletionService<>(executor);

    // reuse the translation logic to go from top level set to per-segment set
    baseSet = docs.getTopFilter();

    final List<AtomicReaderContext> leaves = searcher.getTopReaderContext().leaves();
    // The list of pending tasks that aren't immediately submitted
    // TODO: Is there a completion service, or a delegating executor that can
    // limit the number of concurrent tasks submitted to a bigger executor?
    LinkedList<Callable<SegFacet>> pending = new LinkedList<>();

    int threads = nThreads <= 0 ? Integer.MAX_VALUE : nThreads;

    int numValidLeaves = 0;
    for (final AtomicReaderContext leaf : leaves) {
      final SegFacet segFacet = newSegFacet(leaf);

      if (segFacet == null) {
        continue;
      }

      ++numValidLeaves;

      Callable<SegFacet> task = new Callable<SegFacet>() {
        @Override
        public SegFacet call() throws Exception {
          segFacet.countTerms();
          return segFacet;
        }
      };

      // TODO: if limiting threads, submit by largest segment first?

      if (--threads >= 0) {
        completionService.submit(task);
      } else {
        pending.add(task);
      }
    }


    // now merge the per-segment results
    PriorityQueue<SegFacet> queue = new PriorityQueue<SegFacet>(leaves.size()) {
      @Override
      protected boolean lessThan(SegFacet a, SegFacet b) {
        return a.tempBR().compareTo(b.tempBR()) < 0;
      }
    };


    boolean hasMissingCount=false;
    int missingCount=0;
    for (int i=0; i<numValidLeaves; i++) {
      SegFacet seg = null;

      try {
        Future<SegFacet> future = completionService.take();
        seg = future.get();
        if (!pending.isEmpty()) {
          completionService.submit(pending.removeFirst());
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) {
          throw (RuntimeException)cause;
        } else {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error in per-segment faceting on field: " + fieldName, cause);
        }
      }

      if (seg.counts() == null) {
        continue;
      }

      if (seg.startTermIndex() < seg.endTermIndex()) {
        if (seg.startTermIndex()==-1) {
          hasMissingCount=true;
          missingCount += seg.counts()[0];
          seg.setPos(0);
        } else {
          seg.setPos(seg.startTermIndex());
        }
        if (seg.pos() < seg.endTermIndex()) {
          seg.initTermsEnum();
          seg.termsEnum().seekExact(seg.pos());
          seg.setTempBR(seg.termsEnum().term());
          queue.add(seg);
        }
      }
    }

    FacetCollector collector;
    if (sort.equals(FacetParams.FACET_SORT_COUNT) || sort.equals(FacetParams.FACET_SORT_COUNT_LEGACY)) {
      collector = new CountSortedFacetCollector(offset, limit, mincount);
    } else {
      collector = new IndexSortedFacetCollector(offset, limit, mincount);
    }

    BytesRef val = new BytesRef();

    while (queue.size() > 0) {
      SegFacet seg = queue.top();

      // we will normally end up advancing the term enum for this segment
      // while still using "val", so we need to make a copy since the BytesRef
      // may be shared across calls.
      val.copyBytes(seg.tempBR);

      int count = 0;

      do {
        count += seg.counts[seg.pos - seg.startTermIndex()];

        // if mincount>0 then seg.pos++ can skip ahead to the next non-zero entry.
        do{
          ++seg.pos;
        }
        while(
                (seg.pos < seg.endTermIndex())  //stop incrementing before we run off the end
                        && (seg.termsEnum().next() != null || true) //move term enum forward with position -- dont care about value
                        && (mincount > 0) //only skip ahead if mincount > 0
                        && (seg.counts[seg.pos - seg.startTermIndex()] == 0) //check zero count
                );

        if (seg.pos >= seg.endTermIndex()) {
          queue.pop();
          seg = queue.top();
        }  else {
          seg.tempBR = seg.termsEnum().term();
          seg = queue.updateTop();
        }
      } while (seg != null && val.compareTo(seg.tempBR) == 0);

      boolean stop = collector.collect(val, count);
      if (stop) break;
    }

    NamedList<Integer> res = collector.getFacetCounts();

    // convert labels to readable form
    FieldType ft = searcher.getSchema().getFieldType(fieldName);
    int sz = res.size();
    for (int i=0; i<sz; i++) {
      res.setName(i, ft.indexedToReadable(res.getName(i)));
    }

    if (missing) {
      if (!hasMissingCount) {
        missingCount = SimpleFacets.getFieldMissingCount(searcher,docs,fieldName);
      }
      res.add(null, missingCount);
    }

    return res;
  }

  abstract class SegFacet {
    AtomicReaderContext context;
    SegFacet(AtomicReaderContext context) {
      this.context = context;
    }

    int[] counts;

    int pos; // only used when merging
    TermsEnum tenum; // only used when merging

    BytesRef tempBR = new BytesRef();

    abstract protected void countTerms() throws IOException;

    protected AtomicReaderContext context() {
      return context;
    }
    protected int[] counts() {
      return counts;
    }
    protected TermsEnum termsEnum() { return tenum; }
    protected int pos() { return pos; }
    protected BytesRef tempBR() {
      return tempBR;
    }

    protected void setCounts(int[] counts) {
      this.counts = counts;
    }
    protected void setPos(int pos) { this.pos = pos; }
    protected void setTermsEnum(TermsEnum tenum) { this.tenum = tenum; }
    protected void setTempBR(BytesRef tempBR) { this.tempBR = tempBR; }

    abstract protected int startTermIndex();
    abstract protected int endTermIndex();
    abstract protected void initTermsEnum();
  }

  private class SortedDocValuesSegFacet extends SegFacet {
    final private SortedDocValues si;
    private int startTermIndex;
    private int endTermIndex;

    SortedDocValuesSegFacet(AtomicReaderContext context) throws IOException {
      super(context);
      si = FieldCache.DEFAULT.getTermsIndex(context().reader(), fieldName);
      initStartAndEndTermIndex();
    }

    private void initStartAndEndTermIndex() {
      int startTermIndex;
      int endTermIndex;

      if (prefix!=null) {
        BytesRef prefixRef = new BytesRef(prefix);
        startTermIndex = si.lookupTerm(prefixRef);
        if (startTermIndex<0) startTermIndex=-startTermIndex-1;
        prefixRef.append(UnicodeUtil.BIG_TERM);
        // TODO: we could constrain the lower endpoint if we had a binarySearch method that allowed passing start/end
        endTermIndex = si.lookupTerm(prefixRef);
        assert endTermIndex < 0;
        endTermIndex = -endTermIndex-1;
      } else {
        startTermIndex=-1;
        endTermIndex=si.getValueCount();
      }

      this.startTermIndex = startTermIndex;
      this.endTermIndex = endTermIndex;
    }

    @Override
    protected int startTermIndex() {
      return startTermIndex;
    }

    @Override
    protected int endTermIndex() {
      return endTermIndex;
    }

    @Override
    protected void countTerms() throws IOException {
      final int nTerms=endTermIndex-startTermIndex;
      if (nTerms>0) {
        // count collection array only needs to be as big as the number of terms we are
        // going to collect counts for.
        final int[] counts = new int[nTerms];
        DocIdSet idSet = baseSet.getDocIdSet(context(), null);  // this set only includes live docs
        DocIdSetIterator iter = idSet.iterator();


        ////
        int doc;

        if (prefix==null) {
          // specialized version when collecting counts for all terms
          while ((doc = iter.nextDoc()) < DocIdSetIterator.NO_MORE_DOCS) {
            counts[1+si.getOrd(doc)]++;
          }
        } else {
          // version that adjusts term numbers because we aren't collecting the full range
          while ((doc = iter.nextDoc()) < DocIdSetIterator.NO_MORE_DOCS) {
            int term = si.getOrd(doc);
            int arrIdx = term-startTermIndex;
            if (arrIdx>=0 && arrIdx<nTerms) counts[arrIdx]++;
          }
        }

        setCounts(counts);
      }
    }

    @Override
    protected void initTermsEnum() {
      setTermsEnum(si.termsEnum());
    }
  }

  private class SortedSetDocValuesSegFacet extends SegFacet {
    final private SortedSetDocValues si;
    private int startTermIndex;
    private int endTermIndex;

    SortedSetDocValuesSegFacet(AtomicReaderContext context) throws IOException {
      super(context);
      si = FieldCache.DEFAULT.getDocTermOrds(context().reader(), fieldName);
      initStartAndEndTermIndex();
    }

    private void initStartAndEndTermIndex() {
      long startTermIndex;
      long endTermIndex;

      if (prefix!=null) {
        BytesRef prefixRef = new BytesRef(prefix);
        startTermIndex = si.lookupTerm(prefixRef);
        if (startTermIndex<0) startTermIndex=-startTermIndex-1;
        prefixRef.append(UnicodeUtil.BIG_TERM);
        // TODO: we could constrain the lower endpoint if we had a binarySearch method that allowed passing start/end
        endTermIndex = si.lookupTerm(prefixRef);
        assert endTermIndex < 0;
        endTermIndex = -endTermIndex-1;
      } else {
        startTermIndex=-1;
        endTermIndex=si.getValueCount();
      }

      if (startTermIndex < Integer.MIN_VALUE || endTermIndex < Integer.MIN_VALUE
              || startTermIndex > Integer.MAX_VALUE || endTermIndex > Integer.MAX_VALUE) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid start=" + startTermIndex + " end=" + endTermIndex + " term indices");
      }

      this.startTermIndex = (int) startTermIndex;
      this.endTermIndex = (int) endTermIndex;
    }

    @Override
    protected int startTermIndex() {
      return startTermIndex;
    }

    @Override
    protected int endTermIndex() {
      return endTermIndex;
    }

    @Override
    protected void countTerms() throws IOException {
      final int nTerms = endTermIndex - startTermIndex;
      if (nTerms>0) {
        // count collection array only needs to be as big as the number of terms we are
        // going to collect counts for.
        final int[] counts = new int[nTerms];
        DocIdSet idSet = baseSet.getDocIdSet(context(), null);  // this set only includes live docs
        DocIdSetIterator iter = idSet.iterator();

        int doc;

        // specialized version when collecting counts for all terms
        while ((doc = iter.nextDoc()) < DocIdSetIterator.NO_MORE_DOCS) {
          si.setDocument(doc);

          long ord;
          while ((ord = si.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
            assert(ord > Integer.MIN_VALUE && ord < Integer.MAX_VALUE);
            counts[(int) (1 + ord)]++;
          }
        }
        setCounts(counts);
      }
    }

    @Override
    protected void initTermsEnum() {
      setTermsEnum(si.termsEnum());
    }
  }

  private SegFacet newSegFacet(AtomicReaderContext atomicReaderContext) throws IOException {
    final FieldInfo fieldInfo = atomicReaderContext.reader().getFieldInfos().fieldInfo(fieldName);

    if (fieldInfo == null) {
      log.debug("Cannot get field={}", fieldName);
      return null;
    }

    if (!fieldInfo.hasDocValues()) {
      log.error("Cannot facet on field={} with no docvalues", fieldName);
      return null;
    }

    final FieldInfo.DocValuesType docValuesType = fieldInfo.getDocValuesType();
    switch(docValuesType) {
      case SORTED:
        return new SortedDocValuesSegFacet(atomicReaderContext);
      case SORTED_SET:
        return new SortedSetDocValuesSegFacet(atomicReaderContext);
      default:
        log.error("Cannot facet on field={} with unsupported docvalues type={}", fieldName, docValuesType);
        return null;
    }
  }
}
