#ifndef RGW_PERFCOUNTERS_CACHE_H
#define RGW_PERFCOUNTERS_CACHE_H

#include <iostream>
#include <unordered_map>
#include <list>
#include "common/perf_counters.h"
#include "common/ceph_context.h"

typedef std::list<std::string> labels_list;

enum RGWCounters {
  l_rgw_metrics_first = 15000,
  l_rgw_req,
  l_rgw_failed_req,
  l_rgw_put_b,
  l_rgw_get_b,
  l_rgw_metrics_last,
};

struct CacheEntry {
  PerfCounters *counters;
  labels_list::iterator pos;

  CacheEntry(PerfCounters* _counters, labels_list::iterator _pos) {
    counters = _counters; 
    pos = _pos;
  }

  ~CacheEntry() {
  }
};


class PerfCountersCache {
  private:
    int cache_size = 0;
    int curr_size = 0;
    CephContext *cct;
    PerfCounters *metrics_perfcounter = NULL;
    // map of labels & CacheEntry
    std::unordered_map<std::string, CacheEntry*> cache;
    // list of labels in order of most recently updated
    labels_list labels;

    // checks if label is in cache
    bool label_exists(std::string label);

    // move recently updated items in the list to the front
    void update_labels_list(std::string label) {
      labels.erase(cache[label]->pos);
      cache[label]->pos = labels.insert(labels.begin(), label);
    }

    // evicts least recently updated label from labels list
    // removes labels CacheEntry from cache
    void remove_back_label() {
      std::string removed_label = labels.back();
      std::cout << "removed label is: " << removed_label << std::endl;
      labels.pop_back();

      ceph_assert(cache[removed_label]->counters);
      cct->get_perfcounters_collection()->remove(cache[removed_label]->counters);
      //delete cache[removed_label]->counters;
      delete cache[removed_label]->counters;
      cache[removed_label]->counters = NULL;

      delete cache[removed_label];
      cache[removed_label] = NULL;

      cache.erase(removed_label);
      curr_size--;
    }

  public:
    PerfCountersCache(int _cache_size, CephContext *_cct) : cache_size(_cache_size), cct(_cct) {}

    // adds label to cache, removes labels from cache if cache size is over limit
    void add_label(std::string label) {
      if(label_exists(label)) {
        std::cout << "label already exists in cache" << std::endl;
        return;
      } else if(curr_size >= cache_size) {
        remove_back_label();
      }

      // plb gets cleaned up in it's destructor
      PerfCountersBuilder plb(cct, label, l_rgw_metrics_first, l_rgw_metrics_last);
      plb.add_u64_counter(l_rgw_req, "req", "Size of puts");
      plb.add_u64_counter(l_rgw_failed_req, "failed_req", "Aborted Requests");
      plb.add_u64_counter(l_rgw_put_b, "put_b", "Size of puts");
      plb.add_u64_counter(l_rgw_get_b, "get_b", "Size of gets");

      PerfCounters *counters = plb.create_perf_counters();
      // TODO: NEED TO LOOK THROUGH THIS
      cct->get_perfcounters_collection()->add(counters);
      labels_list::iterator pos = labels.insert(labels.begin(), label);
      CacheEntry *m = new CacheEntry(counters, pos);
      cache[label] = m;
      curr_size++;
    }

    // increases label's perfcounter at indx by v
    void inc(std::string label, int indx, uint64_t v) {
      if(!label_exists(label)) {
        std::cout << "label does not exist in cache" << std::endl;
        return;
      }

      PerfCounters *counters = cache[label]->counters;
      counters->inc(indx, v);

      update_labels_list(label);
    }

    // decreases label's perfcounter at indx by v
    void dec(std::string label, int indx, uint64_t v) {
      if(!label_exists(label)) {
        std::cout << "label does not exist in cache" << std::endl;
        return;
      }

      PerfCounters *counters = cache[label]->counters;
      counters->dec(indx, v);

      update_labels_list(label);
    }

    // get perfcounter indx for label
    uint64_t get(std::string label, int indx) {
      if(!label_exists(label)) {
        std::cout << "label does not exist in cache" << std::endl;
        return -1;
      }

      PerfCounters *counters = cache[label]->counters;
      uint64_t val = counters->get(indx);
      return val;
    }

    // sets label's perfcounter at indx to v
    void set(std::string label, int indx, uint64_t v) {
      if(!label_exists(label)) {
        std::cout << "label does not exist in cache" << std::endl;
        return;
      }

      PerfCounters *counters = cache[label]->counters;
      counters->set(indx, v);

      update_labels_list(label);
    }

   // rgw_metrics_perf_stop cannot be called in the destructor and it's contents cannot be run in the destructor either
   // it must be called seperately, uncommenting commented out lines in the destructor leads to segfaults
   void rgw_metrics_perf_stop() {
     for(auto it = cache.begin(); it != cache.end(); ++it ) {
       ceph_assert(it->second->counters);
       cct->get_perfcounters_collection()->remove(it->second->counters);
       delete it->second->counters;
     }
   }

   ~PerfCountersCache() {
     //std::cout << "~PerfCountersCache() Destructor Called" << std::endl;

     //rgw_metrics_perf_stop();
     // deallocate memory in cache
     for(auto it = cache.begin(); it != cache.end(); ++it ) {
       //ceph_assert(it->second->counters);
       //cct->get_perfcounters_collection()->remove(it->second->counters);
       //delete it->second->counters;

       delete it->second;
       it->second = NULL;
      }
    }

    //print labels list for debugging, will delete later
    void print_labels() {
      std::cout << "labels list is: [";
      for(auto it = labels.begin(); it != labels.end(); ++it ) {
        std::cout << *it << ", ";
      }
      std::cout << "]" << std::endl;
        
      std::cout << "labels in cache are: [";
      for(auto it = cache.begin(); it != cache.end(); ++it ) {
        std::cout << it->first << ", ";
      }
      std::cout << "]" << std::endl;

      /*
      std::cout << "labels list from pos in cache is: [";
      for(auto it = cache.begin(); it != cache.end(); ++it ) {
        std::cout << *(it->second->pos) << ", ";
      }
      std::cout << "]" << std::endl;
      */
    }
};


#endif
