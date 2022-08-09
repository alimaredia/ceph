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
    void update_labels_list(std::string label);

    // evicts least recently updated label from labels list
    // removes labels CacheEntry from cache
    void remove_back_label();

  public:
    PerfCountersCache(int _cache_size, CephContext *_cct) : cache_size(_cache_size), cct(_cct) {}

    // adds label to cache, removes labels from cache if cache size is over limit
    void add_label(std::string label);

    // increases label's perfcounter at indx by v
    void inc(std::string label, int indx, uint64_t v);

    // decreases label's perfcounter at indx by v
    void dec(std::string label, int indx, uint64_t v);

    // get perfcounter indx for label
    uint64_t get(std::string label, int indx);

    // sets label's perfcounter at indx to v
    void set(std::string label, int indx, uint64_t v);

   // rgw_metrics_perf_stop cannot be called in the destructor and it's contents cannot be run in the destructor either
   // it must be called seperately, uncommenting commented out lines in the destructor leads to segfaults
   void rgw_metrics_perf_stop();

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
