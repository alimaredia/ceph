#include <iostream>
#include <unordered_map>
#include <list>
#include "common/perf_counters.h"
#include "common/ceph_context.h"
//#include <boost/intrusive/list.hpp>
//#include "common/intrusive_lru.h"

typedef std::list<std::string> labels_list;

enum RGWCounters {
  l_rgw_metrics_first = 15000,
  l_rgw_put_b,
  l_rgw_metrics_last,
};

struct Request {
  std::string label;
  RGWCounters counter;
  uint64_t delta;

  Request(std::string _label, RGWCounters _counter, uint64_t _delta) {
    label = _label; 
    counter = _counter;
    delta = _delta;
  }
};

// Wrapper around PerfCounters Instance + iterator to labels position in list
struct CacheEntry {
  PerfCounters *counters;
  labels_list::iterator *pos;

  CacheEntry(PerfCounters* _counters, labels_list::iterator* _pos) {
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
    bool label_exists(std::string label) {
      std::unordered_map<std::string, CacheEntry*>::iterator got = cache.find(label);
      if(got != cache.end()) {
          return true;
      } 
      return false;
    }

    // move recently updated items in the list to the front
    void update_labels_list(std::string label) {
      labels.erase(*(cache[label]->pos));
      *(cache[label]->pos) = labels.insert(labels.begin(), label);
    }

    // evicts least recently updated label from labels list
    // removes labels CacheEntry from cache
    void remove_label() {
        std::string removed_label = labels.back();
        labels.pop_back();

        ceph_assert(cache[removed_label]->counters);
        cct->get_perfcounters_collection()->remove(cache[removed_label]->counters);
        //delete cache[removed_label]->counters;
        cache[removed_label]->counters = NULL;

        delete cache[removed_label]->pos;
        cache[removed_label]->pos = NULL;

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
      } else if(cache_size == curr_size) {
        remove_label();
      }

      // plb gets cleaned up in it's destructor
      PerfCountersBuilder plb(cct, label, l_rgw_metrics_first, l_rgw_metrics_last);
      plb.add_u64_counter(l_rgw_put_b, "put_b", "Size of puts");

      PerfCounters *counters = plb.create_perf_counters();
      // TODO: NEED TO LOOK THROUGH THIS
      cct->get_perfcounters_collection()->add(counters);
      labels_list::iterator *pos = new labels_list::iterator(labels.insert(labels.begin(), label));
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
    void get(std::string label, int indx) {
      if(!label_exists(label)) {
        std::cout << "label does not exist in cache" << std::endl;
        return;
      }

      PerfCounters *counters = cache[label]->counters;
      uint64_t val = counters->get(indx);
      std::cout << "value of l_rgw_put_b in get() is " << val << std::endl;
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

    void rgw_metrics_perf_stop() {
      for(auto it = cache.begin(); it != cache.end(); ++it ) {
        ceph_assert(it->second->counters);
        cct->get_perfcounters_collection()->remove(it->second->counters);
        delete it->second->counters;
      }

      std::cout << "Stopped rgw metrics perf counters" << std::endl;
    }

    ~PerfCountersCache() {
      //std::cout << "~PerfCountersCache() Destructor Called" << std::endl;
      // deallocate memory in cache
      for(auto it = cache.begin(); it != cache.end(); ++it ) {
        delete it->second->pos;
        delete it->second;
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
        std::cout << **(it->second->pos) << ", ";
      }
      std::cout << "]" << std::endl;
      */
    }
};

int main() {
  auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);
  PerfCountersCache p(5, cct);

  std::string label = "rgw::user=U,bucket=B";
  std::string label2 = "rgw::user=U2,bucket=B2";
  std::string label3 = "rgw::user=U3,bucket=B3";
  std::string label4 = "rgw::user=U4,bucket=B4";
  std::string label5 = "rgw::user=U5,bucket=B5";
  std::string label6 = "rgw::user=U6,bucket=B6";
  std::string label7 = "rgw::user=U7,bucket=B7";
  p.add_label(label);
  p.add_label(label2);
  p.add_label(label3);
  p.add_label(label4);
  p.add_label(label5);
  p.print_labels();
  Request r(label, l_rgw_put_b, 256);

  p.inc(label, l_rgw_put_b, 10);
  p.get(label, l_rgw_put_b);
  p.set(label, l_rgw_put_b, 20);
  p.set(label3, l_rgw_put_b, 34);
  p.get(label, l_rgw_put_b);
  p.get(label3, l_rgw_put_b);
  p.dec(label, l_rgw_put_b, 7);
  p.get(label, l_rgw_put_b);

  p.add_label(label6);
  //p.add_label(label7);
  p.print_labels();
  p.rgw_metrics_perf_stop();
  delete cct;

  return 0;
}

