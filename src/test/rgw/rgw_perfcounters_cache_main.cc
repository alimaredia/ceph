//#include "rgw_perfcounters_cache.h"
//#include "rgw_perfcounters_cache_request.h"
//#include <boost/intrusive/list.hpp>
#include <iostream>
#include "common/intrusive_lru.h"
#include "common/perf_counters.h"
#include "common/ceph_context.h"

enum RGWCounters {
  l_rgw_metrics_first = 15000,
  l_rgw_req,
  l_rgw_failed_req,
  l_rgw_put_b,
  l_rgw_get_b,
  l_rgw_metrics_last,
};

template <typename LRUItem>
struct item_to_key {
  using type = std::string;
  const type &operator()(const LRUItem &item) {
    return item.instance_labels;
  }
};

struct PCountersCacheEntry : public ceph::common::intrusive_lru_base<
  ceph::common::intrusive_lru_config<
    std::string, PCountersCacheEntry, item_to_key<PCountersCacheEntry>>> {
  std::string instance_labels;
  int value;
  CephContext *cct = NULL;
  PerfCounters *perfcounters_instance = NULL;

  PCountersCacheEntry(std::string key) : instance_labels(key) {}

  ~PCountersCacheEntry() {
    // perf counters instance clean up code
    if(perfcounters_instance) {
      // TODO: fix this
      //ceph_assert(perfcounters_instance);
      //cct->get_perfcounters_collection()->remove(perfcounters_instance);
      //delete perfcounters_instance;
      //perfcounters_instance = NULL;
    }
  }
};

class PCountersCache : public PCountersCacheEntry::lru_t {
private:
  CephContext *cct;
public:
  auto add(std::string key, int value) {


    auto [ref, key_existed] = get_or_create(key);
    if (!key_existed) {
      // perf counters instance creation code
      PerfCountersBuilder plb(cct, key, l_rgw_metrics_first, l_rgw_metrics_last);
      plb.add_u64_counter(l_rgw_req, "req", "Size of puts");
      plb.add_u64_counter(l_rgw_failed_req, "failed_req", "Aborted Requests");
      plb.add_u64_counter(l_rgw_put_b, "put_b", "Size of puts");
      plb.add_u64_counter(l_rgw_get_b, "get_b", "Size of gets");

      PerfCounters *counters = plb.create_perf_counters();
      cct->get_perfcounters_collection()->add(counters);
      ref->perfcounters_instance = counters;

      ref->cct = cct;
      ref->value = value;
    }
    return std::pair(ref, key_existed);
  }

  void inc(std::string label, int indx, uint64_t v) {
    auto ref = get(label);
    if(ref) {
      PerfCounters *counters = ref->perfcounters_instance;
      counters->inc(indx, v);
    }
  }

  uint64_t get_counter(std::string label, int indx) {
    auto ref = get(label);
    uint64_t val= 0;
    if(ref) {
      PerfCounters *counters = ref->perfcounters_instance;
      val = counters->get(indx);
    }
    return val;
  }

  void label_perf_stop(std::string label) {
    auto ref = get(label);
    if(ref) {
      ceph_assert(ref->perfcounters_instance);
      cct->get_perfcounters_collection()->remove(ref->perfcounters_instance);
      delete ref->perfcounters_instance;
      ref->perfcounters_instance = NULL;
    }
  }

  PCountersCache(CephContext *_cct, size_t _cache_size) {
    cct = _cct;
    set_target_size(_cache_size);
  }

  ~PCountersCache() {
  }
};

// Wrapper around PerfCounters Instance + iterator to labels position in list
int main() {
  /*
  int cache_size = 10;
  PerfCountersCache *p = new PerfCountersCache(cache_size, cct);

  std::vector<Request> requests = gen_requests(20, 5,5,10,30);
  process_requests(p, requests);

  p->print_labels();
  p->rgw_metrics_perf_stop();
  delete p;
  delete cct;
  */
  auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);



  // TODO: NEED TO LOOK THROUGH THIS
  size_t target_size = 3;
  PCountersCache cache(cct, target_size);

  std::string key1 = "sally";
  int val1 = 11;
  auto [ref, key_existed] = cache.add(key1, val1);
  std::cout << "instance_labels for entry 1: " << ref->instance_labels << std::endl;

  cache.inc(key1, l_rgw_put_b, 55);
  uint64_t ret = cache.get_counter(key1, l_rgw_put_b);
  std::cout << "ret after inc is " << ret << std::endl;
  cache.label_perf_stop(key1);

  /*
  std::string key2 = "harry";
  int val2 = 22;
  auto [ref2, key_existed2] = cache.add(key2, val2);
  std::cout << "instance_labels for entry 2: " << ref2->instance_labels << std::endl;
  */

  delete cct;

  return 0;
}
