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
  //int value;
  CephContext *cct = NULL;
  PerfCounters *perfcounters_instance = NULL;
  //PerfCountersCollection *collection = NULL;

  PCountersCacheEntry(std::string key) : instance_labels(key) {}

  ~PCountersCacheEntry() {
    // perf counters instance clean up code
    if(perfcounters_instance) {
      // TODO: fix this
      //ceph_assert(perfcounters_instance);
      //collection->remove(perfcounters_instance);
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
  auto add(std::string key) {
    auto [ref, key_existed] = get_or_create(key);
    if (!key_existed) {
      // perf counters instance creation code
      PerfCountersBuilder plb(cct, key, l_rgw_metrics_first, l_rgw_metrics_last);
      plb.add_u64_counter(l_rgw_req, "req", "number of reqs");
      plb.add_u64_counter(l_rgw_failed_req, "failed_req", "Aborted Requests");
      plb.add_u64_counter(l_rgw_put_b, "put_b", "Size of puts");
      plb.add_u64_counter(l_rgw_get_b, "get_b", "Size of gets");

      PerfCounters *counters = plb.create_perf_counters();
      cct->get_perfcounters_collection()->add(counters);
      ref->perfcounters_instance = counters;
      //ref->collection = cct->get_perfcounters_collection();
      //ref->collection->add(counters);

      ref->cct = cct;
    }
    return std::pair(ref, key_existed);
  }

  void inc(std::string label, int indx, uint64_t v) {
    auto ref = get(label);
    if(ref) {
      if(ref->perfcounters_instance) {
        PerfCounters *counters = ref->perfcounters_instance;
        counters->inc(indx, v);
      }
    }
  }

  void dec(std::string label, int indx, uint64_t v) {
    auto ref = get(label);
    if(ref) {
      if(ref->perfcounters_instance) {
        PerfCounters *counters = ref->perfcounters_instance;
        counters->dec(indx, v);
      }
    }
  }

  void set_counter(std::string label, int indx, uint64_t val) {
    auto ref = get(label);
    if(ref) {
      if(ref->perfcounters_instance) {
        PerfCounters *counters = ref->perfcounters_instance;
        counters->set(indx, val);
      }
    }
  }

  uint64_t get_counter(std::string label, int indx) {
    auto ref = get(label);
    uint64_t val= 0;
    if(ref) {
      if(ref->perfcounters_instance) {
        PerfCounters *counters = ref->perfcounters_instance;
        val = counters->get(indx);
      }
    }
    return val;
  }

  PCountersCache(CephContext *_cct, size_t _cache_size) {
    cct = _cct;
    set_target_size(_cache_size);
  }

  ~PCountersCache() {
  }
};

int main() {
  auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);

  size_t target_size = 1;
  PCountersCache cache(cct, target_size);

  std::string key1 = "sally";
  auto [ref, key_existed] = cache.add(key1);
  std::cout << "instance_labels for entry 1: " << ref->instance_labels << std::endl;


  uint64_t ret = cache.get_counter(key1, l_rgw_put_b);
  std::cout << "sally initial l_rgw_put_b is " << ret << std::endl;
  cache.inc(key1, l_rgw_put_b, 500);
  ret = cache.get_counter(key1, l_rgw_put_b);
  std::cout << "sally l_rgw_put_b after inc 500 is " << ret << std::endl;
  cache.set_counter(key1, l_rgw_put_b, 300);
  ret = cache.get_counter(key1, l_rgw_put_b);
  std::cout << "sally l_rgw_put_b after set 300 is " << ret << std::endl;
  cache.dec(key1, l_rgw_put_b, 50);
  ret = cache.get_counter(key1, l_rgw_put_b);
  std::cout << "sally l_rgw_put_b after dec 50 is " << ret << std::endl;
  std::cout << std::endl;

  std::string key2 = "harry";
  auto [ref2, key_existed2] = cache.add(key2);
  std::cout << "instance_labels for entry 2: " << ref2->instance_labels << std::endl;

  ret = cache.get_counter(key2, l_rgw_get_b);
  std::cout << "harry initial l_rgw_get_b is " << ret << std::endl;
  cache.set_counter(key2, l_rgw_get_b, 400);
  ret = cache.get_counter(key2, l_rgw_get_b);
  std::cout << "harry l_rgw_get_b after set 400 is " << ret << std::endl;
  cache.inc(key2, l_rgw_get_b, 1000);
  ret = cache.get_counter(key2, l_rgw_get_b);
  std::cout << "harry l_rgw_get_b after inc 1000 is " << ret << std::endl;
  cache.dec(key2, l_rgw_get_b, 500);
  ret = cache.get_counter(key2, l_rgw_get_b);
  std::cout << "harry l_rgw_get_b after dec 500 is " << ret << std::endl;

  delete cct;

  return 0;
}
