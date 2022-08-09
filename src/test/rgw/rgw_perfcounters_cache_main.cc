#include "rgw_perfcounters_cache.h"
#include "rgw_perfcounters_cache_request.h"
//#include <boost/intrusive/list.hpp>
//#include "common/intrusive_lru.h"


// Wrapper around PerfCounters Instance + iterator to labels position in list
int main() {
  auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);
  int cache_size = 10;
  PerfCountersCache *p = new PerfCountersCache(cache_size, cct);

  std::vector<Request> requests = gen_requests(20, 5,5,10,30);
  process_requests(p, requests);

  p->print_labels();
  p->rgw_metrics_perf_stop();
  delete p;
  delete cct;

  return 0;
}
