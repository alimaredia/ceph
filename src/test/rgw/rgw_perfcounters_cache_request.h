#ifndef RGW_PERFCOUNTERS_CACHE_REQUEST_H
#define RGW_PERFCOUNTERS_CACHE_REQUEST_H

#include "rgw_perfcounters_cache.h"

#include <vector>

struct Request {
  std::string label;
  RGWCounters counter;
  uint64_t val;
  int op;

  Request(std::string _label, RGWCounters _counter, uint64_t _val, int _op) {
    label = _label; 
    counter = _counter;
    val = _val;
    op = _op;
  }
};

std::vector<Request> gen_requests(int num_requests, int num_buckets, int num_users, int min_val, int max_val);

void process_requests(PerfCountersCache *p, std::vector<Request> requests);

#endif
