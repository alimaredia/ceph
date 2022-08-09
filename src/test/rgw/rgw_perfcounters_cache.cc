#include "rgw_perfcounters_cache.h"

bool PerfCountersCache::label_exists(std::string label) {
  std::unordered_map<std::string, CacheEntry*>::iterator got = cache.find(label);
  if(got != cache.end()) {
      return true;
  } 
  return false;
}

