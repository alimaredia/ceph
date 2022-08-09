#include "rgw_perfcounters_cache.h"

bool PerfCountersCache::label_exists(std::string label) {
  std::unordered_map<std::string, CacheEntry*>::iterator got = cache.find(label);
  if(got != cache.end()) {
      return true;
  } 
  return false;
}

void PerfCountersCache::update_labels_list(std::string label) {
  labels.erase(cache[label]->pos);
  cache[label]->pos = labels.insert(labels.begin(), label);
}

void PerfCountersCache::remove_back_label() {
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

void PerfCountersCache::add_label(std::string label) {
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

void PerfCountersCache::inc(std::string label, int indx, uint64_t v) {
  if(!label_exists(label)) {
    std::cout << "label does not exist in cache" << std::endl;
    return;
  }

  PerfCounters *counters = cache[label]->counters;
  counters->inc(indx, v);

  update_labels_list(label);
}

void PerfCountersCache::dec(std::string label, int indx, uint64_t v) {
  if(!label_exists(label)) {
    std::cout << "label does not exist in cache" << std::endl;
    return;
  }

  PerfCounters *counters = cache[label]->counters;
  counters->dec(indx, v);

  update_labels_list(label);
}

uint64_t PerfCountersCache::get(std::string label, int indx) {
  if(!label_exists(label)) {
    std::cout << "label does not exist in cache" << std::endl;
    return -1;
  }

  PerfCounters *counters = cache[label]->counters;
  uint64_t val = counters->get(indx);
  return val;
}

void PerfCountersCache::set(std::string label, int indx, uint64_t v) {
  if(!label_exists(label)) {
    std::cout << "label does not exist in cache" << std::endl;
    return;
  }

  PerfCounters *counters = cache[label]->counters;
  counters->set(indx, v);

  update_labels_list(label);
}

void PerfCountersCache::rgw_metrics_perf_stop() {
 for(auto it = cache.begin(); it != cache.end(); ++it ) {
   ceph_assert(it->second->counters);
   cct->get_perfcounters_collection()->remove(it->second->counters);
   delete it->second->counters;
 }
}
