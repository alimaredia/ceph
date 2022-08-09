#include "rgw_perfcounters_cache_request.h"
#include <random>
#include <iostream>

std::vector<Request> gen_requests(int num_requests, int num_buckets, int num_users, int min_val, int max_val) {
  std::vector<Request> requests;

  std::random_device rd_buckets; // obtain a random number from hardware
  std::mt19937 gen_buckets(rd_buckets()); // seed the generator
  std::uniform_int_distribution<> distr_buckets(1, num_buckets); // define the range

  std::random_device rd_users;
  std::mt19937 gen_users(rd_users());
  std::uniform_int_distribution<> distr_users(1, num_users);
                                                 
  std::random_device rd_counters;
  std::mt19937 gen_counters(rd_counters());
  std::uniform_int_distribution<> distr_counters(l_rgw_metrics_first+1, l_rgw_metrics_last-1);

  std::random_device rd_vals;
  std::mt19937 gen_vals(rd_vals());
  std::uniform_int_distribution<> distr_vals(min_val, max_val);

  std::random_device rd_ops;
  std::mt19937 gen_ops(rd_ops());
  std::uniform_int_distribution<> distr_ops(1, 3);

  for(int i = 0; i < num_requests; ++i) {
    int user_num = distr_users(gen_users);
    std::string user_str = std::to_string(user_num);

    int bucket_num = distr_buckets(gen_buckets);
    std::string bucket_str = std::to_string(bucket_num);

    std::string label = "rgw::user=U" + user_str + "bucket=B" + bucket_str;

    int counter_num = distr_counters(gen_counters);
    RGWCounters counter = static_cast<RGWCounters>(counter_num);

    int val = 0;
    if(counter == l_rgw_req || counter == l_rgw_failed_req) {
      val = 1;
    } else {
      val = distr_vals(gen_vals);
    }

    int op = distr_ops(gen_ops);
    //std::cout << label << " counter=" << counter << " val=" << val << " op=" << op << std::endl;

    Request r(label, counter, val, op);
    requests.push_back(r);
  }

  return requests;
}

void process_requests(PerfCountersCache *p, std::vector<Request> requests) {
  for(unsigned i = 0; i < requests.size(); i++) {
    p->add_label(requests[i].label);
    if(requests[i].op == 1) {
      p->set(requests[i].label, requests[i].counter, requests[i].val);
    } else if(requests[i].op == 2) {
      p->dec(requests[i].label, requests[i].counter, requests[i].val);
    } else {
      p->inc(requests[i].label, requests[i].counter, requests[i].val);
    }
  }
}
