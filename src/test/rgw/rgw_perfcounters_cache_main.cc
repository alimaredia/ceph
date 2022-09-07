#include "rgw_perfcounters_cache.h"
#include "rgw_perfcounters_cache_request.h"
#include <boost/intrusive/list.hpp>
//#include "common/intrusive_lru.h"

class Label : public boost::intrusive::list_base_hook<>
{
  public:
    std::string val;
    Label(std::string _label) :  val{std::move(_label)}  {}
    //Label(std::string _label) :  val(_label)  {}
};


typedef boost::intrusive::list<Label> LabelsList;

class ListWrapper {
  private:
    LabelsList ll;

  public:
    void add_label(Label l1) {
      ll.push_front(l1);
    }

    void print_labels() {
      for(LabelsList::iterator it = ll.begin(); it != ll.end(); ++it) {
        //std::cout << (*it).val << std::endl;
        std::cout << "hello world" << std::endl;
      }
    }

    /*
    ~ListWrapper() {
      for(LabelsList::iterator it = ll.begin(); it != ll.end(); ++it) {
        ll.erase(it);
      }
    }
    */

};

/*
void add_label(LabelsList ll) {
  Label l1("label1");
  ll.push_front(l1);
}

*/


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

  std::cout << "Intrusive play around" << std::endl;
  Label *l1 = new Label{"label1"};
  // label must be declared before the list so that it can be destructed by the destructor AFTER the list is destructed
  LabelsList *ll = new LabelsList;
  ll->push_back(*l1);

  for (const Label &l : *ll)
    std::cout << l.val << std::endl;

  Label *l2 = new Label{"label2"};
  LabelsList *ll2 = new LabelsList;
  //ll2->push_back(*l1);
  ll2->push_back(*l2);

  for (const Label &l : *ll2)
    std::cout << l.val << std::endl;

  delete ll;
  delete ll2;
  delete l1;
  delete l2;


  /*
  //Label l1 = new Label("label1");
  //std::string l2 = "label2";
  //add_label(ll);
  //LabelsList::iterator it = ll.begin();
  ListWrapper *lw = new ListWrapper;
  lw->add_label(l1);
  //lw->add_label(l2);
  //lw->print_labels();
  delete lw;
  */

  return 0;
}
