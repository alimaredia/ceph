// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive/list.hpp>

namespace ceph::common {

/**
 * intrusive_lru: lru implementation with embedded map and list hook
 *
 * Note, this implementation currently is entirely thread-unsafe.
 */

template <typename K, typename V, typename VToK>
struct intrusive_lru_config {
  using key_type = K;
  using value_type = V;
  using key_of_value = VToK;
};

template <typename Config>
class intrusive_lru;

template <typename Config>
class intrusive_lru_base;

template <typename Config>
void intrusive_ptr_add_ref(intrusive_lru_base<Config> *p);

template <typename Config>
void intrusive_ptr_release(intrusive_lru_base<Config> *p);


template <typename Config>
class intrusive_lru_base {
  unsigned use_count = 0;

  // null if unreferenced
  intrusive_lru<Config> *lru = nullptr;

public:
  boost::intrusive::set_member_hook<> set_hook;
  boost::intrusive::list_member_hook<> list_hook;

  using Ref = boost::intrusive_ptr<typename Config::value_type>;
  using lru_t = intrusive_lru<Config>;

  friend intrusive_lru<Config>;
  friend void intrusive_ptr_add_ref<>(intrusive_lru_base<Config> *);
  friend void intrusive_ptr_release<>(intrusive_lru_base<Config> *);

  virtual ~intrusive_lru_base() {}
};

template <typename Config>
class intrusive_lru {
  using base_t = intrusive_lru_base<Config>;
  using K = typename Config::key_type;
  using T = typename Config::value_type;
  using TRef = typename base_t::Ref;

  using lru_set_option_t = boost::intrusive::member_hook<
    base_t,
    boost::intrusive::set_member_hook<>,
    &base_t::set_hook>;

  using VToK = typename Config::key_of_value;
  struct VToKWrapped {
    using type = typename VToK::type;
    const type &operator()(const base_t &obc) {
      return VToK()(static_cast<const T&>(obc));
    }
  };
  using lru_set_t = boost::intrusive::set<
    base_t,
    lru_set_option_t,
    boost::intrusive::key_of_value<VToKWrapped>
    >;
  lru_set_t lru_set;

  using lru_list_t = boost::intrusive::list<
    base_t,
    boost::intrusive::member_hook<
      base_t,
      boost::intrusive::list_member_hook<>,
      &base_t::list_hook>>;
  lru_list_t unreferenced_list;

  size_t lru_target_size = 0;

  void evict() {
    std::cout << "first bool is: " << !unreferenced_list.empty() << std::endl;
    std::cout << "second bool is: " << (lru_set.size() > lru_target_size) << std::endl;
    std::cout << "lru_set.size() is: " << lru_set.size() << std::endl;
    while (!unreferenced_list.empty() &&
	   lru_set.size() > lru_target_size) {
      auto &b = unreferenced_list.front();
      assert(!b.lru);
      unreferenced_list.pop_front();
      lru_set.erase_and_dispose(
	lru_set.iterator_to(b),
	[](auto *p) { delete p; }
      );
      std::cout << "Made it into instrusive_lru::evict while loop, delete called" << std::endl;
    }
  }

  void access(base_t &b) {
    if (b.lru)
      return;
    unreferenced_list.erase(lru_list_t::s_iterator_to(b));
    b.lru = this;
  }

  void insert(base_t &b) {
    assert(!b.lru);
    lru_set.insert(b);
    b.lru = this;
    std::cout << "evict() called in insert" << std::endl;
    evict();
  }

  void unreferenced(base_t &b) {
    assert(b.lru);
    unreferenced_list.push_back(b);
    b.lru = nullptr;
    std::cout << "evict() called in unreferenced" << std::endl;
    evict();
  }

public:
  /**
   * Returns the TRef corresponding to k if it exists or
   * creates it otherwise.  Return is:
   * std::pair(reference_to_val, found)
   */
  std::pair<TRef, bool> get_or_create(const K &k) {
    typename lru_set_t::insert_commit_data icd;
    auto [iter, missing] = lru_set.insert_check(
      k,
      icd);
    if (missing) {
      std::cout << "missing is true" << std::endl;
      auto ret = new T(k);
      lru_set.insert_commit(*ret, icd);
      insert(*ret);
      return {TRef(ret), false};
    } else {
      std::cout << "missing is false" << std::endl;
      access(*iter);
      return {TRef(static_cast<T*>(&*iter)), true};
    }
  }

  /**
   * Returns the TRef corresponding to k if it exists or
   * nullptr otherwise.
   */
  TRef get(const K &k) {
    if (auto iter = lru_set.find(k); iter != std::end(lru_set)) {
      access(*iter);
      return TRef(static_cast<T*>(&*iter));
    } else {
      return nullptr;
    }
  }

  void set_target_size(size_t target_size) {
    lru_target_size = target_size;
    std::cout << "evict() called in set_target_size" << std::endl;
    evict();
  }

  ~intrusive_lru() {
    std::cout << "\ndestructor for intrusive_lru called" << std::endl;
    lru_target_size = 0;
    while (!unreferenced_list.empty()) {
      evict();
    }
  }

  friend void intrusive_ptr_add_ref<>(intrusive_lru_base<Config> *);
  friend void intrusive_ptr_release<>(intrusive_lru_base<Config> *);
};

template <typename Config>
void intrusive_ptr_add_ref(intrusive_lru_base<Config> *p) {
  std::cout << "intrusive_ptr_add_ref called" << std::endl;
  assert(p);
  assert(p->lru);
  p->use_count++;
}

template <typename Config>
void intrusive_ptr_release(intrusive_lru_base<Config> *p) {
  std::cout << "intrusive_ptr_release called" << std::endl;
  assert(p);
  assert(p->use_count > 0);
  --p->use_count;
  if (p->use_count == 0) {
    std::cout << "intrusive_ptr_release p->user_count is 0" << std::endl;
    p->lru->unreferenced(*p);
  }
}


}
