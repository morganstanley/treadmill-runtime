#pragma once

#include <atomic>
#include <condition_variable>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <variant>

#include <zookeeper.h>

#include <treadmill/logging.h>

namespace treadmill {

#ifdef UNIT_TEST
namespace test {
struct ZMock;
}
#endif

class StringVector {
public:
  StringVector()
      : strings_{0, nullptr}, guard_(std::shared_ptr<struct String_vector>(
                                  &strings_, ::deallocate_String_vector)) {}
  explicit StringVector(const struct String_vector *strings)
      : strings_(*strings) {}
  StringVector(const StringVector &) = default;
  StringVector(StringVector &&) = default;
  StringVector &operator=(const StringVector &) = default;
  ~StringVector() = default;

  int size() const { return strings_.count; }

  struct iterator {
    iterator(const StringVector *owner_, int idx_) : owner(owner_), idx(idx_) {}
    bool operator==(const iterator &rhs) const {
      return owner == rhs.owner && idx == rhs.idx;
    }
    bool operator!=(const iterator &rhs) const { return !(*this == rhs); }
    iterator &operator++() {
      CHECK(owner);
      ++idx;
      return *this;
    }
    iterator &operator--() {
      CHECK(owner);
      --idx;
      return *this;
    }
    const char *operator*() const {
      CHECK(owner);
      CHECK(owner->strings_.data);
      CHECK(idx < owner->strings_.count);
      return owner->strings_.data[idx];
    }

    const StringVector *owner;
    int idx;
  };

  iterator begin() const {
    CHECK(strings_.data);
    return iterator(this, 0);
  }
  iterator end() const { return iterator(this, strings_.count); }

  std::vector<std::string> sorted() const {
    // TODO: possible optimization given that children list mostly sorted:
    //       - maintain list in sorted order while inserting elements. simple
    //         compare with the last element is enough.
    std::vector<std::string> result;
    result.reserve(size());
    for (int i = 0; i < size(); ++i) {
      result.push_back(strings_.data[i]);
    }
    std::sort(result.begin(), result.end());
    return result;
  }

  struct String_vector strings_;
  std::shared_ptr<struct String_vector> guard_;
};

typedef std::function<bool(const std::string &)> match_fn_t;
typedef std::vector<struct ACL> acls_t;
typedef std::pair<match_fn_t, acls_t> permission_match_t;
typedef std::vector<permission_match_t> permissions_t;

inline match_fn_t child_of(const std::string &parent) {
  auto len = parent.size();
  return [parent, len](const std::string &path) {
    return (strncmp(parent.c_str(), path.c_str(), len) == 0) &&
           (path[len] == 0 || path[len] == '/');
  };
}

struct CreateOp {
  std::string path;
  std::string data;
  int flags;
};

struct SetOp {
  std::string path;
  std::string data;
  int version;
};

struct DelOp {
  std::string path;
  int version;
};

typedef std::variant<CreateOp, SetOp, DelOp> MultiOp;

struct CreateOpResult {
  std::optional<std::string> path;
};

struct SetOpResult {
  std::optional<struct Stat> stat;
};

struct DelOpResult {};

struct MultiOpResult {
  int rc;
  std::string path;
  std::variant<CreateOpResult, SetOpResult, DelOpResult> result;
};

class ZkClient {
public:
  ZkClient();
  ZkClient(const ZkClient &) = delete;

  explicit ZkClient(zhandle_t *zh);
  ZkClient(ZkClient &&src);
  ~ZkClient();

  typedef std::function<void(int, int)> ConnectionWatch;
  typedef std::function<void(zhandle_t *)> ConnectionAuthClbk;

  bool connect(
      const std::string &connstr, unsigned int timeout_sec,
      const std::optional<ConnectionWatch> &connection_watch = std::nullopt);

  std::string authscheme() const { return authscheme_; }

  void disconnect();
  void permissions(const permissions_t &permissions) {
    permissions_ = permissions;
  }

  typedef struct Stat stat_t;
  typedef std::optional<const StringVector> List;
  typedef std::optional<const std::string_view> Data;
  typedef std::string_view Path;
  typedef std::optional<stat_t> Stat;

  typedef std::function<bool(int, const Path &, const List &)> ChildrenWatch;

  typedef std::function<bool(int, const Path &, const Data &, const Stat &)>
      DataWatch;

  typedef std::function<bool(int, const Path &, const Stat &)> ExistsWatch;
  typedef std::function<void(int)> DeleteClbk;
  typedef std::function<void(int, const Stat &)> SetClbk;
  typedef std::function<void(int, const Data &)> CreateClbk;
  typedef std::function<void(int, const std::vector<MultiOpResult> &)>
      MultiOpClbk;

  void watch_children(const std::string &path, ChildrenWatch *watcher,
                      bool watch, std::promise<void> *done = nullptr);
  void watch_data(const std::string &path, DataWatch *watcher, bool watch,
                  std::promise<void> *done = nullptr);
  void watch_exists(const std::string &path, ExistsWatch *watcher, bool watch,
                    std::promise<void> *done = nullptr);

  void del(const std::string &path, const DeleteClbk *clbk) {
    del(path, -1, clbk);
  }
  void del(const std::string &path, int version, const DeleteClbk *clbk);

  void set(const std::string &path, const char *data, size_t data_len,
           const SetClbk *clbk) {
    set(path, -1, data, data_len, clbk);
  }
  void set(const std::string &path, int version, const char *data,
           size_t data_len, const SetClbk *clbk);

  void create(const std::string &path, const char *data, size_t data_len,
              int flags, const CreateClbk *clbk);

  void multi(const std::vector<MultiOp> &ops, const MultiOpClbk *clbk);

  template <class T> std::optional<T> get_children(const std::string &path) {
    std::optional<T> result;
    std::promise<void> done;
    ChildrenWatch clbk = [&result](int rc, const Path &path,
                                   const List &children) {
      if (children) {
        T list;
        for (const auto &c : children.value()) {
          list.emplace_back(c);
        }
        result = list;
      }
      return false;
    };

    watch_children(path, &clbk, false, &done);
    done.get_future().wait();
    return result;
  }

  std::optional<std::string> get(const std::string &path) {
    std::optional<std::string> result;
    std::promise<void> done;

    DataWatch clbk = [&result](int rc, const Path &path, const Data &data,
                               const Stat &stat) {
      if (data) {
        result = std::string(data.value());
      }
      return false;
    };

    watch_data(path, &clbk, false, &done);
    done.get_future().wait();
    return result;
  }

  Stat exists(const std::string &path) {
    Stat result;
    std::promise<void> done;

    ExistsWatch clbk = [&result](int rc, const Path &path, const Stat &stat) {
      result = stat;
      return false;
    };

    watch_exists(path, &clbk, false, &done);
    done.get_future().wait();
    return result;
  }

private:
  zhandle_t *zh_;
  std::string connection_str_;
  std::string authscheme_;

  std::atomic<int> state_;
  permissions_t permissions_;

  std::optional<ConnectionWatch> connection_watch_;
  std::optional<ConnectionAuthClbk> auth_clbk_;

  static void children_watcher(zhandle_t *zh, int type, int state,
                               const char *path, void *ctx);
  static void data_watcher(zhandle_t *zh, int type, int state, const char *path,
                           void *ctx);
  static void exists_watcher(zhandle_t *zh, int type, int state,
                             const char *path, void *ctx);
  static void connection_watcher(zhandle_t *zh, int type, int state,
                                 const char *path, void *ctx);

  enum class MultiOpCode { create, set, del };

  struct MultiData {
    MultiData(const MultiOpClbk *clbk, size_t size)
        : clbk_(clbk), ops_(size), results_(size) {}

    const MultiOpClbk *clbk_;
    std::vector<std::pair<MultiOpCode, std::string>> ops_;
    std::vector<zoo_op_result_t> results_;
  };

  template <typename T> struct Async {
    Async() = default;
    Async(zhandle_t *zh_, const std::string &path_, T *watcher_, bool watch_)
        : zh(zh_), path(path_), watcher(watcher_), watch(watch_),
          done(nullptr){};
    Async(zhandle_t *zh_, const std::string &path_, T *watcher_, bool watch_,
          std::promise<void> *done_)
        : zh(zh_), path(path_), watcher(watcher_), watch(watch_), done(done_){};
    zhandle_t *zh;
    std::string path;
    T *watcher;
    bool watch;
    std::promise<void> *done;
  };

  template <typename T> class AsyncCompletions {
  public:
    typedef Async<T> AsyncT;

    AsyncT *make(ZkClient *owner, const std::string &path, T *watcher,
                 bool watch, std::promise<void> *done) {
      std::unique_lock<std::mutex> lck(mx_);
      std::unique_ptr<AsyncT> ptr =
          std::make_unique<AsyncT>(owner->zh_, path, watcher, watch, done);
      auto raw_ptr = ptr.get();
      auto result = comps_.emplace(raw_ptr, std::move(ptr));
      bool success = result.second;
      CHECK(success);
      return raw_ptr;
    }

    void erase(void *comp_ptr) {
      std::unique_lock<std::mutex> lck(mx_);
      comps_.erase(comp_ptr);
    }

    /*
    void erase_by_owner(ZkClient *owner) {
      std::unique_lock<std::mutex> lck(mx_);
      for(auto it = comps_.begin(); it != comps_.end(); ) {
        if (it->second->owner == owner) {
          it = comps_.erase(it);
        } else {
          ++it;
        }
      }
    }*/

  private:
    // tsl::htrie_map<char, std::unique_ptr<ZkClient::Async<T>>> comps_;
    // tsl::array_map<char, std::unique_ptr<ZkClient::Async<T>>> comps_;
    // tsl::robin_map<std::string, std::unique_ptr<ZkClient::Async<T>>> comps_;
    //
    // Default unordered_set implementation.
    std::mutex mx_;
    std::unordered_map<void *, std::unique_ptr<AsyncT>> comps_;
  };

  static AsyncCompletions<DataWatch> data_completions_;
  static AsyncCompletions<ChildrenWatch> children_completions_;
  static AsyncCompletions<ExistsWatch> exists_completions_;

  static void data_completion(int rc, const char *value, int value_len,
                              const stat_t *stat, const void *ctx);
  static void children_stat_completion(int rc,
                                       const struct String_vector *strings,
                                       const stat_t *stat, const void *ctx);
  static void exists_completion(int rc, const stat_t *stat, const void *data);
  static void remove_data_watch_completion(int rc, const void *data);
  static void remove_children_watch_completion(int rc, const void *data);
  static void remove_exists_watch_completion(int rc, const void *data);

  static void delete_clbk(int rc, const void *data);
  static void set_clbk(int rc, const stat_t *stat, const void *data);
  static void create_clbk(int rc, const char *value, const void *data);
  static void multi_clbk(int rc, const void *data);

#ifdef UNIT_TEST
  friend struct treadmill::test::ZMock;
#endif
};

void parse_zk_connstr(const std::string &connstr, std::string &scheme,
                      std::string &hosts, std::string &chroot,
                      std::unordered_map<std::string, std::string> &params);

bool check_zk_connstr(const std::string &connstr);

} // namespace treadmill
