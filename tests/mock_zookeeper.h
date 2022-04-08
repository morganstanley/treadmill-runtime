#pragma once

#include <atomic>
#include <filesystem>
#include <memory>
#include <sstream>
#include <unordered_map>
#include <utility>

#include <stdlib.h>

#include <zookeeper.h>

#include <treadmill/zkclient.h>

namespace treadmill {
namespace test {

struct ZMock {

  typedef std::pair<watcher_fn, void *> watch_t;
  typedef std::unordered_map<std::string, watch_t> watch_map_t;

  struct Watches {
    std::mutex mx;
    watch_map_t watches;

    std::optional<watch_t> get(const std::string &path) {
      std::unique_lock<std::mutex> lck(mx);
      auto it = watches.find(path);
      if (it == watches.end()) {
        return std::nullopt;
      }

      auto watch = (*it).second;
      watches.erase(path);
      return watch;
    }

    bool put(const std::string &path, watcher_fn watcher, void *ctx) {
      std::unique_lock<std::mutex> lck(mx);
      auto result = watches.emplace(path, std::make_pair(watcher, ctx));
      return result.second;
    }
  };

  std::filesystem::path fsroot;
  void *zoo_context;

  std::mutex completions_mx;
  std::vector<std::thread> completions;

  Watches children_watches;
  Watches data_watches;
  Watches exists_watches;

  ZMock(void *context) : zoo_context(context) {
    char tmpdir_t[32];
    snprintf(tmpdir_t, sizeof(tmpdir_t), "/tmp/zookeeperXXXXXX");
    char *tmpdir = mkdtemp(tmpdir_t);
    CHECK(tmpdir);
    fsroot = tmpdir;
  }
  ZMock(const ZMock &) = delete;
  ZMock(ZMock &&) = delete;
  ZMock &operator=(ZMock &) = delete;
  ZMock() {}
  virtual ~ZMock() {
    join();
    std::filesystem::remove_all(fsroot);
  }

  void fire(const std::string &path, int watch_type, Watches &watches);
  void fire_children_ev(const std::string &path) {
    fire(path, ZWATCHTYPE_CHILD, children_watches);
  }
  void fire_data_ev(const std::string &path) {
    fire(path, ZWATCHTYPE_DATA, data_watches);
  }
  void fire_exists_ev(const std::string &path) {
    fire(path, ZWATCHTYPE_DATA, exists_watches);
  }

  void join();

  virtual const void *get_context();
  virtual zhandle_t *init2(const char *host, watcher_fn fn, int recv_timeout,
                           const clientid_t *clientid, void *context, int flags,
                           log_callback_fn log_callback);
  virtual int close();
  const char *get_current_server();
  int add_auth(const char *scheme, const char *cert, int certLen,
               void_completion_t completion, const void *data);
  virtual int awget_children2(const char *path, watcher_fn watcher,
                              void *watcherCtx,
                              strings_stat_completion_t completion,
                              const void *data);
  virtual int awget(const char *path, watcher_fn watcher, void *watcherCtx,
                    data_completion_t completion, const void *data);

  // These are syncronous no watch calls, they are never used in zkclient, so
  // it is safe to use them in tests to create necessary data.
  virtual int create(const char *path, const char *value, int valuelen,
                     const struct ACL_vector *acl, int flags, char *path_buffer,
                     int path_buffer_len);

  virtual int set(const char *path, const char *buffer, int buflen,
                  int version);
  virtual int adelete(const char *path, int version,
                      void_completion_t completion, const void *data);
  virtual int aset(const char *path, const char *buf, int len, int version,
                   stat_completion_t completion, const void *data);
  virtual int acreate(const char *path, const char *value, int valuelen,
                      const struct ACL_vector *acl, int flags,
                      string_completion_t completion, const void *data);

  static inline ZMock *mock(zhandle_t *zh) {
    return reinterpret_cast<ZMock *>(zh);
  }

  static inline zhandle_t *zh(ZkClient &client) {
    return const_cast<zhandle_t *>(client.zh_);
  }
};

} // namespace test
} // namespace treadmill
