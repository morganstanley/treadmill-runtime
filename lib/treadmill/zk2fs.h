#ifndef TREADMILL_ZK2FS_H
#define TREADMILL_ZK2FS_H

#include <treadmill/counterguard.h>
#include <treadmill/zkclient.h>

#include <filesystem>
#include <iostream>
#include <mutex>

namespace treadmill {

class Zk2Fs {
public:
  enum SyncOpts {
    Skip = 0,
    Watch = 1 << 0,
    File = 1 << 1,
    Dir = 1 << 2,
    Compressed = 1 << 3,
    Empty = 1 << 4
  };

  typedef std::function<int(const std::string &)> Topology;

  explicit Zk2Fs(const std::filesystem::path &fsroot);
  Zk2Fs(const std::filesystem::path &fsroot,
        const std::optional<Topology> &topology);
  Zk2Fs(const Zk2Fs &) = delete;

  void init();
  void use(ZkClient *zkclient) { zkclient_ = zkclient; }

  template <class T> void synchronize(const T &paths, bool once) {
    invalidate(fsroot_);
    if (!paths.size()) {
      return;
    }

    sync(paths);
  }

private:
  template <class T> void sync(const T &paths) {
    for (const auto &p : paths) {
      auto opts = sync_opts(p);
      if (opts & SyncOpts::Dir) {
        invalidate(p);
        sync_children(p, opts & SyncOpts::Watch);
      } else if (opts & SyncOpts::File) {
        sync_data(p, opts & SyncOpts::Watch);
      } else {
        ;
      }
    }
  }

  void sync_data(const std::string &zkpath, bool watch);
  void sync_compressed_data(const std::string &zkpath, bool watch);
  void sync_children(const std::string &zkpath, bool watch);

  template <typename... Args> void watch_children(Args &&... args) {
    ++counter_;
    return zkclient_->watch_children(std::forward<Args>(args)...);
  }

  template <typename... Args> void watch_data(Args &&... args) {
    ++counter_;
    return zkclient_->watch_data(std::forward<Args>(args)...);
  }

  void invalidate(const std::string &zkpath) const;

  void mark_ready(const std::filesystem::path &path) const;

  void check_ready();

  std::filesystem::path target(const std::string &zkpath) const {
    return fsroot_ / zkpath.substr(1);
  }

  std::filesystem::path target(const std::string_view &zkpath) const {
    return fsroot_ / zkpath.substr(1);
  }

  bool sync_file(int rc, const ZkClient::Path &path, const ZkClient::Data &data,
                 const ZkClient::Stat &stat, bool compressed);

  bool sync_file_compressed(int rc, const ZkClient::Path &path,
                            const ZkClient::Data &data,
                            const ZkClient::Stat &stat);

  bool sync_file_uncompressed(int rc, const ZkClient::Path &path,
                              const ZkClient::Data &data,
                              const ZkClient::Stat &stat);

  bool sync_dir_clbk(int rc, const ZkClient::Path &path,
                     const ZkClient::List &nodes);

  int sync_opts(const std::string &path) const;

  void mark_modified() const;

  std::filesystem::path fsroot_;
  std::filesystem::path tmpdir_;
  ZkClient *zkclient_;
  std::optional<Topology> topology_;

  ZkClient::DataWatch data_watch_;
  ZkClient::DataWatch compressed_data_watch_;
  ZkClient::ChildrenWatch children_watch_;

  bool ready_;
  std::atomic_long counter_;

  struct ReadyGuard {
    ReadyGuard(Zk2Fs *owner) : owner_(owner), ready_(owner->ready_) {
      if (!ready_) {
        owner_->check_ready();
      }
    }
    ~ReadyGuard() {
      if (!ready_ && owner_->ready_) {
        // TODO: To implement "once" semantics, this is the place to call
        //       "on_ready" callback. Watches need to be disabled when running
        //       in "once" mode.
        SPDLOG_INFO("Ready.");
        owner_->mark_modified();
        owner_->mark_ready(owner_->fsroot_);
      }
    }

    Zk2Fs *owner_;
    bool ready_;
  };
};

} // namespace treadmill

#endif // TREADMILL_ZK2FS_H
