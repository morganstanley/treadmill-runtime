#include <treadmill/fs.h>
#include <treadmill/scopeguard.h>
#include <treadmill/zk2fs.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <system_error>
#include <unistd.h>
#include <utime.h>

#include <zlib.h>

namespace fs = std::filesystem;

namespace treadmill {

inline bool connectionloss(int rc) {
  return false; /* return rc == ZCONNECTIONLOSS; */
}

static int default_topology(const std::string &zkpath) {
  bool is_toplevel = (zkpath.rfind('/') == 0);
  return is_toplevel ? Zk2Fs::SyncOpts::Dir : Zk2Fs::SyncOpts::File;
}

Zk2Fs::Zk2Fs(const std::filesystem::path &fsroot)
    : Zk2Fs(fsroot, std::nullopt) {}

Zk2Fs::Zk2Fs(const std::filesystem::path &fsroot,
             const std::optional<Topology> &topology)
    : fsroot_(fsroot), tmpdir_(fsroot / ".tmp"), zkclient_(nullptr),
      topology_(topology),
      data_watch_([this](auto &&... args) -> decltype(auto) {
        return sync_file_uncompressed(std::forward<decltype(args)>(args)...);
      }),
      compressed_data_watch_([this](auto &&... args) -> decltype(auto) {
        return sync_file_compressed(std::forward<decltype(args)>(args)...);
      }),
      children_watch_([this](auto &&... args) -> decltype(auto) {
        return sync_dir_clbk(std::forward<decltype(args)>(args)...);
      }),
      ready_(false), counter_(0) {
  fs::create_directories(tmpdir_);
}

bool Zk2Fs::sync_file_uncompressed(int rc, const ZkClient::Path &path,
                                   const ZkClient::Data &data,
                                   const ZkClient::Stat &stat) {
  return sync_file(rc, path, data, stat, false);
}

bool Zk2Fs::sync_file_compressed(int rc, const ZkClient::Path &path,
                                 const ZkClient::Data &data,
                                 const ZkClient::Stat &stat) {
  return sync_file(rc, path, data, stat, true);
}

bool Zk2Fs::sync_file(int rc, const ZkClient::Path &path,
                      const ZkClient::Data &data, const ZkClient::Stat &stat,
                      bool compressed) {
  ReadyGuard g(this);

  if (!(rc == ZOK || rc == ZNONODE)) {
    SPDLOG_INFO("Ignoring rc: {}", rc);
    return false;
  }

  auto fspath = target(path);
  std::error_code err;
  if (fs::is_directory(fspath, err)) {
    fspath /= ".data";
  }

  if (!data) {
    CHECK(rc == ZNONODE);
    // it is safe to call fs::remove on non-existing file.
    SPDLOG_DEBUG("del node: {}", path);
    fs::remove(fspath);
    return false;
  }

  CHECK(stat);
  std::chrono::duration<decltype(stat.value().mtime), std::milli> mtime(
      stat.value().mtime);

  namespace c = std::chrono;
  auto sec = c::duration_cast<c::seconds>(mtime);
  auto nsec = c::duration_cast<c::nanoseconds>(c::duration<long, std::milli>(
      mtime - c::duration_cast<c::milliseconds>(sec)));

  SPDLOG_DEBUG("add node: {}, mtime: [{} {}], compressed: {}", path,
               sec.count(), nsec.count(), compressed);
  if (compressed) {
    for (int i = 1; i < 3; ++i) {
      uLong length = i * 10 * data.value().size();
      std::unique_ptr<Byte[]> uncompressed(new Byte[length]());
      int status =
          uncompress(uncompressed.get(), &length,
                     (const Bytef *)data.value().data(), data.value().size());
      if (status == 0) {
        atomic_write(fspath, tmpdir_, sec.count(), nsec.count(),
                     (const char *)uncompressed.get(), length);
        break;
      }

      if (status == Z_DATA_ERROR) {
        SPDLOG_ERROR("Corrupted data - can't uncompress: {}", path);
        atomic_write(fspath, tmpdir_, sec.count(), nsec.count(),
                     data.value().data(), data.value().size());
        break;
      }
      CHECK(status == Z_BUF_ERROR);
    }

  } else {
    atomic_write(fspath, tmpdir_, sec.count(), nsec.count(),
                 data.value().data(), data.value().size());
  }

  mark_modified();

  return true;
}

int Zk2Fs::sync_opts(const std::string &path) const {
  return topology_ ? topology_.value()(path) : default_topology(path);
}

bool Zk2Fs::sync_dir_clbk(int rc, const ZkClient::Path &path,
                          const ZkClient::List &nodes) {
  ReadyGuard g(this);

  if (!(rc == ZOK || rc == ZNONODE)) {
    SPDLOG_INFO("Ignoring rc: {}", rc);
    return false;
  }

  if (!nodes) {
    CHECK(rc == ZNONODE);
    SPDLOG_INFO("node deleted: {}, rc = {}", path, rc);
    fs::remove_all(target(path));
    return false;
  }

  bool up_to_date = fs::exists(target(path) / ".ready");

  std::map<std::string, int> state;
  for (const auto &node : nodes.value()) {
    state.emplace(node, 1);
  }

  const fs::path fsdir = target(path);
  fs::create_directories(fsdir);

  for (auto &dirent : fs::directory_iterator(fsdir)) {
    std::string filename = dirent.path().filename();
    if (filename[0] == '.') {
      continue;
    }

    auto result = state.emplace(filename, -1);
    // Result of insert is pair of <iterator, bool>.
    //
    // Second item is false if no insert happened (item already exists)
    //
    // in this case, decrement the counter, so that for common items
    // the counter is 0.
    if (!result.second) {
      --(*result.first).second;
    }
  }

  int opts = Zk2Fs::SyncOpts::Skip;
  bool first = true;

  size_t created_count = 0;
  size_t deleted_count = 0;

  for (const auto &item : state) {
    const std::string &name = item.first;

    bool extra = (item.second < 0);
    bool common = (item.second == 0);
    bool missing = (item.second > 0);

    std::string zknode = std::string(path) + "/" + name;
    // TODO: all children nodes share same topology. Consider topolofy
    //        returning a tuple of actions - for "self" and for children.
    if (first) {
      opts = sync_opts(zknode);
      first = false;
    }

    if (missing || (!up_to_date && common)) {
      SPDLOG_DEBUG("add child: {} {}", zknode, int(opts));
      if (opts & SyncOpts::Dir) {
        if (!up_to_date) {
          invalidate(zknode);
        }
        sync_children(zknode, opts & SyncOpts::Watch);
      }

      if (opts & SyncOpts::File) {
        if (opts & SyncOpts::Empty) {
          if (!common) {
            ++created_count;
            atomic_write(fsdir / name, tmpdir_, nullptr, 0);
          }
          mark_modified();
        } else {
          ++created_count;
          if (opts & SyncOpts::Compressed) {
            sync_compressed_data(zknode, opts & SyncOpts::Watch);
          } else {
            sync_data(zknode, opts & SyncOpts::Watch);
          }
        }
      }
    }

    if (extra) {
      SPDLOG_DEBUG("del node: {}", zknode);
      ++deleted_count;
      fs::remove_all(target(path) / name);
    }
  }

  SPDLOG_INFO("Processed {}: +{} -{}", path, created_count, deleted_count);
  if (!up_to_date) {
    mark_ready(target(path));
  }
  return true;
}

void Zk2Fs::init() {}

void Zk2Fs::sync_data(const std::string &zkpath, bool watch) {
  CHECK(zkclient_);

  fs::create_directories(target(zkpath).parent_path());
  watch_data(zkpath, &data_watch_, watch);
}

void Zk2Fs::sync_compressed_data(const std::string &zkpath, bool watch) {
  CHECK(zkclient_);

  fs::create_directories(target(zkpath).parent_path());
  watch_data(zkpath, &compressed_data_watch_, watch);
}

void Zk2Fs::sync_children(const std::string &zkpath, bool watch) {
  CHECK(zkclient_);

  SPDLOG_DEBUG("sync children: {}", zkpath);
  fs::create_directories(target(zkpath));
  watch_children(zkpath, &children_watch_, watch);
}

void Zk2Fs::invalidate(const std::string &zkpath) const {
  fs::remove(target(zkpath) / ".ready");
}

void Zk2Fs::mark_ready(const std::filesystem::path &path) const {
  atomic_write(path / ".ready", tmpdir_, nullptr, 0);
}

void Zk2Fs::check_ready() {
  if (ready_) {
    return;
  }

  long prev = counter_.fetch_sub(1);
  if (prev == 1) {
    mark_modified();
    mark_ready(fsroot_);
    ready_ = true;
  }
}

void Zk2Fs::mark_modified() const {
  auto modified_file = fsroot_ / ".modified";

  struct stat fstat;
  time_t mtime;
  struct utimbuf new_times;

  int rc = stat(modified_file.c_str(), &fstat);
  if (rc == 0) {
    mtime = fstat.st_mtime;            /* seconds since the epoch */
    new_times.actime = fstat.st_atime; /* keep atime unchanged */
    new_times.modtime = time(NULL);    /* set mtime to current time */
    rc = utime(modified_file.c_str(), &new_times);
  }
  if (rc != 0) {
    if (errno == ENOENT) {
      atomic_write(modified_file, tmpdir_, nullptr, 0);
    } else {
      SPDLOG_ERROR("Unable to update .modified file: {}", errno);
    }
  }
}

} // namespace treadmill
