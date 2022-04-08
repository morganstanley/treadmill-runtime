#include <treadmill/zklock.h>

#include <limits.h>

namespace treadmill {

#define _bind_memfn(f)                                                         \
  [this](auto &&... args) -> decltype(auto) {                                  \
    return f(std::forward<decltype(args)>(args)...);                           \
  }

ZkLock::ZkLock(ZkClient *zk, const std::string &path, ZkLock::LockClbk *clbk)
    : zk_(zk), path_(path), clbk_(clbk), create_clbk_(_bind_memfn(on_create)),
      children_watch_(_bind_memfn(on_children)),
      exists_watch_(_bind_memfn(on_exists)) {}

void ZkLock::lock() {
  char hostname[HOST_NAME_MAX];
  int rc = gethostname(hostname, HOST_NAME_MAX);
  CHECK(rc == 0);

  auto data = fmt::format("{}.{}", hostname, getpid());

  SPDLOG_DEBUG("creating lock: {}", path_ + "/lock-");
  zk_->create(path_ + "/lock-", data.c_str(), data.size(),
              ZOO_EPHEMERAL | ZOO_SEQUENCE, &create_clbk_);
}

void ZkLock::on_create(int rc, const ZkClient::Data &path) {
  CHECK_LOG(rc == ZOK, "Unable to create lock, rc = {}", rc);
  CHECK(path);
  self_ = path.value().substr(path_.size() + 1);
  SPDLOG_DEBUG("Created lock: {}", self_);
  zk_->watch_children(path_, &children_watch_, false);
}

bool ZkLock::on_children(int rc, const ZkClient::Path &path,
                         const ZkClient::List &nodes) {
  CHECK(rc == ZOK);
  CHECK(nodes);

  std::string before;
  for (const auto &node : nodes.value()) {
    SPDLOG_DEBUG("Current children: {}", node);
    if (node < self_) {
      if (before.size() == 0 || node > before) {
        before = node;
      }
    }
  }
  if (before.empty()) {
    SPDLOG_DEBUG("Locked.");
    if (clbk_) {
      (*clbk_)();
    }
  } else {
    SPDLOG_DEBUG("Waiting for lock on: {}", before);
    auto watchpath = path_ + "/" + before;
    zk_->watch_exists(watchpath, &exists_watch_, true);
  }
  return false;
}

bool ZkLock::on_exists(int rc, const ZkClient::Path &path,
                       const ZkClient::Stat &stat) {
  SPDLOG_DEBUG("Exists watch: {} {}", path, rc);
  if (rc == ZNONODE) {
    zk_->watch_children(path_, &children_watch_, false);
    return false;
  }

  CHECK(rc == ZOK);
  return true;
}

} // namespace treadmill
