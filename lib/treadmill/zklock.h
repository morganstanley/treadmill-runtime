#pragma once

#include <treadmill/zkclient.h>

#include <memory>

namespace treadmill {

class ZkLock {
public:
  typedef std::function<void(void)> LockClbk;

  ZkLock(ZkClient *zk, const std::string &path, LockClbk *clbk);
  void lock();

private:
  void on_create(int rc, const ZkClient::Data &path);
  bool on_children(int rc, const ZkClient::Path &path,
                   const ZkClient::List &nodes);
  bool on_exists(int rc, const ZkClient::Path &path,
                 const ZkClient::Stat &stat);

  ZkClient *zk_;
  std::string path_;
  LockClbk *clbk_;
  std::string self_;

  ZkClient::CreateClbk create_clbk_;
  ZkClient::ChildrenWatch children_watch_;
  ZkClient::ExistsWatch exists_watch_;
};

} // namespace treadmill
