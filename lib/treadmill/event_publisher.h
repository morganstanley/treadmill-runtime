#pragma once

#include <treadmill/dirwatch.h>
#include <treadmill/zkclient.h>

#include <deque>
#include <filesystem>
#include <mutex>

namespace treadmill {

class EventPublisher {
public:
  EventPublisher(const std::string &spool_dir, ZkClient *zkclient);
  void on_event(const std::string &name, const std::stringstream &data);

private:
  void commit(int rc, const ZkClient::Data &path);

  std::filesystem::path spool_dir_;
  ZkClient *zkclient_;
  std::string hostname_;
  std::deque<std::string> transactions_;
  std::mutex transactions_mx_;
  ZkClient::CreateClbk commit_clbk_;
  DirWatch::created_clbk_t event_clbk_;
};

std::optional<std::string> app_event_zkpath(const std::string &event,
                                            const std::string &hostname);

} // namespace treadmill
