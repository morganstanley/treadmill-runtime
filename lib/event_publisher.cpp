#include <treadmill/event_publisher.h>

#include <limits.h>
#include <stdio.h>
#include <unistd.h>
#include <uv.h>

#include <regex>

namespace treadmill {

static constexpr long num_of_appev_shards = 256;

static std::regex event_regex("^([^,]+),([^,]+#\\d+),([^,]+),([^,]+)$");

std::optional<std::string> app_event_zkpath(const std::string &event,
                                            const std::string &hostname) {
  std::smatch match;
  if (std::regex_match(event, match, event_regex)) {
    std::string when(match[1].str());
    std::string instanceid(match[2].str());
    std::string event_type(match[3].str());
    std::string event_data(match[4].str());

    long shard = atol(instanceid.substr(instanceid.find("#") + 1).c_str()) %
                 num_of_appev_shards;
    return fmt::format("/trace/{:04X}/{},{},{},{},{}", shard, instanceid, when,
                       hostname, event_type, event_data);
  }

  return std::nullopt;
}

EventPublisher::EventPublisher(const std::string &spool_dir, ZkClient *zkclient)
    : spool_dir_(spool_dir), zkclient_(zkclient),
      commit_clbk_([this](auto &&... args) -> decltype(auto) {
        return commit(std::forward<decltype(args)>(args)...);
      }),
      event_clbk_([this](auto &&... args) -> decltype(auto) {
        return on_event(std::forward<decltype(args)>(args)...);
      }) {
  char hostname[HOST_NAME_MAX];
  gethostname(hostname, HOST_NAME_MAX);
  hostname_ = hostname;
}

void EventPublisher::commit(int rc, const ZkClient::Data &path) {
  std::unique_lock<std::mutex> lck(transactions_mx_);
  const auto &event = transactions_.front();
  if (rc == ZOK || rc == ZNODEEXISTS) {
    auto fullpath = spool_dir_ / event;
    int rc = unlink(fullpath.c_str());
    SPDLOG_INFO("committed: {}, rc: {}", event, rc);
    CHECK_LOG(rc == 0, "Error deleting {}, errno: {}", fullpath.c_str(), errno);
  } else {
    SPDLOG_ERROR("failed to commit: {}", event);
  }
  transactions_.pop_front();
}

void EventPublisher::on_event(const std::string &name,
                              const std::stringstream &data) {
  auto zkpath = app_event_zkpath(name, hostname_);
  if (zkpath) {
    std::unique_lock<std::mutex> lck(transactions_mx_);

    SPDLOG_INFO("app-event: {}", zkpath.value());
    auto payload = data.str();

    transactions_.push_back(name);
    zkclient_->create(zkpath.value(), payload.c_str(), payload.size(), 0,
                      &commit_clbk_);
    // TODO: process app terminal events - killer/abored/terminated.
    //       on such event:
    //       a) create "finished" node with payload on server and reason
    //       b) delete "scheduled" node.
  } else {
    SPDLOG_WARN("unrecognized app-event: {}", name);
  }
}

} // namespace treadmill
