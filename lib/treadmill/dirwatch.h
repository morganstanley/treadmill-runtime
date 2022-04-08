#pragma once

#include <filesystem>
#include <functional>
#include <optional>
#include <sstream>
#include <string>

#include <uv.h>

#include <treadmill/logging.h>

namespace treadmill {

class DirWatch {
public:
  typedef std::function<void(const std::string &, const std::stringstream &)>
      created_clbk_t;
  typedef std::function<void(const std::string &)> deleted_clbk_t;

  DirWatch(uv_loop_t *loop, std::string dir)
      : loop_(loop), dir_(std::move(dir)) {}

  void start();

  std::optional<created_clbk_t> on_created;
  std::optional<deleted_clbk_t> on_deleted;

private:
  uv_loop_t *loop_;
  std::filesystem::path dir_;
  uv_fs_event_t fs_event_;
  uv_fs_t fs_scan_;

  static void open_cb(uv_fs_t *req);
  static void close_cb(uv_fs_t *req);
  static void read_cb(uv_fs_t *req);
  static void scandir_cb(uv_fs_t *req);
  static void on_event(uv_fs_event_t *handle, const char *filename, int events,
                       int status);

  struct UvFsReq : public uv_fs_t {
    constexpr static size_t data_len = 1024;

    explicit UvFsReq(std::string fname) : filename(std::move(fname)), file(-1) {
      buf.base = read_buf;
      buf.len = data_len;
    }

    void reset() {
      file = -1;
      filename.clear();
      std::stringstream temp;
      str.swap(temp);
      memset(buf.base, 0, buf.len);
    }

    std::string filename;
    uv_file file;
    uv_buf_t buf;
    char read_buf[data_len] = {0};
    std::stringstream str;
  };

  auto alloc_fsreq(std::string filename) {
    auto *req = new UvFsReq(std::move(filename));
    req->data = this;
    return req;
  }

  void free_fsreq(UvFsReq *req) { delete req; }
};

} // namespace treadmill
