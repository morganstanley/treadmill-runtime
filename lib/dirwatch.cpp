#include <treadmill/dirwatch.h>

namespace treadmill {

void DirWatch::close_cb(uv_fs_t *req) {
  auto *dw = reinterpret_cast<DirWatch *>(req->data);

  int result = req->result;
  if (result != 0) {
    SPDLOG_ERROR("close: {} - {}", result, uv_strerror(result));
  }

  uv_fs_req_cleanup(req);
  auto *req_ex = reinterpret_cast<DirWatch::UvFsReq *>(req);
  dw->free_fsreq(req_ex);
}

void DirWatch::open_cb(uv_fs_t *req) {
  auto *dw = reinterpret_cast<DirWatch *>(req->data);

  int result = req->result;

  uv_fs_req_cleanup(req);
  auto *req_ex = reinterpret_cast<DirWatch::UvFsReq *>(req);

  if (result >= 0) {
    req_ex->file = result;
    uv_fs_read(dw->loop_, req, req_ex->file, &req_ex->buf, 1, -1,
               DirWatch::read_cb);
  } else {
    if (result != -ENOENT) {
      SPDLOG_ERROR("open: {} - {}", result, uv_strerror(result));
    } else {
      if (dw->on_deleted) {
        dw->on_deleted.value()(req_ex->filename);
      }
    }
    dw->free_fsreq(req_ex);
  }
}

void DirWatch::read_cb(uv_fs_t *req) {
  auto *dw = reinterpret_cast<DirWatch *>(req->data);

  int result = req->result;

  uv_fs_req_cleanup(req);
  auto *req_ex = reinterpret_cast<DirWatch::UvFsReq *>(req);

  if (result < 0) {
    SPDLOG_ERROR("read: {} - {}", result, uv_strerror(result));
    uv_fs_close(dw->loop_, req, result, DirWatch::close_cb);
  } else {
    req_ex->str.write(req_ex->buf.base, result);

    if (result == 0) {
      if (dw->on_created) {
        dw->on_created.value()(req_ex->filename, req_ex->str);
      }
      uv_fs_close(dw->loop_, req, req_ex->file, DirWatch::close_cb);
    } else {
      uv_fs_read(dw->loop_, req, req_ex->file, &req_ex->buf, 1, -1,
                 DirWatch::read_cb);
    }
  }
}

void DirWatch::scandir_cb(uv_fs_t *req) {
  auto *dw = reinterpret_cast<DirWatch *>(req->data);

  uv_dirent_t ent;
  for (;;) {
    auto err = uv_fs_scandir_next(req, &ent);
    if (err == UV_EOF) {
      break;
    }
    if (err != 0) {
      SPDLOG_ERROR("scandir_next: {}", err);
      break;
    }
    if (ent.type != UV_DIRENT_FILE) {
      continue;
    }

    auto *req = dw->alloc_fsreq(ent.name);

    std::filesystem::path path = dw->dir_ / ent.name;
    uv_fs_open(dw->loop_, req, path.c_str(), O_RDONLY, S_IRUSR,
               DirWatch::open_cb);
  }
  uv_fs_req_cleanup(req);
}

void DirWatch::on_event(uv_fs_event_t *handle, const char *filename, int events,
                        int status) {
  auto *dw = reinterpret_cast<DirWatch *>(handle->data);

  if (filename[0] == '.') {
    return;
  }

  if (events != UV_RENAME) {
    return;
  }

  auto *req = dw->alloc_fsreq(filename);
  std::filesystem::path path = dw->dir_ / filename;
  uv_fs_open(dw->loop_, req, path.c_str(), O_RDONLY, S_IRUSR, open_cb);
}

void DirWatch::start() {
  fs_scan_.data = this;
  uv_fs_scandir(loop_, &fs_scan_, dir_.c_str(), 0, DirWatch::scandir_cb);

  fs_event_.data = this;
  uv_fs_event_init(loop_, &fs_event_);
  uv_fs_event_start(&fs_event_, DirWatch::on_event, dir_.c_str(),
                    UV_FS_EVENT_WATCH_ENTRY | UV_FS_EVENT_STAT);
}

} // namespace treadmill
