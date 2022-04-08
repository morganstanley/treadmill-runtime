#include "mock_zookeeper.h"

#include <fstream>
#include <future>
#include <iostream>
#include <sstream>

#include <strings.h>

#include <treadmill/fs.h>

using namespace treadmill::test;
namespace fs = std::filesystem;

extern "C" {

/* zookeeper state constants */
const int ZOO_EXPIRED_SESSION_STATE = -112;
const int ZOO_AUTH_FAILED_STATE = -113;
const int ZOO_CONNECTING_STATE = 1;
const int ZOO_ASSOCIATING_STATE = 2;
const int ZOO_CONNECTED_STATE = 3;
const int ZOO_READONLY_STATE = 5;
const int ZOO_NOTCONNECTED_STATE = 999;

/* zookeeper event type constants */
const int ZOO_CREATED_EVENT = 1;
const int ZOO_DELETED_EVENT = 2;
const int ZOO_CHANGED_EVENT = 3;
const int ZOO_CHILD_EVENT = 4;
const int ZOO_SESSION_EVENT = -1;
const int ZOO_NOTWATCHING_EVENT = -2;
const int ZOO_SEQUENCE = 1 << 1;
const int ZOO_EPHEMERAL = 1;

static char *_world = (char *)"world";
static char *_anyone = (char *)"anyone";
static char *_auth = (char *)"auth";
static char *_empty = (char *)"";

const int ZOO_PERM_READ = 1 << 0;
const int ZOO_PERM_WRITE = 1 << 1;
const int ZOO_PERM_CREATE = 1 << 2;
const int ZOO_PERM_DELETE = 1 << 3;
const int ZOO_PERM_ADMIN = 1 << 4;
const int ZOO_PERM_ALL = 0x1f;
struct Id ZOO_ANYONE_ID_UNSAFE = {_world, _anyone};
struct Id ZOO_AUTH_IDS = {_auth, _empty};
static struct ACL _OPEN_ACL_UNSAFE_ACL[] = {{0x1f, {_world, _anyone}}};
static struct ACL _READ_ACL_UNSAFE_ACL[] = {{0x01, {_world, _anyone}}};
static struct ACL _CREATOR_ALL_ACL_ACL[] = {{0x1f, {_auth, _empty}}};
struct ACL_vector ZOO_OPEN_ACL_UNSAFE = {1, _OPEN_ACL_UNSAFE_ACL};
struct ACL_vector ZOO_READ_ACL_UNSAFE = {1, _READ_ACL_UNSAFE_ACL};
struct ACL_vector ZOO_CREATOR_ALL_ACL = {1, _CREATOR_ALL_ACL_ACL};

const void *zoo_get_context(zhandle_t *zh) {
  return reinterpret_cast<ZMock *>(zh)->get_context();
}

static ZMock *mock(zhandle_t *zh) { return reinterpret_cast<ZMock *>(zh); }

int zoo_awget_children2(zhandle_t *zh, const char *path, watcher_fn watcher,
                        void *watcherCtx, strings_stat_completion_t completion,
                        const void *data) {
  return mock(zh)->awget_children2(path, watcher, watcherCtx, completion, data);
}

int zoo_awget(zhandle_t *zh, const char *path, watcher_fn watcher,
              void *watcherCtx, data_completion_t completion,
              const void *data) {
  return mock(zh)->awget(path, watcher, watcherCtx, completion, data);
}

int zoo_add_auth(zhandle_t *zh, const char *scheme, const char *cert,
                 int cert_len, void_completion_t completion, const void *data) {

  return mock(zh)->add_auth(scheme, cert, cert_len, completion, data);
}

const char *zoo_get_current_server(zhandle_t *zh) {
  return mock(zh)->get_current_server();
}

zhandle_t *zookeeper_init2(const char *host, watcher_fn fn, int recv_timeout,
                           const clientid_t *clientid, void *context, int flags,
                           log_callback_fn log_callback) {
  ZMock *mock = new ZMock(context);
  return mock->init2(host, fn, recv_timeout, clientid, context, flags,
                     log_callback);
}

zhandle_t *zookeeper_init_sasl(const char *host, watcher_fn fn,
                               int recv_timeout, const clientid_t *clientid,
                               void *context, int flags,
                               log_callback_fn log_callback,
                               zoo_sasl_params_t *sasl_params) {
  return zookeeper_init2(host, fn, recv_timeout, clientid, context, flags,
                         log_callback);
}

sasl_callback_t *zoo_sasl_make_basic_callbacks(const char *user,
                                               const char *realm,
                                               const char *password_file) {
  return NULL;
}

int zookeeper_close(zhandle_t *zh) {
  delete mock(zh);
  return ZOK;
}

// Sync methods mocks.
int zoo_create(zhandle_t *zh, const char *path, const char *value, int valuelen,
               const struct ACL_vector *acl, int flags, char *path_buffer,
               int path_buffer_len) {
  return mock(zh)->create(path, value, valuelen, acl, flags, path_buffer,
                          path_buffer_len);
}

int zoo_set(zhandle_t *zh, const char *path, const char *buffer, int buflen,
            int version) {
  return mock(zh)->set(path, buffer, buflen, version);
}

int zoo_remove_watches(zhandle_t *zh, const char *path, ZooWatcherType wtype,
                       watcher_fn watcher, void *watcherCtx, int local) {
  return 0;
}

int zoo_awexists(zhandle_t *zh, const char *path, watcher_fn watcher,
                 void *watcherCtx, stat_completion_t completion,
                 const void *data) {
  return 0;
}

int zoo_adelete(zhandle_t *zh, const char *path, int version,
                void_completion_t completion, const void *data) {
  return mock(zh)->adelete(path, version, completion, data);
}

int zoo_aset(zhandle_t *zh, const char *path, const char *buf, int len,
             int version, stat_completion_t completion, const void *data) {
  return mock(zh)->aset(path, buf, len, version, completion, data);
}

int zoo_acreate(zhandle_t *zh, const char *path, const char *value,
                int valuelen, const struct ACL_vector *acl, int flags,
                string_completion_t completion, const void *data) {
  return mock(zh)->acreate(path, value, valuelen, acl, flags, completion, data);
}

int zoo_amulti(zhandle_t *zh, int count, const zoo_op_t *ops,
               zoo_op_result_t *results, void_completion_t, const void *data) {
  return 0;
}

void zoo_create_op_init(zoo_op_t *op, const char *path, const char *value,
                        int valuelen, const struct ACL_vector *acl, int flags,
                        char *path_buffer, int path_buffer_len) {
  CHECK(op);
  op->type = ZOO_CREATE_OP;
  op->create_op.path = path;
  op->create_op.data = value;
  op->create_op.datalen = valuelen;
  op->create_op.acl = acl;
  op->create_op.flags = flags;
  op->create_op.buf = path_buffer;
  op->create_op.buflen = path_buffer_len;
}

void zoo_delete_op_init(zoo_op_t *op, const char *path, int version) {
  CHECK(op);
  op->type = ZOO_DELETE_OP;
  op->delete_op.path = path;
  op->delete_op.version = version;
}

void zoo_set_op_init(zoo_op_t *op, const char *path, const char *buffer,
                     int buflen, int version, struct Stat *stat) {
  CHECK(op);
  op->type = ZOO_SETDATA_OP;
  op->set_op.path = path;
  op->set_op.data = buffer;
  op->set_op.datalen = buflen;
  op->set_op.version = version;
  op->set_op.stat = stat;
}

} // extern "C"

const void *ZMock::get_context() { return zoo_context; }

zhandle_t *ZMock::init2(const char *host, watcher_fn fn, int recv_timeout,
                        const clientid_t *clientid, void *context, int flags,
                        log_callback_fn log_callback) {
  zoo_context = context;
  zhandle_t *zh = reinterpret_cast<zhandle_t *>(this);
  std::thread async([this, zh, fn]() {
    (*fn)(zh, ZOO_SESSION_EVENT, ZOO_CONNECTED_STATE, nullptr, zoo_context);
  });

  std::unique_lock<std::mutex> lck(completions_mx);
  completions.push_back(std::move(async));
  return zh;
}

int ZMock::add_auth(const char *scheme, const char *cert, int cert_len,
                    void_completion_t completion, const void *data) {
  return 0;
}

const char *ZMock::get_current_server() {
  static const char current_server[] = "server:1234";
  return current_server;
}

int ZMock::awget_children2(const char *path, watcher_fn watcher,
                           void *watcherCtx,
                           strings_stat_completion_t completion,
                           const void *data) {
  // TODO: add watch.
  fs::path dirname = fsroot / std::string_view(path).substr(1);
  if (watcher != nullptr) {
    children_watches.put(path, watcher, watcherCtx);
  }

  std::thread async([dirname, completion, data]() {
    if (!fs::exists(dirname)) {
      (*completion)(ZNONODE, nullptr, nullptr, data);
      return;
    }

    std::vector<fs::path> names;
    for (auto &p : fs::directory_iterator(dirname)) {
      names.push_back(p.path().filename());
    }
    struct String_vector children;
    std::unique_ptr<char *[]> chars(new char *[names.size()]);
    children.data = chars.get();
    children.count = names.size();
    int idx = 0;
    for (auto &p : names) {
      children.data[idx++] = const_cast<char *>(p.c_str());
    }
    struct Stat stat {
      0
    };
    (*completion)(ZOK, &children, &stat, data);
  });

  std::unique_lock<std::mutex> lck(completions_mx);
  completions.push_back(std::move(async));

  return ZOK;
}

int ZMock::awget(const char *path, watcher_fn watcher, void *watcherCtx,
                 data_completion_t completion, const void *data) {

  if (watcher != nullptr) {
    data_watches.put(path, watcher, watcherCtx);
  }
  fs::path fname = fsroot / std::string_view(path).substr(1) / ".data";

  std::thread async([fname, completion, data]() {
    if (!fs::exists(fname)) {
      (*completion)(ZNONODE, nullptr, 0, nullptr, data);
      return;
    }

    std::ifstream in(fname, std::ios::out | std::ios::binary);
    std::ostringstream out;
    in >> out.rdbuf();

    struct Stat stat {
      0
    };
    (*completion)(ZOK, out.str().c_str(), out.str().size(), &stat, data);
  });

  std::unique_lock<std::mutex> lck(completions_mx);
  completions.push_back(std::move(async));
  return ZOK;
}

int ZMock::adelete(const char *path, int version, void_completion_t completion,
                   const void *data) {
  return 0;
}

int ZMock::aset(const char *path, const char *buf, int len, int version,
                stat_completion_t completion, const void *data) {
  return 0;
}

int ZMock::acreate(const char *path, const char *value, int valuelen,
                   const struct ACL_vector *acl, int flags,
                   string_completion_t completion, const void *data) {
  return 0;
}

int ZMock::close() { return 0; }

int ZMock::create(const char *path, const char *value, int valuelen,
                  const struct ACL_vector *acl, int flags, char *path_buffer,
                  int path_buffer_len) {
  CHECK(path[0] == '/');
  fs::path node_path = fsroot / (path + 1);
  std::error_code err;
  fs::create_directories(node_path, err);
  if (err.value() == EEXIST) {
    return ZNODEEXISTS;
  }

  fs::path data_path = node_path / ".data";
  atomic_write(data_path, std::nullopt, value, valuelen);
  if (path_buffer && path_buffer_len > (int)strlen(path)) {
    strcpy(path_buffer, path);
  }
  return ZOK;
}

int ZMock::set(const char *path, const char *buffer, int buflen, int version) {
  CHECK(path[0] == '/');
  fs::path node_path = fsroot / (path + 1);
  if (!fs::exists(node_path)) {
    return ZNONODE;
  }

  fs::path data_path = node_path / ".data";
  atomic_write(data_path, std::nullopt, buffer, buflen);
  return ZOK;
}

void ZMock::fire(const std::string &path, int watch_type,
                 ZMock::Watches &watches) {
  auto watch = watches.get(path);
  if (!watch) {
    return;
  }

  auto fn = watch.value().first;
  auto ctx = watch.value().second;
  std::thread async([this, path, fn, ctx, watch_type]() {
    fn(reinterpret_cast<zhandle_t *>(this), watch_type, ZOO_CONNECTED_STATE,
       path.c_str(), ctx);
  });

  std::unique_lock<std::mutex> lck(completions_mx);
  completions.push_back(std::move(async));
}

void ZMock::join() {
  std::vector<std::thread> copy;
  {
    std::unique_lock<std::mutex> lck(completions_mx);
    for (auto &t : completions) {
      copy.push_back(std::move(t));
    }
    completions.erase(completions.begin(), completions.end());
  }
  for (auto &t : copy) {
    t.join();
  }
}
