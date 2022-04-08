#include <treadmill/gssapi.h>
#include <treadmill/zkclient.h>

#include <fmt/format.h>
#include <iostream>
#include <regex>

namespace treadmill {

// Example connection strings:
//
// zookeeper://host1:123,host2.234
// zookeeper://host1:123,host2.234/ch/root
// zookeeper+sasl://host1:123,host2.234#service=foo,mechlist=GSSAPI
// zookeeper+sasl://host1:123,host2.234/ch/root#service=foo,mechlist=GSSAPI
//
// clang-format off
static const std::regex connstr_regex(
    "^(zookeeper[^:/?#]*)"                    // zookeeper+sasl
    "://([^/?#]*)"                            // ://host:port,host,port
    "(/[^?#]*)?"                              // optional chroot
    "(#([^=]+)=([^,]+)(,([^=]+)=([^,]+))*)?$" // optional #k=v[,k=v]
);
// clang-format on

ZkClient::AsyncCompletions<ZkClient::DataWatch> ZkClient::data_completions_;
ZkClient::AsyncCompletions<ZkClient::ChildrenWatch>
    ZkClient::children_completions_;
ZkClient::AsyncCompletions<ZkClient::ExistsWatch> ZkClient::exists_completions_;

bool check_zk_connstr(const std::string &connstr) {
  std::smatch match;
  return std::regex_search(connstr, match, connstr_regex);
}

void parse_zk_connstr(const std::string &connstr, std::string &scheme,
                      std::string &hosts, std::string &chroot,
                      std::unordered_map<std::string, std::string> &params) {

  std::smatch match;
  if (std::regex_search(connstr, match, connstr_regex)) {
    auto m = match.cbegin();
    scheme = *(++m);
    hosts = *(++m);
    chroot = *(++m);
    while (++m != match.cend()) {
      std::string key = *(++m);
      std::string val = *(++m);
      params[key] = val;
    }
  }
}

static void zk_log(const char *message) {
  // TODO: Zookeeper log has redundant info which we already log, investigate
  //       way to make it shorter/nicer.
  // std::string_view log_message(message);

  // Rely on zk log in the format: ... SEVERITY@source@ ...
  // DEBUG(log_message.substr(log_message.find('@') + 1));
}

struct AuthContext {
  std::string service;
  struct buffer auth;

  ~AuthContext() {
    if (auth.buff) {
      free(auth.buff);
    }
  }
};

struct buffer krb_auth_clbk(const char *hostname, const void *ctx) {

  CHECK(ctx);

  AuthContext *auth_ctx =
      reinterpret_cast<AuthContext *>(const_cast<void *>(ctx));
  auth_ctx->auth = {0, 0};
  auto serverid = fmt::format("{}@{}", auth_ctx->service, hostname);
  auto cert = get_kerberos_token(serverid);

  if (cert) {
    auth_ctx->auth.buff = strdup(const_cast<char *>(cert.value().c_str()));
    auth_ctx->auth.len = cert.value().size();
  }

  return auth_ctx->auth;
}

void free_auth_ctx(void *ctx) {
  SPDLOG_DEBUG("free_auth_ctx: {}", ctx);
  AuthContext *auth_ctx =
      reinterpret_cast<AuthContext *>(const_cast<void *>(ctx));
  delete auth_ctx;
}

void auth_completion(int rc, const void *data) {
  SPDLOG_DEBUG("auth_completion: {} {}", rc, data);
}

void ZkClient::connection_watcher(zhandle_t *zh, int type, int state,
                                  const char *path, void *watcherCtx) {
  SPDLOG_DEBUG("connection_watcher: type = {}, state: {}", type, state);
  auto conn = const_cast<ZkClient *>(
      reinterpret_cast<const ZkClient *>(zoo_get_context(zh)));

  CHECK(conn);
  conn->state_ = state;

  if (state == ZOO_CONNECTED_STATE) {
    SPDLOG_DEBUG("Connected to: {}", zoo_get_current_server(zh));
    if (conn->auth_clbk_) {
      (*(conn->auth_clbk_))(zh);
    }
  }

  if (conn->connection_watch_) {
    conn->connection_watch_.value()(type, state);
  }
}

void ZkClient::remove_data_watch_completion(int rc, const void *data) {
  if (rc != ZOK) {
    SPDLOG_DEBUG("remove_data_watch_completion: {} {}", rc, data);
  }
}

void ZkClient::remove_children_watch_completion(int rc, const void *data) {
  if (rc != ZOK) {
    SPDLOG_DEBUG("remove_children_watch_completion: {} {}", rc, data);
  }
}

void ZkClient::remove_exists_watch_completion(int rc, const void *data) {
  if (rc != ZOK) {
    SPDLOG_DEBUG("remove_exists_watch_completion: {} {}", rc, data);
  }
}

void ZkClient::data_completion(int rc, const char *value, int value_len,
                               const ZkClient::stat_t *stat, const void *ctx) {
  if (rc != ZOK) {
    SPDLOG_DEBUG("data_completion: {}", rc);
  }

  auto completion = reinterpret_cast<ZkClient::Async<ZkClient::DataWatch> *>(
      const_cast<void *>(ctx));

  if (rc == ZOK) {
    CHECK(stat);
    CHECK(value);
    CHECK(value_len >= 0);
    bool again = (*completion->watcher)(
        rc, ZkClient::Path(completion->path),
        std::make_optional(std::string_view(value, value_len)),
        std::make_optional(*stat));
    if (completion->watch && !again) {
      // TODO: singature of zoo_remove_completion is strange. Rather than
      //       accepting void_completion_t, it needs void_completion_t **.
      //       It seems that syncronous variety compliles.
      ::zoo_remove_watches(completion->zh, completion->path.c_str(),
                           ZWATCHTYPE_DATA, ZkClient::data_watcher, completion,
                           1);
      data_completions_.erase(completion);
    }
  } else {
    (*completion->watcher)(rc, ZkClient::Path(completion->path), std::nullopt,
                           std::nullopt);
  }

  if (rc == ZCLOSING) {
    data_completions_.erase(completion);
    return;
  }

  if (completion->done) {
    completion->done->set_value();
    completion->done = nullptr;
  }

  if (!completion->watch || rc != ZOK) {
    data_completions_.erase(completion);
  }
}

void ZkClient::children_stat_completion(int rc,
                                        const struct String_vector *strings,
                                        const ZkClient::stat_t *stat,
                                        const void *ctx) {
  if (rc != ZOK) {
    SPDLOG_DEBUG("children_stat_completion: {}", rc);
  }

  auto completion =
      reinterpret_cast<ZkClient::Async<ZkClient::ChildrenWatch> *>(
          const_cast<void *>(ctx));

  if (rc == ZOK) {

    CHECK(strings);
    CHECK(stat);
    CHECK(ctx);

    StringVector children(strings);
    bool again = (*completion->watcher)(rc, ZkClient::Path(completion->path),
                                        std::make_optional(children));
    if (completion->watch && !again) {
      // TODO: singature of zoo_remove_completion is strange. Rather than
      //       accepting void_completion_t, it needs void_completion_t **.
      //       It seems that syncronous variety compliles.
      ::zoo_remove_watches(completion->zh, completion->path.c_str(),
                           ZWATCHTYPE_CHILD, ZkClient::children_watcher,
                           completion, 1);
      children_completions_.erase(completion);
    }
  } else {
    (*completion->watcher)(rc, ZkClient::Path(completion->path), std::nullopt);
  }

  if (rc == ZCLOSING) {
    children_completions_.erase(completion);
    return;
  }

  if (completion->done) {
    completion->done->set_value();
    completion->done = nullptr;
  }

  if (!completion->watch || rc != ZOK) {
    children_completions_.erase(completion);
  }
}

void ZkClient::exists_completion(int rc, const ZkClient::stat_t *stat,
                                 const void *ctx) {
  if (rc != ZOK) {
    SPDLOG_DEBUG("exists_completion: {}", rc);
  }
  auto completion = reinterpret_cast<ZkClient::Async<ZkClient::ExistsWatch> *>(
      const_cast<void *>(ctx));

  if (rc == ZOK) {
    CHECK(stat);
    bool again = (*completion->watcher)(rc, ZkClient::Path(completion->path),
                                        std::make_optional(*stat));
    if (completion->watch && !again) {
      // TODO: singature of zoo_remove_completion is strange. Rather than
      //       accepting void_completion_t, it needs void_completion_t **.
      //       It seems that syncronous variety compliles.
      ::zoo_remove_watches(completion->zh, completion->path.c_str(),
                           ZWATCHTYPE_DATA, ZkClient::exists_watcher,
                           completion, 1);
      exists_completions_.erase(completion);
    }
  } else {
    (*completion->watcher)(rc, ZkClient::Path(completion->path), std::nullopt);
  }

  if (rc == ZCLOSING) {
    exists_completions_.erase(completion);
    return;
  }

  if (completion->done) {
    completion->done->set_value();
    completion->done = nullptr;
  }

  if (!completion->watch || !(rc == ZOK || rc == ZNONODE)) {
    exists_completions_.erase(completion);
  }
}

void ZkClient::children_watcher(zhandle_t *zh, int type, int state,
                                const char *path, void *ctx) {
  SPDLOG_DEBUG(
      "children_watcher: handle: {}, type: {}, state: {}, path: {}, ctx: {}",
      (void *)zh, type, state, (path ? path : "~"), ctx);

  if (state != ZOO_CONNECTED_STATE) {
    return;
  }

  if (type == ZOO_SESSION_EVENT) {
    return;
  }

  auto completion =
      reinterpret_cast<ZkClient::Async<ZkClient::ChildrenWatch> *>(ctx);

  CHECK(completion);
  // NOLINTNEXTLINE(clang-analyzer-core.NonNullParamChecker)
  CHECK(completion->path == path);
  int rc = ::zoo_awget_children2(
      zh, completion->path.c_str(), ZkClient::children_watcher, completion,
      ZkClient::children_stat_completion, completion);
  CHECK(rc == ZOK);
}

void ZkClient::data_watcher(zhandle_t *zh, int type, int state,
                            const char *path, void *ctx) {
  SPDLOG_DEBUG(
      "data_watcher: handle: {}, type: {}, state: {}, path: {}, ctx: {}",
      (void *)zh, type, state, (path ? path : "~"), ctx);

  if (state != ZOO_CONNECTED_STATE) {
    return;
  }

  if (type == ZOO_SESSION_EVENT) {
    return;
  }

  auto completion =
      reinterpret_cast<ZkClient::Async<ZkClient::DataWatch> *>(ctx);

  CHECK(completion);
  // NOLINTNEXTLINE(clang-analyzer-core.NonNullParamChecker)
  CHECK(completion->path == path);
  int rc = ::zoo_awget(zh, completion->path.c_str(), ZkClient::data_watcher,
                       completion, ZkClient::data_completion, completion);
  CHECK(rc == ZOK);
}

void ZkClient::exists_watcher(zhandle_t *zh, int type, int state,
                              const char *path, void *ctx) {
  SPDLOG_DEBUG(
      "exists_watcher: handle: {}, type: {}, state: {}, path: {}, ctx: {}",
      (void *)zh, type, state, (path ? path : "~"), ctx);

  if (state != ZOO_CONNECTED_STATE) {
    return;
  }

  if (type == ZOO_SESSION_EVENT) {
    return;
  }

  auto completion =
      reinterpret_cast<ZkClient::Async<ZkClient::ExistsWatch> *>(ctx);

  CHECK(completion);
  // NOLINTNEXTLINE(clang-analyzer-core.NonNullParamChecker)

  CHECK(completion->path == path);
  int rc =
      ::zoo_awexists(zh, completion->path.c_str(), ZkClient::exists_watcher,
                     completion, ZkClient::exists_completion, completion);
  CHECK(rc == ZOK);
}

void ZkClient::delete_clbk(int rc, const void *data) {
  auto clbk =
      reinterpret_cast<ZkClient::DeleteClbk *>(const_cast<void *>(data));

  if (clbk) {
    (*clbk)(rc);
  }
}

void ZkClient::multi_clbk(int rc, const void *data) {
  auto *multi_data = reinterpret_cast<MultiData *>(const_cast<void *>(data));

  auto result = multi_data->results_.begin();
  auto last = multi_data->results_.end();
  auto op = multi_data->ops_.begin();

  std::vector<MultiOpResult> results;
  while (result != last) {
    auto &[code, path] = *op;
    auto err = result->err;

    switch (code) {
    case MultiOpCode::create:
      results.push_back(
          {err, path,
           CreateOpResult{(err == ZOK && result->value)
                              ? std::make_optional<std::string>(
                                    result->value, result->valuelen)
                              : std::nullopt}});
      break;
    case MultiOpCode::set:
      results.push_back(
          {err, path,
           SetOpResult{(err == ZOK && result->stat)
                           ? std::make_optional<stat_t>(*result->stat)
                           : std::nullopt}});
      break;
    case MultiOpCode::del:
      results.push_back({err, path, DelOpResult{}});
      break;
    }

    if (result->stat) {
      delete result->stat;
    }
    if (result->value) {
      delete[] result->value;
    }
    ++result;
    ++op;
  }

  if (multi_data->clbk_) {
    (*multi_data->clbk_)(rc, results);
  }

  delete multi_data;
}

void ZkClient::set_clbk(int rc, const stat_t *stat, const void *data) {
  auto clbk = reinterpret_cast<ZkClient::SetClbk *>(const_cast<void *>(data));

  if (clbk) {
    if (rc == ZOK) {
      (*clbk)(rc, *stat);
    } else {
      (*clbk)(rc, std::nullopt);
    }
  }
}

void ZkClient::create_clbk(int rc, const char *value, const void *data) {
  auto clbk =
      reinterpret_cast<ZkClient::CreateClbk *>(const_cast<void *>(data));

  if (clbk) {
    if (rc == ZOK) {
      (*clbk)(rc, std::make_optional(std::string_view(value)));
    } else {
      (*clbk)(rc, std::nullopt);
    }
  }
}

ZkClient::ZkClient()
    : zh_(nullptr), state_(ZOO_NOTCONNECTED_STATE), connection_watch_(nullptr) {
}

ZkClient::ZkClient(ZkClient &&src)
    : zh_(src.zh_), connection_watch_(src.connection_watch_) {
  src.zh_ = nullptr;
  src.state_ = ZOO_NOTCONNECTED_STATE;
  src.connection_watch_ = nullptr;
}

ZkClient::~ZkClient() {
  SPDLOG_DEBUG("ZKClient::dtor");
  disconnect();
}

void ZkClient::disconnect() {
  if (zh_ != nullptr) {
    auto zh = zh_;
    zh_ = nullptr;
    state_ = ZOO_NOTCONNECTED_STATE;

    ::zookeeper_close(zh);
  }
}

bool ZkClient::connect(const std::string &connstr, unsigned int timeout_sec,
                       const std::optional<ConnectionWatch> &connection_watch) {

  std::string scheme;
  std::string chroot;
  std::unordered_map<std::string, std::string> params;

  connection_watch_ = connection_watch;

  parse_zk_connstr(connstr, scheme, connection_str_, chroot, params);
  // TODO: change regex not to parse chroot, as Zookeeper API natively
  //       supports the syntax.
  if (!chroot.empty()) {
    connection_str_ += chroot;
  }

  if ((scheme == "zookeeper+mskrb") || (scheme == "zookeeper")) {
    authscheme_ = "kerberos";
    auto service_it = params.find("service");
    std::optional<ConnectionAuthClbk> auth_clbk = std::nullopt;
    if (service_it != params.end()) {
      std::string service = service_it->second;
      auth_clbk_ = [service](zhandle_t *zh) {
        std::string current_server(zoo_get_current_server(zh));
        std::string current_hostname =
            current_server.substr(0, current_server.find(':'));

        AuthContext auth;
        auth.service = service;
        krb_auth_clbk(current_hostname.c_str(), &auth);
        SPDLOG_DEBUG("Adding kerberos authentication, service: {}", service);
        ::zoo_add_auth(zh, "kerberos", auth.auth.buff, auth.auth.len,
                       auth_completion, nullptr);
      };
    }

    zh_ = zookeeper_init2(connection_str_.c_str(), ZkClient::connection_watcher,
                          10000, 0, this, 0, zk_log);
  } else if (scheme == "zookeeper+sasl") {
    authscheme_ = "sasl";
    zoo_sasl_params_t sasl_params = {0};
    int sr = sasl_client_init(NULL);
    CHECK(sr == SASL_OK);

    auto service_it = params.find("service");
    if (service_it != params.end()) {
      sasl_params.service = service_it->second.c_str();
    }

    auto host_it = params.find("host");
    if (host_it != params.end()) {
      sasl_params.host = host_it->second.c_str();
    }

    auto mech_it = params.find("mechlist");
    if (mech_it != params.end()) {
      sasl_params.mechlist = mech_it->second.c_str();
    } else {
      sasl_params.mechlist = "GSSAPI";
    }

    sasl_params.callbacks = zoo_sasl_make_basic_callbacks(NULL, NULL, NULL);

    zh_ = zookeeper_init_sasl(connection_str_.c_str(),
                              ZkClient::connection_watcher, 10000, 0, this, 0,
                              zk_log, &sasl_params);
  } else {
    CHECK(false);
  }

  return true;
}

void ZkClient::watch_children(const std::string &path,
                              ZkClient::ChildrenWatch *watcher, bool watch,
                              std::promise<void> *done) {
  CHECK(watcher);
  auto completion =
      children_completions_.make(this, path, watcher, watch, done);
  int rc = ::zoo_awget_children2(zh_, completion->path.c_str(),
                                 watch ? ZkClient::children_watcher : nullptr,
                                 completion, ZkClient::children_stat_completion,
                                 completion);
  if (rc != ZOK) {
    SPDLOG_DEBUG("zoo_awget_children2: {}", rc);
    children_completions_.erase(completion);
    if (watcher) {
      (*watcher)(rc, path, std::nullopt);
    }
  }
}

void ZkClient::watch_data(const std::string &path, ZkClient::DataWatch *watcher,
                          bool watch, std::promise<void> *done) {
  CHECK(watcher);
  auto completion = data_completions_.make(this, path, watcher, watch, done);
  int rc = ::zoo_awget(zh_, completion->path.c_str(),
                       watch ? ZkClient::data_watcher : nullptr, completion,
                       ZkClient::data_completion, completion);
  if (rc != ZOK) {
    SPDLOG_DEBUG("zoo_awget: {}", rc);
    if (watcher) {
      (*watcher)(rc, path, std::nullopt, std::nullopt);
    }
  }
}

void ZkClient::watch_exists(const std::string &path,
                            ZkClient::ExistsWatch *watcher, bool watch,
                            std::promise<void> *done) {
  CHECK(watcher);
  auto completion = exists_completions_.make(this, path, watcher, watch, done);
  int rc = ::zoo_awexists(zh_, completion->path.c_str(),
                          watch ? ZkClient::exists_watcher : nullptr,
                          completion, ZkClient::exists_completion, completion);
  if (rc != ZOK) {
    SPDLOG_DEBUG("zoo_awexists: {}", rc);
    if (watcher) {
      (*watcher)(rc, path, std::nullopt);
    }
  }
}

void ZkClient::del(const std::string &path, int version,
                   const DeleteClbk *clbk) {
  int rc =
      ::zoo_adelete(zh_, path.c_str(), version, ZkClient::delete_clbk, clbk);
  if (rc != ZOK) {
    SPDLOG_DEBUG("zoo_adelete: {}", rc);
    if (clbk) {
      (*clbk)(rc);
    }
  }
}

void ZkClient::set(const std::string &path, int version, const char *data,
                   size_t data_len, const SetClbk *clbk) {
  int rc = ::zoo_aset(zh_, path.c_str(), data, data_len, version,
                      ZkClient::set_clbk, clbk);
  if (rc != ZOK) {
    SPDLOG_DEBUG("zoo_aset: {}", rc);
    if (clbk) {
      (*clbk)(rc, std::nullopt);
    }
  }
}

inline ACL_vector get_acl(const std::string &path, permissions_t &permissions) {
  for (auto &[fn, perms] : permissions) {
    if (fn(path)) {
      return {(int)perms.size(), &perms[0]};
    }
  }

  static ACL_vector default_acl = ZOO_OPEN_ACL_UNSAFE;
  return default_acl;
}

void ZkClient::create(const std::string &path, const char *data,
                      size_t data_len, int flags, const CreateClbk *clbk) {
  struct ACL_vector acl = get_acl(path, permissions_);
  int rc = ::zoo_acreate(zh_, path.c_str(), data, data_len, &acl, flags,
                         ZkClient::create_clbk, clbk);
  if (rc != ZOK) {
    SPDLOG_DEBUG("zoo_acreate: {}", rc);
    if (clbk) {
      (*clbk)(rc, std::nullopt);
    }
  }
}

void ZkClient::multi(const std::vector<MultiOp> &ops, const MultiOpClbk *clbk) {
  std::vector<zoo_op_t> zoo_ops(ops.size());
  MultiData *data = new MultiData(clbk, ops.size());
  auto &zoo_results = data->results_;

  size_t idx = 0;
  for (auto &op : ops) {
    auto &zoo_result = zoo_results[idx];
    auto &zoo_op = zoo_ops[idx];

    if (auto create_op = std::get_if<CreateOp>(&op)) {
      struct ACL_vector acl = get_acl(create_op->path, permissions_);
      int path_buffer_len = create_op->path.size() + 32;
      char *path_buffer = new char[path_buffer_len];
      zoo_create_op_init(&zoo_op, create_op->path.c_str(),
                         create_op->data.c_str(), create_op->data.size(), &acl,
                         create_op->flags, path_buffer, path_buffer_len);
      data->ops_[idx] = std::make_pair(MultiOpCode::create, create_op->path);
    }

    if (auto set_op = std::get_if<SetOp>(&op)) {
      zoo_set_op_init(&zoo_op, set_op->path.c_str(), set_op->data.c_str(),
                      set_op->data.size(), set_op->version, new stat_t());
      data->ops_[idx] = std::make_pair(MultiOpCode::set, set_op->path);
    }

    if (auto del_op = std::get_if<DelOp>(&op)) {
      zoo_delete_op_init(&zoo_op, del_op->path.c_str(), del_op->version);
      zoo_result.valuelen = 0;
      zoo_result.value = nullptr;
      zoo_result.stat = nullptr;
      data->ops_[idx] = std::make_pair(MultiOpCode::del, del_op->path);
    }

    ++idx;
  }

  auto rc = zoo_amulti(zh_, zoo_ops.size(), &zoo_ops[0], &(zoo_results)[0],
                       multi_clbk, data);
  // TODO: handle rc != ZOK same way as other methods, by invoking callback
  //       syncronously
  CHECK_LOG(rc == ZOK, "zoo_amulti: {}", rc);
}

} // namespace treadmill
