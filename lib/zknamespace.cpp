#include <treadmill/zknamespace.h>

namespace treadmill {
namespace zknamespace {

void set_permissions(ZkClient &zkclient) {

  static char scheme[256];

  // TODO: why all of these are static?
  strncpy(scheme, zkclient.authscheme().c_str(), sizeof(scheme));
  SPDLOG_DEBUG("Using auth scheme: {}", scheme);

  static char admin_role[] = "file:///treadmill/roles/admin";
  static char reader_role[] = "file:///treadmill/roles/readers";
  static char server_role[] = "file:///treadmill/roles/servers";

  static acls_t default_acl{{ZOO_PERM_ALL, {scheme, admin_role}},
                            {ZOO_PERM_READ, {scheme, reader_role}}};

  static acls_t server_acl_all{{ZOO_PERM_ALL, {scheme, admin_role}},
                               {ZOO_PERM_ALL, {scheme, server_role}},
                               {ZOO_PERM_READ, {scheme, reader_role}}};

  static acls_t server_acl_del{{ZOO_PERM_ALL, {scheme, admin_role}},
                               {ZOO_PERM_DELETE, {scheme, server_role}},
                               {ZOO_PERM_READ, {scheme, reader_role}}};

  using namespace treadmill::zknamespace::constants;

  zkclient.permissions({{child_of(discovery), server_acl_all},
                        {child_of(discovery_state), server_acl_all},
                        {child_of(scheduled), server_acl_del},
                        {child_of(blackedout_servers), server_acl_all},
                        {child_of(endpoints), server_acl_all},
                        {child_of(events), server_acl_all},
                        {child_of(running), server_acl_all},
                        {child_of(server_presence), server_acl_all},
                        {child_of(version), server_acl_all},
                        {child_of(version_history), server_acl_all},
                        {child_of(reboots), server_acl_all},
                        {child_of(trace), server_acl_all},
                        {child_of(finished), server_acl_all},
                        {child_of(""), default_acl}});
}

} // namespace zknamespace
} // namespace treadmill
