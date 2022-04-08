#pragma once

#include <treadmill/zkclient.h>

namespace treadmill {
namespace zknamespace {
namespace constants {

constexpr char allocations[] = "/allocations";
constexpr char appgroups[] = "/app-groups";
constexpr char appgroup_lookup[] = "/appgroup-lookups";
constexpr char appmonitors[] = "/app-monitors";
constexpr char archive_config[] = "/archive/config";
constexpr char blackedout_apps[] = "/blackedout.apps";
constexpr char blackedout_servers[] = "/blackedout.servers";
constexpr char buckets[] = "/buckets";
constexpr char cell[] = "/cell";
constexpr char cron_jobs[] = "/cron-jobs";
constexpr char cron_trace[] = "/cron-trace";
constexpr char cron_trace_history[] = "/cron-trace.history";
constexpr char data_records[] = "/data-records";
constexpr char discovery[] = "/discovery";
constexpr char discovery_state[] = "/discovery.state";
constexpr char election[] = "/election";
constexpr char endpoints[] = "/endpoints";
constexpr char events[] = "/events";
constexpr char finished[] = "/finished";
constexpr char finished_history[] = "/finished.history";
constexpr char globals[] = "/globals";
constexpr char identity_groups[] = "/identity-groups";
constexpr char keytab_locker[] = "/keytab-locker";
constexpr char partitions[] = "/partitions";
constexpr char placement[] = "/placement";
constexpr char reboots[] = "/reboots";
constexpr char running[] = "/running";
constexpr char scheduled[] = "/scheduled";
constexpr char scheduled_stats[] = "/scheduled-stats";
constexpr char scheduler[] = "/scheduler";
constexpr char servers[] = "/servers";
constexpr char server_presence[] = "/server.presence";
constexpr char server_trace[] = "/server-trace";
constexpr char server_trace_history[] = "/server-trace.history";
constexpr char state_reports[] = "/reports";
constexpr char strategies[] = "/strategies";
constexpr char ticket_locker[] = "/ticket-locker";
constexpr char tickets[] = "/tickets";
constexpr char trace[] = "/trace";
constexpr char trace_history[] = "/trace.history";
constexpr char traits[] = "/traits";
constexpr char treadmill[] = "/treadmill";
constexpr char trigger_trace[] = "/trigger-trace";
constexpr char trigger_trace_history[] = "/trigger-trace.history";
constexpr char version[] = "/version";
constexpr char version_history[] = "/version.history";
constexpr char version_id[] = "/version-id";
constexpr char warpgate[] = "/warpgate";
constexpr char zookeeper[] = "/zookeeper";
constexpr char scheduler_lock[] = "/scheduler-lock";

constexpr long num_trace_shards = 256;
constexpr long num_cron_trace_shards = 256;
constexpr long num_server_trace_shards = 256;

} // namespace constants

void set_permissions(ZkClient &zkclient);

} // namespace zknamespace
} // namespace treadmill
