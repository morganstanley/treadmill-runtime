#include <treadmill/gssapi.h>
#include <treadmill/logging.h>
#include <treadmill/scopeguard.h>

#include <cpp-base64/base64.h>
#include <gssapi/gssapi.h>

namespace treadmill {

void display_status_1(const char *m, OM_uint32 code, int type) {
  OM_uint32 maj_stat, min_stat;
  gss_buffer_desc msg;
  OM_uint32 msg_ctx;

  msg_ctx = 0;
  while (1) {
    maj_stat = gss_display_status(&min_stat, code, type, GSS_C_NULL_OID,
                                  &msg_ctx, &msg);
    CHECK(maj_stat == GSS_S_COMPLETE);
    SPDLOG_WARN("GSS-API error {} : {}", m, (const char *)msg.value);
    gss_release_buffer(&min_stat, &msg);

    if (!msg_ctx)
      break;
  }
}

void display_status(const char *msg, OM_uint32 maj_stat, OM_uint32 min_stat) {
  display_status_1(msg, maj_stat, GSS_C_GSS_CODE);
  display_status_1(msg, min_stat, GSS_C_MECH_CODE);
}

std::optional<std::string> get_kerberos_token(const std::string &service) {
  OM_uint32 major, minor = 0;

  gss_buffer_desc server_name_buf;
  server_name_buf.value = (void *)service.c_str();
  server_name_buf.length = service.size();

  gss_name_t server_name{GSS_C_NO_NAME};
  major = gss_import_name(&minor, &server_name_buf, GSS_C_NT_HOSTBASED_SERVICE,
                          &server_name);

  auto server_name_guard = make_scopeguard([&server_name](void *) {
    OM_uint32 minor = 0;
    if (server_name != GSS_C_NO_NAME) {
      gss_release_name(&minor, &server_name);
    }
  });

  if (GSS_ERROR(major)) {
    display_status("gss_import_name", major, minor);
    return std::nullopt;
  }

  gss_ctx_id_t sec_context = GSS_C_NO_CONTEXT;
  auto sec_context_guard = make_scopeguard([&sec_context](void *) {
    OM_uint32 minor = 0;
    if (sec_context != GSS_C_NO_CONTEXT) {
      gss_delete_sec_context(&minor, &sec_context, GSS_C_NO_BUFFER);
    }
  });

  gss_cred_id_t cred_handle = GSS_C_NO_CREDENTIAL;
  gss_OID mech_type = GSS_C_NO_OID;
  OM_uint32 req_flags = 0;
  OM_uint32 time_req = 0;
  gss_channel_bindings_t input_chan_bindings = GSS_C_NO_CHANNEL_BINDINGS;
  gss_buffer_desc *in_token_ptr = GSS_C_NO_BUFFER;
  OM_uint32 *ret_flags = nullptr;
  OM_uint32 *ret_time = nullptr;
  gss_OID mech = GSS_C_NULL_OID;

  gss_buffer_desc out_token{0, nullptr};
  auto out_token_guard = make_scopeguard([&out_token](void *) {
    OM_uint32 minor = 0;
    if (out_token.value) {
      gss_release_buffer(&minor, &out_token);
    }
  });

  major = gss_init_sec_context(&minor, cred_handle, &sec_context, server_name,
                               mech_type, req_flags, time_req,
                               input_chan_bindings, in_token_ptr, &mech,
                               &out_token, ret_flags, ret_time);

  if (GSS_ERROR(major)) {
    display_status("gss_init_sec_context", major, minor);
    return std::nullopt;
  }

  // TODO: Not clear if this is can be the case, as we checked status code
  //       above and it was success.
  if (!out_token.length) {
    return std::nullopt;
  }

  return std::make_optional(
      base64_encode((const uint8_t *)out_token.value, out_token.length));
}

} // namespace treadmill
