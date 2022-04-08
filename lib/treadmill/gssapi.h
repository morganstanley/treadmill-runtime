#ifndef TREADMILL_GSSAPI_H
#define TREADMILL_GSSAPI_H

#include <optional>
#include <string>

namespace treadmill {

std::optional<std::string> get_kerberos_token(const std::string &service);
}

#endif // TREADMILL_GSSAPI_H
