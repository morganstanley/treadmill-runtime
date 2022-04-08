#pragma once

#include <functional>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <yaml.h>

namespace treadmill {

struct YAMLParser : public yaml_parser_t {

  typedef std::function<void(const char *)> callback_t;

  YAMLParser() { yaml_parser_initialize(this); }
  ~YAMLParser() { yaml_parser_delete(this); }

  void callback(const std::string &path, const callback_t &clbk) {
    callbacks_.emplace(path, clbk);
  }

  void parse_stream(const unsigned char *s, int len);
  void parse_stream(const char *s, int len) {
    parse_stream(reinterpret_cast<const unsigned char *>(s), len);
  }

private:
  void parse(yaml_event_t *event);
  void parse_document();
  void parse_mapping(const std::string &path);
  void parse_mapping_value(const std::string &path);
  void parse_sequence(const std::string &path);

  typedef std::unordered_map<std::string, callback_t> callbacks_t;
  callbacks_t callbacks_;
};

} // namespace treadmill
