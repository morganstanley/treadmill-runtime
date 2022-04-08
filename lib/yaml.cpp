#include <treadmill/logging.h>
#include <treadmill/profiler.h>
#include <treadmill/yaml.h>

namespace treadmill {

inline bool is_null(const char *value) {
  return strcmp(value, "~") == 0 || strcasecmp(value, "null") == 0;
}

struct YAMLToken : public yaml_token_t {
  ~YAMLToken() { yaml_token_delete(this); }
};

struct YAMLEvent : public yaml_event_t {
  ~YAMLEvent() { yaml_event_delete(this); }
};

void YAMLParser::parse_stream(const unsigned char *s, int len) {
  Timer t;
  yaml_parser_set_input_string(this, s, len);

  while (true) {
    YAMLEvent event;
    parse(&event);
    switch (event.type) {
    case YAML_NO_EVENT:
    case YAML_ALIAS_EVENT:
    case YAML_STREAM_START_EVENT:
      break;
    case YAML_STREAM_END_EVENT:
      return;
    case YAML_DOCUMENT_START_EVENT:
      parse_document();
      break;
    default:
      CHECK_LOG(false, "Unexpected event type: {}", event.type);
      break;
    }
  }
}

void YAMLParser::parse(yaml_event_t *event) {
  if (!yaml_parser_parse(this, event)) {
    // TODO: record information about the error - line/pos.
    throw std::invalid_argument("Invalid YAML stream.");
  }
}

void YAMLParser::parse_document() {
  while (true) {
    YAMLEvent event;
    parse(&event);
    switch (event.type) {
    case YAML_NO_EVENT:
    case YAML_ALIAS_EVENT:
      break;
    case YAML_DOCUMENT_END_EVENT:
    case YAML_STREAM_END_EVENT:
      return;
    case YAML_SEQUENCE_START_EVENT:
      parse_sequence("/");
      break;
    case YAML_MAPPING_START_EVENT:
      parse_mapping("/");
      break;
    default:
      CHECK_LOG(false, "Unexpected event type: {}", event.type);
      break;
    }
  }
}

void YAMLParser::parse_mapping(const std::string &path) {
  while (true) {
    YAMLEvent event;
    parse(&event);
    switch (event.type) {
    case YAML_NO_EVENT:
    case YAML_ALIAS_EVENT:
    case YAML_STREAM_END_EVENT:
    case YAML_MAPPING_END_EVENT:
      return;
    case YAML_SCALAR_EVENT: {
      const char *value =
          reinterpret_cast<const char *>(event.data.scalar.value);
      std::string new_path =
          (path[path.size() - 1] == '/') ? path + value : path + "/" + value;
      parse_mapping_value(new_path);
    } break;
    default:
      CHECK_LOG(false, "Unexpected event type: {}", event.type);
      break;
    }
  }
}

void YAMLParser::parse_mapping_value(const std::string &path) {

  YAMLEvent event;
  parse(&event);

  switch (event.type) {
  case YAML_NO_EVENT:
  case YAML_ALIAS_EVENT:
  case YAML_STREAM_END_EVENT:
    return;
  case YAML_SCALAR_EVENT: {
    auto clbk = callbacks_.find(path);
    if (clbk != callbacks_.end()) {
      const char *value =
          reinterpret_cast<const char *>(event.data.scalar.value);
      if (!is_null(value)) {
        clbk->second(value);
      } else {
        clbk->second(nullptr);
      }
    }
  }
    return;
  case YAML_SEQUENCE_START_EVENT:
    return parse_sequence(path);
  case YAML_MAPPING_START_EVENT:
    return parse_mapping(path);
  default:
    CHECK_LOG(false, "Unexpected event type: {}", event.type);
    break;
  }
}

void YAMLParser::parse_sequence(const std::string &path) {
  while (true) {

    YAMLEvent event;
    parse(&event);

    switch (event.type) {
    case YAML_NO_EVENT:
    case YAML_ALIAS_EVENT:
    case YAML_STREAM_END_EVENT:
    case YAML_SEQUENCE_END_EVENT:
      return;
    case YAML_SCALAR_EVENT: {
      const char *value =
          reinterpret_cast<const char *>(event.data.scalar.value);
      auto clbk = callbacks_.find(path);
      if (clbk != callbacks_.end()) {
        if (!is_null(value)) {
          clbk->second(value);
        } else {
          clbk->second(nullptr);
        }
      }
    } break;
    case YAML_MAPPING_START_EVENT: {
      auto clbk = callbacks_.find(path);
      if (clbk != callbacks_.end()) {
        clbk->second(nullptr);
      }
    }
      parse_mapping(path);
      break;
    case YAML_SEQUENCE_START_EVENT: {
      auto clbk = callbacks_.find(path);
      if (clbk != callbacks_.end()) {
        clbk->second(nullptr);
      }
    }
      parse_sequence(path);
      break;
    default:
      CHECK_LOG(false, "Unexpected event type: {}", event.type);
      break;
    }
  }
}

} // namespace treadmill
