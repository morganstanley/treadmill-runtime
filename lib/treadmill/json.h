#pragma once

#include <chrono>
#include <optional>

namespace treadmill {
namespace js {

template <typename Writer> class Array;

template <typename Writer> class Object {
public:
  explicit Object(Writer *writer) : writer_(writer) { writer_->StartObject(); }
  ~Object() { writer_->EndObject(); }

  template <class T> Object &operator()(const char *key, T value) {
    writer_->Key(key);
    write(value);
    return *this;
  }

  template <class It> Object &operator()(const char *key, It begin, It end) {
    auto a = arr(key);
    for (auto v = begin; v != end; ++v) {
      a.write(*v);
    }
    return *this;
  }

  Object<Writer> obj(const char *key) {
    writer_->Key(key);
    return Object<Writer>(writer_);
  }

  Array<Writer> arr(const char *key) {
    writer_->Key(key);
    return Array<Writer>(writer_);
  }

  void write(std::nullopt_t value) { writer_->Null(); }
  void write(bool value) { writer_->Bool(value); }
  void write(const char *value) { writer_->String(value); }
  void write(const std::string &value) { writer_->String(value.c_str()); }
  void write(const int value) { writer_->Int(value); }
  void write(const unsigned value) { writer_->Uint(value); }
  void write(const int64_t value) { writer_->Int64(value); }
  void write(const uint64_t value) { writer_->Uint64(value); }
  void write(const double value) { writer_->Double(value); }
  void write(const std::chrono::time_point<std::chrono::system_clock> value) {
    auto epoch = value.time_since_epoch();
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(epoch);
    writer_->Uint64(seconds.count());
  }
  void write(const std::chrono::duration<long> value) {
    writer_->Uint64(
        std::chrono::duration_cast<std::chrono::seconds>(value).count());
  }
  template <class T> void write(std::optional<T> value) {
    if (value) {
      write(value.value());
    } else {
      writer_->Null();
    }
  }

private:
  Writer *writer_;
};

template <typename Writer> class Array {
public:
  explicit Array(Writer *writer) : writer_(writer) { writer_->StartArray(); }
  template <typename It>
  Array(Writer *writer, It begin, It end) : writer_(writer) {
    for (auto v = begin; v != end; ++v) {
      write(*v);
    }
  }
  ~Array() { writer_->EndArray(); }

  Object<Writer> obj() { return Object<Writer>(writer_); }
  Array<Writer> arr() { return Array<Writer>(writer_); }

  template <class T> Array<Writer> &operator<<(T value) {
    write(value);
    return *this;
  }

  void write(std::nullopt_t value) { writer_->Null(); }
  void write(bool value) { writer_->Bool(value); }
  void write(const char *value) { writer_->String(value); }
  void write(const std::string &value) { writer_->String(value.c_str()); }
  void write(const int value) { writer_->Int(value); }
  void write(const unsigned value) { writer_->Uint(value); }
  void write(const int64_t value) { writer_->Int64(value); }
  void write(const uint64_t value) { writer_->Uint64(value); }
  void write(const double value) { writer_->Double(value); }
  void write(const std::chrono::time_point<std::chrono::system_clock> value) {
    auto epoch = value.time_since_epoch();
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(epoch);
    writer_->Uint64(seconds.count());
  }
  void write(const std::chrono::duration<long> value) {
    writer_->Uint64(
        std::chrono::duration_cast<std::chrono::seconds>(value).count());
  }
  template <class T> void write(std::optional<T> value) {
    if (value) {
      write(value.value());
    } else {
      writer_->Null();
    }
  }

private:
  Writer *writer_;
};

} // namespace js
} // namespace treadmill
