#ifndef TREADMILL_FS_H
#define TREADMILL_FS_H

#include <filesystem>
#include <functional>
#include <optional>

#include <sys/time.h>

namespace treadmill {

typedef std::function<void(int)> write_data_f;

int atomic_write(const std::filesystem::path &path,
                 const std::optional<std::filesystem::path> &tmpdir,
                 const std::optional<time_t> &mtime, long nsec,
                 const write_data_f &write_data);

int atomic_write(const std::filesystem::path &path,
                 const std::optional<std::filesystem::path> &tmpdir,
                 const std::optional<time_t> &mtime, long nsec,
                 const char *data, size_t len);

inline int atomic_write(const std::filesystem::path &path,
                        const std::optional<std::filesystem::path> &tmpdir,
                        const char *data, size_t len) {
  return atomic_write(path, tmpdir, std::nullopt, 0, data, len);
}

inline int atomic_write(const std::filesystem::path &path,
                        const std::optional<std::filesystem::path> &tmpdir,
                        const std::optional<time_t> &mtime, const char *data,
                        size_t len) {
  return atomic_write(path, tmpdir, mtime, 0, data, len);
}

} // namespace treadmill

#endif
