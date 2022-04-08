#include <treadmill/fs.h>
#include <treadmill/logging.h>

#include <algorithm>
#include <string>
#include <utility>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <unistd.h>

namespace treadmill {

int atomic_write(const std::filesystem::path &path,
                  const std::optional<std::filesystem::path> &tmpdir,
                  const std::optional<time_t> &mtime, long nsec,
                  const write_data_f &write_data) {
  char fname[PATH_MAX];
  auto dirname = tmpdir ? tmpdir.value() : path.parent_path();
  auto filename = path.filename();

  if (filename.native().size() > NAME_MAX) {
    SPDLOG_ERROR("Filename too long: {}", filename.native());
    return -1;
  }

  CHECK(path.native().size() < sizeof(fname) - 16);


  ::snprintf(fname, sizeof(fname), "%s/.tmpXXXXXX", dirname.c_str());
  auto tempfd = ::mkstemp(fname);
  PCHECK_LOG(tempfd != -1, "mkstemp: {}", fname);

  write_data(tempfd);

  if (mtime) {
    struct timespec times[2]{{0}, {0}};
    times[0].tv_sec = mtime.value();
    times[1].tv_sec = mtime.value();

    PCHECK(0 == ::futimens(tempfd, times));
  }

  PCHECK(0 == ::fchmod(tempfd, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH));
  PCHECK(0 == ::close(tempfd));
  PCHECK(0 == ::rename(fname, path.c_str()));

  return 0;
}

int atomic_write(const std::filesystem::path &path,
                  const std::optional<std::filesystem::path> &tmpdir,
                  const std::optional<time_t> &mtime, long nsec,
                  const char *data, size_t len) {
  return atomic_write(path, tmpdir, mtime, nsec, [data, len](int tempfd) {
    if (data) {
      const char *buf = data;
      size_t remaining = len;
      const size_t max_write_len = SSIZE_MAX;

      while (remaining > 0) {
        int written = ::write(tempfd, buf, std::min(remaining, max_write_len));
        PCHECK(written > 0);
        remaining -= written;
        buf += written;
      }
    }
  });
}

} // namespace treadmill
