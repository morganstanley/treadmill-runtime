#pragma once

namespace treadmill {

template <class InputIt1, class InputIt2, typename F1, typename F2>
void for_each_diff(InputIt1 first1, InputIt1 last1, InputIt2 first2,
                   InputIt2 last2, F1 f1, F2 f2) {
  while (true) {
    if (first1 == last1) {
      std::for_each(first2, last2, f2);
      break;
    }
    if (first2 == last2) {
      std::for_each(first1, last1, f1);
      break;
    }

    if (*first1 < *first2) {
      f1(*first1++);
    } else if (*first2 < *first1) {
      f2(*first2++);
    } else {
      ++first1;
      ++first2;
    }
  }
}

template <class InputIt1, class InputIt2, typename F1, typename F2, typename FC,
          typename Less>
void for_each_diff(InputIt1 first1, InputIt1 last1, InputIt2 first2,
                   InputIt2 last2, F1 f1, F2 f2, FC fc, Less less) {
  while (true) {
    if (first1 == last1) {
      std::for_each(first2, last2, f2);
      break;
    }
    if (first2 == last2) {
      std::for_each(first1, last1, f1);
      break;
    }

    if (less(*first1, *first2)) {
      f1(*first1++);
    } else if (less(*first2, *first1)) {
      f2(*first2++);
    } else {
      fc(*first1++, *first2++);
    }
  }
}

} // namespace treadmill
