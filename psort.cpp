#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <jni.h>

#include <algorithm>
#include <ext/hash_map>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <string>
#include <sstream>
#include <vector>

#include "bucket_sort_PDC3.h"

#define COMPARE_WINDOW 20
#define RECURSIVE_DEPTH 10
#define REDUCE_FACTOR 0.9
//#define BWT

using namespace std;

typedef __gnu_cxx::hash_map<unsigned int, unsigned int> repeats_t;
repeats_t *repeats;
typedef __gnu_cxx::hash_map<unsigned int, unsigned int> rank_t;
rank_t *rank_cache;
unsigned int long_repeat_1 = 0, long_repeat_2 = 0, long_repeat = 0;

int dbg = 0;

// Utility function to print vectors.
template <class T>
void print_vector(const std::vector<T> &vec) {
  copy(vec.begin(), vec.end(), ostream_iterator<T>(cout, ","));
  cout << endl;
}

int compare_strings(const char *input,
    const unsigned int size, 
    const unsigned int &idx, const unsigned int &prev_idx,
    const unsigned int &idx_start, const unsigned int &num_chars,
    const bool &use_repeats) {
  
  unsigned int i = 0;
  unsigned int skip = idx_start;

  if (use_repeats) {
    // Optimization code.
    // Check to see if we can utilize the ranks of already ranked indices.
    if (rank_cache->count(idx) && rank_cache->count(prev_idx)) {
      if ((*rank_cache)[idx] < (*rank_cache)[prev_idx]) {
        return -1;
      } else {
        return 1;
      }
    }

    unsigned int idx_repeat = (*repeats)[idx];
    unsigned int prev_idx_repeat = (*repeats)[prev_idx];

    // If we find that the two indices are within the same repeat region.
    if ((idx < prev_idx) && (idx + idx_repeat > prev_idx)) {
      skip = max(skip, idx_repeat - (prev_idx-idx));
    } else if ((prev_idx < idx) && (prev_idx + prev_idx_repeat > idx)) {
      skip = max(skip, prev_idx_repeat - (idx-prev_idx));
    }

    // If we find that we have a same letter repeats at multiple positions.
    if (input[idx] == input[prev_idx]) {
      skip = max(skip, min(idx_repeat, prev_idx_repeat));
    }

    // If we find that the index is part of some long repeat that was checked
    // during this execution.
    if (idx > long_repeat_1 && idx < long_repeat_1 + long_repeat) {
      if (idx - long_repeat_1 == prev_idx - long_repeat_2) {
        skip = long_repeat - (idx - long_repeat_1);
      }
    } else if (idx > long_repeat_2 && idx < long_repeat_2 + long_repeat) {
      if (idx - long_repeat_2 == prev_idx - long_repeat_1) {
        skip = long_repeat - (idx - long_repeat_2);
      }
    }
  }

  int inc = 1;
  for (; idx+i+skip < size &&
      prev_idx+i+skip < size &&
      i < num_chars &&
      input[idx+i+skip] != '|' &&
      input[prev_idx+i+skip] != '|'
      ; i += inc) {
    inc = 1;
    if (input[idx+i+skip] < input[prev_idx+i+skip]) {
      return -1;
    } else if (input[idx+i+skip] > input[prev_idx+i+skip]) {
      return 1;
    }
    if (use_repeats) {
      // Use the long repeat optimization here also.
      if (idx+i+skip > long_repeat_1 && idx+i+skip < long_repeat_1 + long_repeat) {
        if (idx+i+skip - long_repeat_1 == prev_idx+i+skip - long_repeat_2) {
          skip += long_repeat - (idx+i+skip - long_repeat_1);
          long_repeat_1 = idx;
          long_repeat_2 = prev_idx;
          long_repeat = i+skip;
          inc = 0;
          continue;
        }
      } else  if (idx+i+skip > long_repeat_2 && idx+i+skip < long_repeat_2 + long_repeat) {
        if (idx+i+skip - long_repeat_2 == prev_idx+i+skip - long_repeat_1) {
          skip += long_repeat - (idx+i+skip - long_repeat_2);
          long_repeat_1 = idx;
          long_repeat_2 = prev_idx;
          long_repeat = i+skip;
          inc = 0;
          continue;
        }
      } 
      // Use the calculated ranks to see if it can be used to stop comparison.
      if (rank_cache->count(idx+i+skip) && rank_cache->count(prev_idx+i+skip)) {
        if ((*rank_cache)[idx+i+skip] < (*rank_cache)[prev_idx+i+skip]) {
          return -1;
        } else {
          return 1;
        }
      }

      // Update long repeats.
      if (long_repeat < i+skip) {
        long_repeat = i+skip;
        long_repeat_1 = idx;
        long_repeat_2 = prev_idx;
      }
    }
  }
  if (i < num_chars) {
    if (prev_idx+i+skip >= size || input[prev_idx+i+skip] == '|') {
      return 1;
    } else if (idx+i+skip >= size || input[idx+i+skip] == '|') {
      return -1;
    }
  }
  return 0;
}

// Structure to hold the quick sort comparison data.
struct SortComparator {
  SortComparator() {}
  char *input_;
  unsigned int input_size_;
  unsigned int idx_start_;
  unsigned int num_chars_;
  bool use_repeats_;
};

SortComparator sort_comparator;

int sort_cmp_fn(const void *a, const void *b) {
  return compare_strings(sort_comparator.input_, sort_comparator.input_size_,
  *(unsigned int*)a, *(unsigned int*)b, sort_comparator.idx_start_,
  sort_comparator.num_chars_, sort_comparator.use_repeats_);
}

void do_sort(char *input, unsigned int &input_size,
    unsigned int *indices, const unsigned int &sz,
    const unsigned int &idx_start, const unsigned int &num_chars,
    const unsigned int &start, const unsigned int &end,
    const bool &do_recurse) {

  static unsigned int count = 0;
  if (count % 10000 == 0) {
    cerr << ((double)start/sz)*100 << "\t\r";
  }
  ++count;

  unsigned int num_to_compare = num_chars;
  bool use_repeats = false;
  if (!do_recurse || idx_start/num_chars >= RECURSIVE_DEPTH) {
    num_to_compare = input_size;
    use_repeats = true;
  }
  sort_comparator.input_ = input;
  sort_comparator.input_size_ = input_size;
  sort_comparator.idx_start_ = idx_start;
  sort_comparator.num_chars_ = num_to_compare;
  sort_comparator.use_repeats_ = use_repeats;
  qsort(indices+start, end-start+1, sizeof(unsigned int), sort_cmp_fn);

  bool prev_idx_set = false;
  unsigned int prev_idx = 0;
  bool run = false;
  unsigned int run_start = 0;
  // Perform recurssive sorts for those subsets of indices which are
  // not sorted.
  for (unsigned int i = start; i <= end; ++i) {
    if (prev_idx_set) {
      int cmp = compare_strings(input, input_size, indices[i],
          indices[prev_idx], idx_start, num_to_compare, use_repeats);
      if (cmp > 0) {
        if (!run) {
        } else {
          bool recurse = true;
          if (prev_idx - run_start >= REDUCE_FACTOR*(end - start)) {
            recurse = false;
          }
          do_sort(input, input_size, indices, sz, idx_start+num_to_compare,
              num_to_compare, run_start, prev_idx, recurse);
        }
        run = false;
      } else {
        if (!run)
          run_start = prev_idx;
        run = true;
      }
    }
    prev_idx = i;
    prev_idx_set = true;
  }
  if (run) {
    bool recurse = true;
    if (end - run_start >= REDUCE_FACTOR*(end - start)) {
      recurse = false;
    }
    do_sort(input, input_size, indices, sz, idx_start+num_to_compare,
        num_to_compare, run_start, end, recurse);
  }
  for (int k = start; k <= end; ++k) {
    (*rank_cache)[indices[k]] = k;
  }
}

// Function to calculate repeats at a particular index. Calculates
// repeats given the previous index.
unsigned int find_repeats(const char *input, const unsigned int input_size,
    const unsigned int &idx, const unsigned int &prev_idx,
    const unsigned int &prev_repeat, const bool &first_idx) {
  if (!first_idx) {
    if (prev_idx + prev_repeat > idx) {
      return prev_repeat - (idx - prev_idx);
    }
  }
  unsigned int ret = 1;
  for (unsigned int i = idx+1; i < input_size && input[i] == input[i-1]; ++i) {
    ++ret;
  }
  return ret;
}

int index_compare(const void * a, const void * b) {
  unsigned int lhs = *(unsigned int*)a;
  unsigned int rhs = *(unsigned int*)b;
  if (lhs < rhs) return -1;
  else if (lhs > rhs) return 1;
  return 0;
}

void call_sort(const std::string &dna_file, const std::string &suffix_file,
    const std::string &output_file, const int &num_indices) {
  // Read input string
  int fd;
  char *input;
  unsigned int input_size = 0;
  struct stat sbuf;

  // Put the input string to a shared memory location.
  if ((fd = open(dna_file.c_str(), O_RDONLY)) == -1) {
    cerr << "Error opening: " << dna_file << endl;
    return;
  }

  if (stat(dna_file.c_str(), &sbuf) == -1) {
    cerr << "Unable to stat: " << dna_file << endl;
    return;
  }

  if ((input = (char*)mmap((caddr_t)0, sbuf.st_size, PROT_READ, MAP_SHARED, fd, 0)) == (caddr_t)(-1)) {
    cerr << "Unable to mmap" << endl;
    close(fd);
    return;
  }
  input_size = sbuf.st_size;

  repeats = new repeats_t();
  rank_cache = new rank_t();

  // Read indices
  unsigned int *indices = NULL;
  unsigned int sz = 0;
  if (dbg) {
    sz = input_size;
  } else {
    sz = num_indices;
  }
  indices = new unsigned int[sz];

  if (dbg) {
    unsigned int idx, prev_idx = 0, prev_repeat = 0;
    bool first_idx = true;
    for (unsigned int i = 0; i < input_size; ++i) {
      indices[i] = i;
      unsigned int repeat = find_repeats(input, input_size, i, prev_idx, prev_repeat, first_idx);
      (*repeats)[i] = repeat;
      prev_idx = i;
      prev_repeat = repeat;
      first_idx = false;
    }
  } else {
    ifstream ifs(suffix_file.c_str());
    unsigned int idx, k = 0, max_idx = 0;
    while (ifs >> idx) {
      indices[k++] = idx;
      max_idx = max(max_idx, idx);
    }
    ifs.close();
    if (max_idx > INT_MAX) {
      qsort(indices, sz, sizeof(unsigned int), index_compare);
    }
    unsigned int prev_idx = 0, prev_repeat = 0;
    bool first_idx = true;
    for (int i = 0; i < sz; ++i) {
      unsigned int index = indices[i];
      unsigned int repeat = find_repeats(input, input_size, index, prev_idx, prev_repeat, first_idx);
      (*repeats)[index] = repeat;
      prev_idx = index;
      prev_repeat = repeat;
      first_idx = false;
    }
  }

  unsigned int num_chars = COMPARE_WINDOW;
  do_sort(input, input_size, indices, sz, 0, num_chars, 0, sz-1, true);
  delete repeats;
  delete rank_cache;
  ofstream ofs(output_file.c_str());
#ifdef BWT
  char *output = new char[sz];
  for (unsigned int i = 0; i < sz; ++i) {
    unsigned int k = indices[i];
    if (k == 0) {
      k = input_size;
    }
    output[i] = input[k-1];
  }
  munmap(input, input_size);
  close(fd);
  ofs << output << endl;
  delete output;
#else
  munmap(input, input_size);
  close(fd);
  ofs.write((char*)indices, sz*sizeof(int));
#endif
  ofs.close();
}

JNIEXPORT void JNICALL
Java_bucket_1sort_PSort_do_1sort(JNIEnv *env, jobject obj,
    jstring dna_file, jstring suffix_file, jstring output_file, jint num_indices, jint shard)
{
  const char *filename = env->GetStringUTFChars(dna_file, 0);
  const char *suffix_filename = env->GetStringUTFChars(suffix_file, 0);
  const char *output_filename = env->GetStringUTFChars(output_file, 0);
  cout << "Filename: " << filename << endl;
  cout << "Suffix Filename: " << suffix_filename << endl;
  cout << "Output Filename: " << output_filename << endl;
  time_t start_time = time(NULL);
  cout << "Partition: " << shard << endl;
  call_sort(filename, suffix_filename, output_filename, num_indices);
  time_t end_time = time(NULL);
  cout << "Time taken: " << end_time - start_time << endl;
  return;
}

int main(int argc, char *argv[]) {
  if (argc < 2) {
    cerr << "Usage: " << argv[0] << " <input-file>" << endl;
    return 0;
  }
  time_t start_time = time(NULL);
  dbg = 1;
  call_sort(argv[1], argv[2], argv[3], 30956775);
  time_t end_time = time(NULL);
  cout << "Time taken: " << end_time - start_time << endl;
  return 0;
}
