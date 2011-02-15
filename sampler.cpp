#include <stdlib.h>
#include <ctype.h>
#include <time.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <vector>

#define RAND_SKIP 1000

using namespace std;

struct CharRun {
  CharRun(const char &c, const unsigned int &l): data(tolower(c)), len(l) {}
  char data;
  unsigned int len;
};

typedef vector<CharRun> Sample;
typedef vector<Sample> Samples;

// Function to compare two sample points.
bool compare_samples(const Sample &lhs, const Sample &rhs) {
  unsigned int i = 0, j = 0, ci = lhs[0].len, cj = rhs[0].len;
  size_t lhs_sz = lhs.size(), rhs_sz = rhs.size();
  // We can decrement a lot if for the first characters.
  unsigned int dec = min(ci, cj);
  while (i < lhs_sz && j < rhs_sz && lhs[i].data == rhs[j].data) {
    ci -= dec;
    cj -= dec;
    dec = 1;
    if (ci == 0) {
      ++i;
      if (i < lhs_sz) {
        ci = lhs[i].len;
      }
    }
    if (cj == 0) {
      ++j;
      if (j < rhs_sz) {
        cj = rhs[j].len;
      }
    }
  }
  if (i == lhs_sz) {
    if (j == rhs_sz) return false;
    else return true;
  }
  else if (j == rhs_sz) return false;
  else return lhs[i].data < rhs[j].data;
}

int main(int argc, char *argv[]) {
  if (argc < 3) {
    cerr << "Usage: " << argv[0] << " <dna-file> <num-splits>" << endl;
    return -1;
  }
  unsigned int num_splits = atoi(argv[2]);
  if (num_splits < 1) num_splits = 1;
  ifstream ifs(argv[1]);
  if (!ifs) {
    cerr << "Error reading file: " << argv[1] << endl;
    return -1;
  }

  string dna;
  ifs >> dna;
  srand (time(NULL));

  Samples samples;
  Sample prev_sample;
  unsigned int pos = 0;
  unsigned int prev_pos = 0;
  unsigned int dna_length = dna.size();
  pos += RAND_SKIP;//rand()%RAND_SKIP + 1;
  while (pos < dna_length - 16) {
    Sample sample;
    // If previous sample points count is greater than the jump,
    // the new sample at the jump can be calculated from the previous
    // sample.
    if (prev_sample.size() && pos-prev_pos < prev_sample[0].len) {
      sample = prev_sample;
      sample[0].len = prev_sample[0].len - (pos-prev_pos);
    } else {
      unsigned int uniq_cnt = 0;
      unsigned int char_cnt = 0;
      unsigned int i = 1;
      char last_char = dna[pos];
      // We make sure that we have atleast 15 chars with atleast 2 unique
      // characters
      while (pos+i < dna_length && (uniq_cnt < 2 || char_cnt < 15)) {
        unsigned int len = 1;
        // Get the run length for each character in the sample.
        while (pos+i < dna_length && dna[pos+i] == last_char) {
          last_char = dna[pos+i];
          ++len;
          ++i;
        }
        sample.push_back(CharRun(last_char, len));
        char_cnt += len;
        if (pos+i < dna_length) {
          last_char = dna[pos+i];
          ++i;
          if (pos+i == dna_length) {
            sample.push_back(CharRun(last_char, 1));
          }
        }
        ++uniq_cnt;
      }
      if (pos+i == dna_length) {
        sample.push_back(CharRun(0, 1));
      }
    }
    prev_sample = sample;
    prev_pos = pos;
    samples.push_back(sample);
    pos += RAND_SKIP;//rand()%RAND_SKIP + 1;
  }

  // Sort the samples and get the boundary.
  sort(samples.begin(), samples.end(), compare_samples);
  for (unsigned int i = 1; i < num_splits; ++i) {
    unsigned int index = (samples.size()*i)/num_splits;
    for (unsigned int j = 0; j < samples[index].size(); ++j) {
      cout << samples[index][j].data << "-" << samples[index][j].len;
      if (j < samples[index].size()-1) {
        cout << ",";
      }
    }
    cout << endl;
  }
  return 0;
}

