/// MEX function arguments helper library.
///
/// Kota Yamaguchi 2013 <kyamagu@cs.stonybrook.edu>

#include "arguments.h"

namespace mex {

void CheckInputArguments(int min_args, int max_args, int nrhs) {
  if (nrhs < min_args)
    mexErrMsgIdAndTxt("mex:error",
                      "Missing input arguments: %d for %d to %d.",
                      nrhs,
                      min_args,
                      max_args);
  if (nrhs > max_args)
    mexErrMsgIdAndTxt("mex:error",
                      "Too many input arguments: %d for %d to %d.",
                      nrhs,
                      min_args,
                      max_args);
}

void CheckOutputArguments(int min_args, int max_args, int nlhs) {
  if (nlhs < min_args)
    mexErrMsgIdAndTxt("mex:error",
                      "Missing output arguments: %d for %d to %d.",
                      nlhs,
                      min_args,
                      max_args);
  if (nlhs > max_args)
    mexErrMsgIdAndTxt("mex:error",
                      "Too many output arguments: %d for %d to %d.",
                      nlhs,
                      min_args,
                      max_args);
}

VariableInputArguments::~VariableInputArguments() {
  for (std::map<std::string, MxArray>::iterator it = entries_.begin();
       it != entries_.end(); ++it)
    it->second.destroy();
}

void VariableInputArguments::update(const mxArray** begin,
                                    const mxArray** end) {
  // Skip until the first key.
  while (begin < end && !MxArray(*begin).isChar())
    ++begin;
  while (begin < end) {
    std::string key = MxArray(*(begin++)).toString();
    EntryIterator it = entries_.find(key);
    if (it == entries_.end())
      mexErrMsgIdAndTxt("mex:arguments",
                        "Invalid option specified: %s",
                        key.c_str());
    // Allow empty value to implicitly specify binary options.
    if ((*it).second.isLogical() && 
        (begin == end || MxArray(*begin).isChar()))
        entries_[key] = MxArray(true);
    // Otherwise, require a value input.
    else if (begin < end)
      entries_[key].reset(*(begin++));
    else
      mexErrMsgIdAndTxt("mex:arguments",
                        "Missing value for option: %s",
                        key.c_str());
  }
}

const MxArray& VariableInputArguments::operator [](
    const std::string& key) const {
  EntryIterator it = entries_.find(key);
  if (it == entries_.end())
    mexErrMsgIdAndTxt("mex:arguments",
                      "Invalid option specified: %s",
                      key.c_str());
  return (*it).second;
}

} // namespace mex
