/// MEX function arguments helper library.
///
/// Kota Yamaguchi 2013 <kyamagu@cs.stonybrook.edu>

#ifndef __MEX_ARGUMENTS_H__
#define __MEX_ARGUMENTS_H__

#include "function.h"
#include "mxarray.h"

namespace mex {

/// Check number of input arguments.
void CheckInputArguments(int min_args, int max_args, int nlhs);

/// Check number of output arguments.
void CheckOutputArguments(int min_args, int max_args, int nlhs);

/// Utility to parse key-value input arguments.
///
/// Usage:
///
///     VariableInputArguments options;
///     options.set("IntegerOption", 1)
///     options.set("StringOption",  "")
///     options.set("BooleanOption", false);
///     arguments.update(nrhs-2, &prhs[2]);
///     myCFunction(options["IntegerOption"].toInt(),
///                 options["StringOption"].toString(),
///                 options["BooleanOption"].toBool());
///
class VariableInputArguments {
public:
  typedef std::map<std::string, MxArray>::const_iterator EntryIterator;

  /// Constructor.
  VariableInputArguments() {}
  /// Destructor.
  virtual ~VariableInputArguments();
  /// Consecutive insertion operator.
  template <typename T>
  void set(const std::string& key, const T& value) {
    entries_[key] = MxArray(value);
  }
  /// Update operation.
  void update(const mxArray** begin, const mxArray** end);
  /// Lookup operator.
  const MxArray& operator [](const std::string& key) const;
  /// Show debugging messages to stdout.
  void show() const {
    for (std::map<std::string, MxArray>::const_iterator it = entries_.begin();
         it != entries_.end(); ++it) {
      if (it->second.isLogical())
        mexPrintf("%s: %d\n", it->first.c_str(), it->second.toBool());
      else if (it->second.isNumeric())
        mexPrintf("%s: %g\n", it->first.c_str(), it->second.toDouble());
      else if (it->second.isChar())
        mexPrintf("%s: %s\n", it->first.c_str(), it->second.toString().c_str());
    }
  }

private:
  /// key-value storage.
  std::map<std::string, MxArray> entries_;
};

} // namespace mex

#endif // __MEX_ARGUMENTS_H__