// MxArray data conversion library.
//
// Kota Yamaguchi 2013 <kyamagu@cs.stonybrook.edu>

#include "mxarray.h"

namespace mex {

MxArray::MxArray() : array_(NULL), mutable_array_(NULL) {}

MxArray::MxArray(mxArray* array) : array_(array), mutable_array_(array) {}

MxArray::MxArray(const mxArray* array) : array_(array), mutable_array_(NULL) {}

MxArray::MxArray(const MxArray& mxarray) :
    array_(mxarray.array_), mutable_array_(mxarray.mutable_array_) {}

MxArray& MxArray::operator=(const MxArray& rhs) {
  if (this != &rhs)
    this->array_ = rhs.array_;
  return *this;
}

MxArray::MxArray(const int value) :
    mutable_array_(mxCreateDoubleScalar(static_cast<double>(value))) {
  array_ = mutable_array_;
  if (!array_)
    mexErrMsgIdAndTxt("mxarray:error", "Null pointer exception.");
}

MxArray::MxArray(const double value) :
    mutable_array_(mxCreateDoubleScalar(value)) {
  array_ = mutable_array_;
  if (!array_)
    mexErrMsgIdAndTxt("mxarray:error", "Null pointer exception.");
}

MxArray::MxArray(const bool value) :
    mutable_array_(mxCreateLogicalScalar(value)) {
  array_ = mutable_array_;
  if (!array_)
    mexErrMsgIdAndTxt("mxarray:error", "Null pointer exception.");
}

MxArray::MxArray(const std::string& value) :
    mutable_array_(mxCreateString(value.c_str())) {
  array_ = mutable_array_;
  if (!array_)
    mexErrMsgIdAndTxt("mxarray:error", "Null pointer exception.");
}

MxArray::MxArray(std::vector<MxArray>* values) :
    array_(NULL), mutable_array_(NULL) {
  if (!values)
    mexErrMsgIdAndTxt("mxarray:error", "Null pointer exception.");
  mutable_array_ = mxCreateCellMatrix(1, values->size());
  array_ = mutable_array_;
  if (!array_)
    mexErrMsgIdAndTxt("mxarray:error", "Null pointer exception.");
  for (int i = 0; i < values->size(); ++i)
    set(i, &(*values)[i]);
}

MxArray::MxArray(int nfields, const char** fields, int rows, int columns) :
    mutable_array_(mxCreateStructMatrix(rows, columns, nfields, fields)) {
  array_ = mutable_array_;
  if (!array_)
    mexErrMsgIdAndTxt("mxarray:error", "Null pointer exception.");
}

MxArray MxArray::Cell(int rows, int columns) {
  mxArray* cell_array = mxCreateCellMatrix(rows, columns);
  if (!cell_array)
    mexErrMsgIdAndTxt("mxarray:error", "Null pointer exception.");
  return MxArray(cell_array);
}

MxArray MxArray::Struct(int nfields,
                        const char** fields,
                        int rows,
                        int columns) {
  return MxArray(nfields, fields, rows, columns);
}

MxArray::~MxArray() {}

void MxArray::reset(const mxArray* array) {
  array_ = array;
  mutable_array_ = NULL;
}

void MxArray::reset(mxArray* array) {
  array_ = array;
  mutable_array_ = array;
}

MxArray MxArray::clone() {
  mxArray* array = mxDuplicateArray(array_);
  if (!array)
    mexErrMsgIdAndTxt("mxarray:error", "Null pointer exception.");
  return MxArray(array);
}

void MxArray::destroy() {
  mxDestroyArray(mutable_array_);
  reset(static_cast<mxArray*>(NULL));
}

const mxArray* MxArray::get() const {
  return array_;
}

mxArray* MxArray::getMutable() {
  if (isConst())
    mexErrMsgIdAndTxt("mxarray:error",
                      "const MxArray cannot be converted to mxArray*.");
  return mutable_array_;
}

int MxArray::toInt() const {
  if (numel() != 1)
    mexErrMsgIdAndTxt("mxarray:error", "MxArray is not a scalar.");
  return at<int>(0);
}

double MxArray::toDouble() const {
  if (numel() != 1)
    mexErrMsgIdAndTxt("mxarray:error", "MxArray is not a scalar.");
  return at<double>(0);
}

bool MxArray::toBool() const {
  if (numel() != 1)
    mexErrMsgIdAndTxt("mxarray:error", "MxArray is not a scalar.");
  return at<bool>(0);
}

std::string MxArray::toString() const {
  if (!isChar())
    mexErrMsgIdAndTxt("mxarray:error",
                      "Cannot convert %s to string.",
                      className().c_str());
  return std::string(mxGetChars(array_), mxGetChars(array_) + numel());
}

std::string MxArray::fieldName(int index) const {
  const char* field_name = mxGetFieldNameByNumber(array_, index);
  if (!field_name)
    mexErrMsgIdAndTxt("mxarray:error",
                      "Failed to get field name at %d.",
                      index);
  return std::string(field_name);
}

std::vector<std::string> MxArray::fieldNames() const {
  if (!isStruct())
    mexErrMsgIdAndTxt("mxarray:error", "MxArray is not a struct array.");
  int num_fields = nfields();
  std::vector<std::string> field_names;
  field_names.reserve(num_fields);
  for (int i = 0; i < num_fields; ++i)
    field_names.push_back(fieldName(i));
  return field_names;
}

mwIndex MxArray::subs(mwIndex row, mwIndex column) const {
  if (row < 0 || row >= rows() || column < 0 || column >= cols())
    mexErrMsgIdAndTxt("mxarray:error", "Subscript is out of range.");
  mwIndex subscripts[] = {row, column};
  return mxCalcSingleSubscript(array_, 2, subscripts);
}

mwIndex MxArray::subs(const std::vector<mwIndex>& subscripts) const {
  return mxCalcSingleSubscript(array_, subscripts.size(), &subscripts[0]);
}

MxArray MxArray::at(const std::string& field_name, mwIndex index) const {
  if (!isStruct())
    mexErrMsgIdAndTxt("mxarray:error",
                      "MxArray is not a struct array but %s.",
                      className().c_str());
  if (index < 0 || numel() <= index)
    mexErrMsgIdAndTxt("mxarray:error", "Index is out of range.");
  mxArray* array = mxGetField(array_, index, field_name.c_str());
  if (!array)
    mexErrMsgIdAndTxt("mxarray:error",
                      "Field '%s' doesn't exist",
                      field_name.c_str());
  if (isConst())
    return MxArray(static_cast<const mxArray*>(array));
  else
    return MxArray(array);
}

void MxArray::set(mwIndex index, MxArray* value) {
  if (!mutable_array_)
    mexErrMsgIdAndTxt("mxarray:error", "Null pointer exception.");
  if (!value)
    mexErrMsgIdAndTxt("mxarray:error", "Null pointer exception.");
  if (!isCell())
    mexErrMsgIdAndTxt("mxarray:error",
                      "MxArray is not a cell array but %s.",
                      className().c_str());
  if (index < 0 || numel() <= index)
    mexErrMsgIdAndTxt("mxarray:error", "Index is out of range.");
  mxSetCell(mutable_array_, index, value->getMutable());
}

void MxArray::set(const std::string& field_name,
                  MxArray* value,
                  mwIndex index) {
  if (!mutable_array_)
    mexErrMsgIdAndTxt("mxarray:error", "Null pointer exception.");
  if (!value)
    mexErrMsgIdAndTxt("mxarray:error", "Null pointer exception.");
  if (!isStruct())
    mexErrMsgIdAndTxt("mxarray:error",
                      "MxArray is not a struct array but %s.",
                      className().c_str());
  if (!isField(field_name)) {
    if (mxAddField(mutable_array_, field_name.c_str()) < 0)
      mexErrMsgIdAndTxt("mxarray:error",
                        "Failed to create a field '%s'",
                        field_name.c_str());
  }
  mxSetField(mutable_array_, index, field_name.c_str(), value->getMutable());
}

template <>
MxArray::MxArray(const std::vector<char>& values) :
    array_(NULL), mutable_array_(NULL) {
  std::string string_value(values.begin(), values.end());
  mutable_array_ = mxCreateString(string_value.c_str());
  array_ = mutable_array_;
  if (!array_)
    mexErrMsgIdAndTxt("mxarray:error", "Null pointer exception.");
}

template <>
MxArray::MxArray(const std::vector<bool>& values) :
    mutable_array_(mxCreateLogicalMatrix(1, values.size())) {
  array_ = mutable_array_;
  if (!array_)
    mexErrMsgIdAndTxt("mxarray:error", "Null pointer exception.");
  std::copy(values.begin(), values.end(), mxGetLogicals(mutable_array_));
}

template <>
MxArray::MxArray(const std::vector<std::string>& values) :
    mutable_array_(mxCreateCellMatrix(1, values.size())) {
  array_ = mutable_array_;
  if (!array_)
    mexErrMsgIdAndTxt("mxarray:error", "Null pointer exception.");
  for (int i = 0; i < values.size(); ++i) {
    MxArray value(values[i]);
    set(i, &value);
  }
}

template <>
MxArray MxArray::at(mwIndex index) const {
  if (!isCell())
    mexErrMsgIdAndTxt("mxarray:error",
                      "MxArray is not a cell array but %s.",
                      className().c_str());
  mxArray* array = mxGetCell(array_, index);
  if (!array)
    mexErrMsgIdAndTxt("mxarray:error", "Null pointer exception.");
  if (isConst())
    return MxArray(static_cast<const mxArray*>(array));
  else
    return MxArray(array);
}

template <>
std::vector<MxArray> MxArray::toVector() const {
  if (isCell()) {
    std::vector<MxArray> values(numel());
    for (int i = 0; i < values.size(); ++i)
      values[i] = at<MxArray>(i);
    return values;
  }
  else
    return std::vector<MxArray>(1, *this);
}

template <>
std::vector<std::string> MxArray::toVector() const {
  std::vector<std::string> values(numel());
  if (!isCell())
    mexErrMsgIdAndTxt("mxarray:error",
                      "Cannot convert to std::vector<std::string>.");
  for (int i = 0; i < values.size(); ++i)
    values[i] = at<MxArray>(i).toString();
  return values;
}

} // namespace mex