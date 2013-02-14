// Berkeley DB matlab driver library.
//
// Kota Yamaguchi 2012 <kyamagu@cs.stonybrook.edu>

#ifdef ENABLE_ZLIB
#include <zlib.h>
#endif
#include "bdbmex.h"

namespace bdbmex {

Record::Record() {
  reset(DB_DBT_REALLOC, DB_DBT_REALLOC);
}

Record::Record(const mxArray* key) {
  reset(DB_DBT_USERMEM, DB_DBT_REALLOC);
  set_key(key);
}

Record::Record(const mxArray* key, const mxArray* value) {
  reset(DB_DBT_USERMEM, DB_DBT_USERMEM);
  set_key(key);
  set_value(value);
}

Record::~Record() {
  if (key_.flags == DB_DBT_REALLOC && key_.data)
    free(key_.data);
  if (value_.flags == DB_DBT_REALLOC && value_.data)
    free(value_.data);
}

void Record::reset(u_int32_t key_flags, u_int32_t value_flags) {
  memset(&key_, 0, sizeof(DBT));
  memset(&value_, 0, sizeof(DBT));
  key_.flags = key_flags;
  value_.flags = value_flags;
}

void Record::set_key(const mxArray* key) {
  serialize_mxarray(key, &key_buffer_);
  key_.data = &key_buffer_[0];
  key_.size = key_buffer_.size();
}

void Record::set_value(const mxArray* value) {
  compress_mxarray(value, &value_buffer_);
  value_.data = &value_buffer_[0];
  value_.size = value_buffer_.size();
}

void Record::get_key(mxArray** key) {
  const uint8_t* key_data = static_cast<const uint8_t*>(key_.data);
  key_buffer_.assign(key_data, key_data + key_.size);
  deserialize_mxarray(key_buffer_, key);
}

void Record::get_value(mxArray** value) {
  const uint8_t* value_data = static_cast<const uint8_t*>(value_.data);
  value_buffer_.assign(value_data, value_data + value_.size);
  decompress_mxarray(value_buffer_, value);
}

void Record::serialize_mxarray(const mxArray* value, vector<uint8_t>* binary) {
  mxArray* serialized_array = static_cast<mxArray*>(mxSerialize(value));
  if (serialized_array == NULL)
    ERROR("Failed to serialize mxArray.");
  const uint8_t* data = static_cast<uint8_t*>(mxGetData(serialized_array));
  binary->assign(data, data + mxGetNumberOfElements(serialized_array));
  mxDestroyArray(serialized_array);
}

void Record::deserialize_mxarray(const vector<uint8_t>& binary,
                                 mxArray** value) {
  *value = static_cast<mxArray*>(mxDeserialize(&binary[0], binary.size()));
  if (value == NULL)
    ERROR("Failed to deserialize mxArray.");
}

#ifdef ENABLE_ZLIB

void Record::compress_mxarray(const mxArray* value, vector<uint8_t>* binary) {
  mxArray* serialzed_array = static_cast<mxArray*>(mxSerialize(value));
  uLongf array_size = mxGetNumberOfElements(serialzed_array);
  vector<uint8_t> buffer(compressBound(array_size));
  uLongf actual_size = buffer.size();
  int code_ = compress(&buffer[0],
                       &actual_size,
                       static_cast<const Bytef*>(mxGetData(serialzed_array)),
                       array_size);
  binary->resize(sizeof(uLongf) + actual_size);
  memcpy(&(*binary)[0], &array_size, sizeof(uLongf));
  copy(buffer.begin(),
       buffer.begin() + actual_size,
       binary->begin() + sizeof(uLongf));
  mxDestroyArray(serialzed_array);
  if (code_ != Z_OK)
    ERROR("Fatal error in compress_mxarray");
}

void Record::decompress_mxarray(const vector<uint8_t>& binary,
                                mxArray** value) {
  if (binary.size() <= sizeof(uint32_t))
    ERROR("Fatal error in decompress_mxarray: invalid binary.");
  uLongf array_size = 0;
  memcpy(&array_size, &binary[0], sizeof(uLongf));
  vector<uint8_t> buffer(array_size);
  uLongf actual_size = array_size;
  int code_ = uncompress(&buffer[0],
                         &actual_size,
                         &binary[0] + sizeof(uLongf),
                         binary.size() - sizeof(uLongf));
  *value = static_cast<mxArray*>(mxDeserialize(&buffer[0], buffer.size()));
  if (code_ != Z_OK)
    ERROR("Fatal error in decompress_mxarray: code = %d.", code_);
}

#else

void Record::compress_mxarray(const mxArray* value, vector<uint8_t>* binary) {
  serialize_mxarray(value, binary);
}

void Record::decompress_mxarray(const vector<uint8_t>& binary,
                                mxArray** value) {
  deserialize_mxarray(binary, value);
}

#endif // ENABLE_ZLIB

Database::Database() : code_(0), database_(NULL), environment_(NULL) {}

Database::~Database() {
  close();
}

bool Database::open(const string& filename, const string& home_dir) {
  if (home_dir.empty()) {
    environment_ = NULL;
  }
  else {
    u_int32_t env_flags = DB_CREATE    |
                          DB_INIT_TXN  |
                          DB_INIT_LOCK |
                          DB_INIT_LOG  |
                          DB_INIT_MPOOL;
    code_ = db_env_create(&environment_, 0);
    if (!ok()) return false;
    code_ = environment_->open(environment_, 
                               home_dir.c_str(),
                               env_flags,
                               0);
    if (!ok()) return false;
    code_ = environment_->set_flags(environment_, DB_AUTO_COMMIT, 1);
    if (!ok()) return false;
  }
  code_ = db_create(&database_, environment_, 0);
  if (!ok()) return false;
  code_ = database_->open(database_,
                          NULL,
                          filename.c_str(),
                          NULL,
                          DB_BTREE,
                          DB_CREATE,
                          0);
  return ok();
}

bool Database::close() {
  if (database_) {
    code_ = database_->close(database_, 0);
    database_ = NULL;
  }
  if (!ok()) return false;
  if (environment_) {
    code_ = environment_->close(environment_, 0);
    environment_ = NULL;
  }
  return ok();
}

int Database::error_code() {
  return code_;
}

const char* Database::error_message() {
  return db_strerror(code_);
}

bool Database::get(const mxArray* key, mxArray** value) {
  Record record(key);
  code_ = database_->get(database_, NULL, record.key(), record.value(), 0);
  if (code_ == 0)
    record.get_value(value);
  else if (code_ == DB_NOTFOUND)
    *value = mxCreateDoubleMatrix(0, 0, mxREAL);
  return ok() || (code_ == DB_NOTFOUND);
}

bool Database::put(const mxArray* key, const mxArray* value) {
  Record record(key, value);
  code_ = database_->put(database_, NULL, record.key(), record.value(), 0);
  return ok();
}

bool Database::del(const mxArray* key) {
  Record record(key);
  code_ = database_->del(database_, NULL, record.key(), 0);
  return ok();
}

bool Database::exists(const mxArray* key, mxArray** value) {
  Record record(key);
  code_ = database_->exists(database_, NULL, record.key(), 0);
  *value = mxCreateLogicalScalar(ok());
  return ok() || code_ == DB_NOTFOUND;
}

bool Database::stat(mxArray** output) {
  DB_BTREE_STAT* stats = NULL;
  stats = static_cast<DB_BTREE_STAT*>(malloc(sizeof(DB_BTREE_STAT)));
  const char* kFields[] = {"magic", "minkey", "ndata", "nkeys",
      "pagecnt", "pagesize", "re_len", "re_pad", "version"};
  code_ = database_->stat(database_, NULL, &stats, 0);
  *output = mxCreateStructMatrix(1, 1, 9, kFields);
  mxSetField(*output, 0, kFields[0], mxCreateDoubleScalar(stats->bt_magic));
  mxSetField(*output, 0, kFields[1], mxCreateDoubleScalar(stats->bt_minkey));
  mxSetField(*output, 0, kFields[2], mxCreateDoubleScalar(stats->bt_ndata));
  mxSetField(*output, 0, kFields[3], mxCreateDoubleScalar(stats->bt_nkeys));
  mxSetField(*output, 0, kFields[4], mxCreateDoubleScalar(stats->bt_pagecnt));
  mxSetField(*output, 0, kFields[5], mxCreateDoubleScalar(stats->bt_pagesize));
  mxSetField(*output, 0, kFields[6], mxCreateDoubleScalar(stats->bt_re_len));
  mxSetField(*output, 0, kFields[7], mxCreateDoubleScalar(stats->bt_re_pad));
  mxSetField(*output, 0, kFields[8], mxCreateDoubleScalar(stats->bt_version));
  free(stats);
  return ok();
}

bool Database::keys(mxArray** output) {
  // Count the number of keys.
  DB_BTREE_STAT* stats = NULL;
  stats = static_cast<DB_BTREE_STAT*>(malloc(sizeof(DB_BTREE_STAT)));
  code_ = database_->stat(database_, NULL, &stats, 0);
  uint32_t num_keys = stats->bt_nkeys;
  free(stats);
  if (code_)
    return false;
  // Retrieve records.
  Cursor cursor;
  code_ = cursor.open(database_);
  if (code_)
    return false;
  *output = mxCreateCellMatrix(num_keys, 1);
  int index = 0;
  Record record;
  while (index < num_keys && 0 == (code_ = cursor.next(&record))) {
    mxArray* key_array;
    record.get_key(&key_array);
    mxSetCell(*output, index++, key_array);
  }
  return ok() || (code_ == DB_NOTFOUND);
}

bool Database::values(mxArray** output) {
  // Count the number of values.
  DB_BTREE_STAT* stats = NULL;
  stats = static_cast<DB_BTREE_STAT*>(malloc(sizeof(DB_BTREE_STAT)));
  code_ = database_->stat(database_, NULL, &stats, 0);
  uint32_t num_values = stats->bt_ndata;
  free(stats);
  if (code_)
    return false;
  // Retrieve records.
  Cursor cursor;
  code_ = cursor.open(database_);
  if (code_)
    return false;
  *output = mxCreateCellMatrix(num_values, 1);
  int index = 0;
  Record record;
  while (index < num_values && 0 == (code_ = cursor.next(&record))) {
    mxArray* value_array;
    record.get_value(&value_array);
    mxSetCell(*output, index++, value_array);
  }
  return ok() || (code_ == DB_NOTFOUND);
}

Sessions::Sessions() : last_id_(0) {}

Sessions::~Sessions() {}

int Sessions::open(const string& filename, const string& home_dir) {
  if (!connections_[++last_id_].open(filename, home_dir)) {
    connections_.erase(last_id_);
    ERROR("Unable to open: %s.", filename.c_str());
  }
  return last_id_;
}

void Sessions::close(int id) {
  connections_.erase(id);
}

int Sessions::default_id() const {
  return (connections_.empty()) ? 0 : connections_.rbegin()->first;
}

int Sessions::last_id() const {
  return last_id_;
}

Database* Sessions::get(int id) {
  map<int, Database>::iterator connection = connections_.find(id);
  if (connection == connections_.end())
    ERROR("Invalid session id: %d. Did you open the database?", id);
  return &connection->second;
}

Sessions Operation::sessions_ = Sessions();

Operation* OperationFactory::parse(int nrhs, const mxArray *prhs[]) {
  if (nrhs < 1 || !mxIsChar(prhs[0]))
    ERROR("Invalid argument: missing operation.");
  string operation_name(mxGetChars(prhs[0]),
                        mxGetChars(prhs[0]) + mxGetNumberOfElements(prhs[0]));
  Operation* operation = OperationFactory::create(operation_name);
  if (operation == NULL)
    ERROR("Invalid operation: %s", operation_name.c_str());
  return operation;
}

void OperationFactory::define(const string& alias,
                              OperationCreator* creator) {
  get_registry()->insert(make_pair(alias, creator));
}

Operation* OperationFactory::create(const string& alias) {
  map<string, OperationCreator*>::const_iterator it =
      get_registry()->find(alias);
  if (it == get_registry()->end())
    return static_cast<Operation*>(NULL);
  else
    return it->second->create();
}

map<string, OperationCreator*>* OperationFactory::get_registry() {
  static map<string, OperationCreator*> registry;
  return &registry;
}

OperationCreator::OperationCreator(const string& name) {
  OperationFactory::define(name, this);
}

} // namespace dbmex