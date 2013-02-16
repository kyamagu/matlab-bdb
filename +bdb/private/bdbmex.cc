// Berkeley DB mex interface.
//
// Kota Yamaguchi 2012 <kyamagu@cs.stonybrook.edu>

#include "libbdbmex.h"
#include "mex_helpers.h"

using bdbmex::Sessions;
using bdbmex::Database;

namespace {

// Convert mxArray to string.
string StringFromMxArray(const mxArray* array) {
  return string(mxGetChars(array),
                mxGetChars(array) + mxGetNumberOfElements(array));
}

MEX_API(open) (int nlhs, mxArray *plhs[], int nrhs, const mxArray *prhs[]) {
  if (nlhs > 1)
    ERROR("Too many output: %d for 1.", nlhs);
  if (nrhs < 1 || nrhs > 2)
    ERROR("Wrong number of arguments: %d for 1 or 2.", nrhs);
  if (!mxIsChar(prhs[0]) || (nrhs == 2 && !mxIsChar(prhs[1])))
    ERROR("Invalid filename is given.");
  string filename(StringFromMxArray(prhs[0]));
  string home_dir((nrhs == 1) ? "" : StringFromMxArray(prhs[1]));
  plhs[0] = mxCreateDoubleScalar(Sessions::open(filename, home_dir));
}

MEX_API(close) (int nlhs, mxArray *plhs[], int nrhs, const mxArray *prhs[]) {
  if (nlhs)
    ERROR("Too many output: %d for 0.", nlhs);
  if (nrhs > 1 || (nrhs == 1 && !mxIsNumeric(prhs[0])))
    ERROR("Invalid input.");
  int id = (nrhs == 0) ? Sessions::default_id() : mxGetScalar(prhs[0]);
  Sessions::close(id);
}

MEX_API(get) (int nlhs, mxArray *plhs[], int nrhs, const mxArray *prhs[]) {
  if (nlhs > 1)
    ERROR("Too many output: %d for 1.", nlhs);
  if (nrhs > 2)
    ERROR("Too many input: %d for 1 or 2.", nrhs);
  Database* connection = NULL;
  const mxArray* key = NULL;
  if (nrhs == 1) {
    connection = Sessions::get(Sessions::default_id());
    key = prhs[0];
  }
  else if (mxIsNumeric(prhs[0])) {
    connection = Sessions::get(mxGetScalar(prhs[0]));
    key = prhs[1];
  }
  else
    ERROR("Invalid input.");
  if (!connection->get(key, &plhs[0]))
    ERROR("Failed to get an entry: %s", connection->error_message());
}

MEX_API(put) (int nlhs, mxArray *plhs[], int nrhs, const mxArray *prhs[]) {
  if (nlhs > 0)
    ERROR("Too many output: %d for 1.", nlhs);
  if (nrhs > 3)
    ERROR("Too many input: %d for 2 or 3.", nrhs);
  Database* connection = NULL;
  const mxArray* key = NULL;
  const mxArray* value = NULL;
  if (nrhs == 2) {
    connection = Sessions::get(Sessions::default_id());
    key = prhs[0];
    value = prhs[1];
  }
  else if (mxIsNumeric(prhs[0])) {
    connection = Sessions::get(mxGetScalar(prhs[0]));
    key = prhs[1];
    value = prhs[2];
  }
  else
    ERROR("Invalid input.");
  if (!connection->put(key, value))
    ERROR("Failed to put an entry: %s", connection->error_message());
}

MEX_API(delete) (int nlhs, mxArray *plhs[], int nrhs, const mxArray *prhs[]) {
  if (nlhs > 0)
    ERROR("Too many output: %d for 0.", nlhs);
  if (nrhs > 2)
    ERROR("Too many input: %d for 1 or 2.", nrhs);
  Database* connection = NULL;
  const mxArray* key = NULL;
  if (nrhs == 1) {
    connection = Sessions::get(Sessions::default_id());
    key = prhs[0];
  }
  else if (mxIsNumeric(prhs[0])) {
    connection = Sessions::get(mxGetScalar(prhs[0]));
    key = prhs[1];
  }
  else
    ERROR("Invalid input.");
  if (!connection->del(key))
    ERROR("Failed to delete an entry: %s", connection->error_message());
}

MEX_API(exists) (int nlhs, mxArray *plhs[], int nrhs, const mxArray *prhs[]) {
  if (nlhs > 1)
    ERROR("Too many output: %d for 1.", nlhs);
  if (nrhs > 2)
    ERROR("Too many input: %d for 1 or 2.", nrhs);
  Database* connection = NULL;
  const mxArray* key = NULL;
  if (nrhs == 1) {
    connection = Sessions::get(Sessions::default_id());
    key = prhs[0];
  }
  else if (mxIsNumeric(prhs[0])) {
    connection = Sessions::get(mxGetScalar(prhs[0]));
    key = prhs[1];
  }
  else
    ERROR("Invalid input.");
  if (!connection->exists(key, &plhs[0]))
    ERROR("Failed to query a key: %s", connection->error_message());
}

MEX_API(stat) (int nlhs, mxArray *plhs[], int nrhs, const mxArray *prhs[]) {
  if (nlhs > 1)
    ERROR("Too many output: %d for 1.", nlhs);
  if (nrhs > 1)
    ERROR("Too many input: %d for 0 or 1.", nrhs);
  Database* connection = NULL;
  if (nrhs == 0)
    connection = Sessions::get(Sessions::default_id());
  else if (mxIsNumeric(prhs[0]))
    connection = Sessions::get(mxGetScalar(prhs[0]));
  else
    ERROR("Invalid input.");
  if (!connection->stat(&plhs[0]))
    ERROR("Failed to query stat: %s", connection->error_message());
}

MEX_API(keys) (int nlhs, mxArray *plhs[], int nrhs, const mxArray *prhs[]) {
  if (nlhs > 1)
    ERROR("Too many output: %d for 1.", nlhs);
  if (nrhs > 1)
    ERROR("Too many input: %d for 0 or 1.", nrhs);
  Database* connection = NULL;
  if (nrhs == 0)
    connection = Sessions::get(Sessions::default_id());
  else if (mxIsNumeric(prhs[0]))
    connection = Sessions::get(mxGetScalar(prhs[0]));
  else
    ERROR("Invalid input.");
  if (!connection->keys(&plhs[0]))
    ERROR("Failed to query keys: %s", connection->error_message());
}

MEX_API(values) (int nlhs, mxArray *plhs[], int nrhs, const mxArray *prhs[]) {
  if (nlhs > 1)
    ERROR("Too many output: %d for 1.", nlhs);
  if (nrhs > 1)
    ERROR("Too many input: %d for 0 or 1.", nrhs);
  Database* connection = NULL;
  if (nrhs == 0)
    connection = Sessions::get(Sessions::default_id());
  else if (mxIsNumeric(prhs[0]))
    connection = Sessions::get(mxGetScalar(prhs[0]));
  else
    ERROR("Invalid input.");
  if (!connection->values(&plhs[0]))
    ERROR("Failed to query values: %s", connection->error_message());
}

} // namespace