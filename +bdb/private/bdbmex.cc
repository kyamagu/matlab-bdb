// Berkeley DB mex interface.
//
// Kota Yamaguchi 2012 <kyamagu@cs.stonybrook.edu>

#include "libbdbmex.h"
#include "mex/function.h"
#include "mex/mxarray.h"

using mex::CheckInputArguments;
using mex::CheckOutputArguments;
using mex::MxArray;
using bdbmex::Sessions;
using bdbmex::Database;

MEX_FUNCTION(open) (int nlhs,
                    mxArray *plhs[],
                    int nrhs,
                    const mxArray *prhs[]) {
  CheckInputArguments(1, 2, nrhs);
  CheckOutputArguments(0, 1, nlhs);
  string filename(MxArray(prhs[0]).toString());
  string home_dir((nrhs == 1) ? "" : MxArray(prhs[1]).toString());
  plhs[0] = MxArray(Sessions::open(filename, home_dir)).getMutable();
}

MEX_FUNCTION(close) (int nlhs,
                     mxArray *plhs[],
                     int nrhs,
                     const mxArray *prhs[]) {
  CheckInputArguments(0, 1, nrhs);
  CheckOutputArguments(0, 0, nlhs);
  int id = (nrhs == 0) ? Sessions::default_id() : MxArray(prhs[0]).toInt();
  Sessions::close(id);
}

MEX_FUNCTION(get) (int nlhs,
                   mxArray *plhs[],
                   int nrhs,
                   const mxArray *prhs[]) {
  CheckInputArguments(1, 2, nrhs);
  CheckOutputArguments(0, 1, nlhs);
  Database* connection = NULL;
  MxArray key;
  if (nrhs == 1) {
    connection = Sessions::get(Sessions::default_id());
    key.reset(prhs[0]);
  }
  else {
    connection = Sessions::get(MxArray(prhs[0]).toInt());
    key.reset(prhs[1]);
  }
  if (!connection->get(key.get(), &plhs[0]))
    ERROR("Failed to get an entry: %s", connection->error_message());
}

MEX_FUNCTION(put) (int nlhs,
                   mxArray *plhs[],
                   int nrhs,
                   const mxArray *prhs[]) {
  CheckInputArguments(2, 3, nrhs);
  CheckOutputArguments(0, 0, nlhs);
  Database* connection = NULL;
  MxArray key, value;
  if (nrhs == 2) {
    connection = Sessions::get(Sessions::default_id());
    key.reset(prhs[0]);
    value.reset(prhs[1]);
  }
  else {
    connection = Sessions::get(MxArray(prhs[0]).toInt());
    key.reset(prhs[1]);
    value.reset(prhs[2]);
  }
  if (!connection->put(key.get(), value.get()))
    ERROR("Failed to put an entry: %s", connection->error_message());
}

MEX_FUNCTION(delete) (int nlhs,
                      mxArray *plhs[],
                      int nrhs,
                      const mxArray *prhs[]) {
  CheckInputArguments(1, 2, nrhs);
  CheckOutputArguments(0, 0, nlhs);
  Database* connection = NULL;
  MxArray key;
  if (nrhs == 1) {
    connection = Sessions::get(Sessions::default_id());
    key.reset(prhs[0]);
  }
  else {
    connection = Sessions::get(MxArray(prhs[0]).toInt());
    key.reset(prhs[1]);
  }
  if (!connection->del(key.get()))
    ERROR("Failed to delete an entry: %s", connection->error_message());
}

MEX_FUNCTION(exists) (int nlhs,
                      mxArray *plhs[],
                      int nrhs,
                      const mxArray *prhs[]) {
  CheckInputArguments(1, 2, nrhs);
  CheckOutputArguments(0, 1, nlhs);
  Database* connection = NULL;
  MxArray key;
  if (nrhs == 1) {
    connection = Sessions::get(Sessions::default_id());
    key.reset(prhs[0]);
  }
  else {
    connection = Sessions::get(MxArray(prhs[0]).toInt());
    key.reset(prhs[1]);
  }
  if (!connection->exists(key.get(), &plhs[0]))
    ERROR("Failed to query a key: %s", connection->error_message());
}

MEX_FUNCTION(stat) (int nlhs,
                    mxArray *plhs[],
                    int nrhs,
                    const mxArray *prhs[]) {
  CheckInputArguments(0, 1, nrhs);
  CheckOutputArguments(0, 1, nlhs);
  Database* connection = NULL;
  if (nrhs == 0)
    connection = Sessions::get(Sessions::default_id());
  else
    connection = Sessions::get(MxArray(prhs[0]).toInt());
  if (!connection->stat(&plhs[0]))
    ERROR("Failed to query stat: %s", connection->error_message());
}

MEX_FUNCTION(keys) (int nlhs,
                    mxArray *plhs[],
                    int nrhs,
                    const mxArray *prhs[]) {
  CheckInputArguments(0, 1, nrhs);
  CheckOutputArguments(0, 1, nlhs);
  Database* connection = NULL;
  if (nrhs == 0)
    connection = Sessions::get(Sessions::default_id());
  else
    connection = Sessions::get(MxArray(prhs[0]).toInt());
  if (!connection->keys(&plhs[0]))
    ERROR("Failed to query keys: %s", connection->error_message());
}

MEX_FUNCTION(values) (int nlhs,
                      mxArray *plhs[],
                      int nrhs,
                      const mxArray *prhs[]) {
  CheckInputArguments(0, 1, nrhs);
  CheckOutputArguments(0, 1, nlhs);
  Database* connection = NULL;
  if (nrhs == 0)
    connection = Sessions::get(Sessions::default_id());
  else
    connection = Sessions::get(MxArray(prhs[0]).toInt());
  if (!connection->values(&plhs[0]))
    ERROR("Failed to query values: %s", connection->error_message());
}