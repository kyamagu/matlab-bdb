// Berkeley DB mex interface.
//
// Kota Yamaguchi 2012 <kyamagu@cs.stonybrook.edu>
#include "bdbmex.h"

using bdbmex::Operation;

// Entry point to the mex function.
void mexFunction(int nlhs, mxArray *plhs[], int nrhs, const mxArray *prhs[]){
  auto_ptr<Operation> operation(Operation::parse(nrhs, prhs));
  operation->run(nlhs, plhs);
}