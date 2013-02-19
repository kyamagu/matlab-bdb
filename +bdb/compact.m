function compact(varargin)
%COMPACT Free unused blocks and shrink the database.
%
%    bdb.compact()
%    bdb.compact(id)
%
% The function apply compact operation to the specified database session. When
% the id is omitted, the default session is used.
  mex_function_(mfilename, varargin{:});
end
