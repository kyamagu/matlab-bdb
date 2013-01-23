function result = stat(varargin)
%BDB.STAT Get a statistics of the database.
%
%    result = bdb.stat()
%    result = bdb.stat(id)
%
% The function retrieves statistics of the specified database session. When
% the id is omitted, the default session is used.
%
% The result is a struct array.
%
% See also bdb.open bdb.close
  result = driver_('stat', varargin{:});
end