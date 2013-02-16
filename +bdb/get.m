function value = get(varargin)
%BDB.GET Retrieve a value given key.
%
%    value = bdb.get(key)
%    value = bdb.get(id, key)
%
% The function retrieves an entry with the given key in the specified
% database session. When the id is omitted, the default session is used.
%
% The key must be an ordinary object.
%
% See also bdb.put bdb.delete
  value = bdbmex_(mfilename, varargin{:});
end
