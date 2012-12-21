function id = open(filename)
%BDB.OPEN Open a Berkeley DB database.
%
%    id = bdb.open(filename)
%
% The function opens the database session for the given db file.
%
% See also bdb.close bdb.put bdb.get bdb.delete bdb.stat bdb.keys
% bdb.values
  id = driver('open', filename);
end