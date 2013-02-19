function bdb_test()
%BDB_TEST Test functionality of the bdb library.

  tests = {@test_functional_1, @test_functional_2};
  for i = 1:numel(tests)
    try
      tests{i}();
      fprintf('PASS: %s\n', func2str(tests{i}));
    catch e
      fprintf('FAIL: %s\n', func2str(tests{i}));
      fprintf('%s\n', e.getReport);
    end
  end

end

function test_functional_1()
%TEST_FUNCTIONAL_1

  filename = fullfile(fileparts(mfilename('fullpath')), '_functional_1.bdb');
  
  function cleanup(filename)
  %CLEANUP
    bdb.close();
    if exist(filename, 'file')
      delete(filename);
    end
  end
  
  bdb.open(filename);
  try
    bdb.put('foo', 'bar');
    assert(strcmp(bdb.get('foo'), 'bar'));
    assert(bdb.exists('foo'));
    bdb.delete('foo');
    bdb.compact();
    assert(~bdb.exists('foo'));
    assert(isempty(bdb.get('foo')));
    assert(isstruct(bdb.stat));
    assert(isempty(bdb.keys));
    assert(isempty(bdb.values));
  catch e
    cleanup(filename);
    rethrow(e);
  end
  cleanup(filename);

end

function test_functional_2()
%TEST_FUNCTIONAL_2

  filename = fullfile(fileparts(mfilename('fullpath')), '_functional_1.bdb');
  
  function cleanup(db_id, filename)
  %CLEANUP
    bdb.close(db_id);
    if exist(filename, 'file')
      delete(filename);
    end
  end
  
  db_id = bdb.open(filename);
  try
    bdb.put(db_id, 'foo', 'bar');
    assert(strcmp(bdb.get(db_id, 'foo'), 'bar'));
    assert(bdb.exists(db_id, 'foo'));
    bdb.delete(db_id, 'foo');
    bdb.compact(db_id);
    assert(~bdb.exists(db_id, 'foo'));
    assert(isempty(bdb.get(db_id, 'foo')));
    assert(isstruct(bdb.stat(db_id)));
    assert(isempty(bdb.keys(db_id)));
    assert(isempty(bdb.values(db_id)));
  catch e
    cleanup(db_id, filename);
    rethrow(e);
  end
  cleanup(db_id, filename);

end