#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Unit tests for HdFileSystem."""

from __future__ import absolute_import

import posixpath
import StringIO
import unittest

from apache_beam.io import hdfilesystem
from apache_beam.io.filesystem import BeamIOError


class FakeFile(StringIO.StringIO):
  """File object for FakeHdfs"""

  def __init__(self, path, mode):
    StringIO.StringIO.__init__(self)
    self.stat = {
        'path': path,
        'mode': mode,
    }
    self.saved_data = None

  def __eq__(self, other):
    return self.stat == other.stat and self.getvalue() == self.getvalue()

  def close(self):
    self.saved_data = self.getvalue()
    StringIO.StringIO.close(self)

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.close()


class FakeHdfsError(Exception):
  """Generic error for FakeHdfs methods."""


class FakeHdfs(object):
  """Fake implementation of hdfs3.HdFileSystem."""

  def __init__(self):
    self.files = {}

  def open(self, path, mode='rb'):
    if mode == 'rb' and not self.exists(path):
      raise FakeHdfsError('path not found: %s' % path)

    if mode in ['rb', 'wb']:
      new_file = FakeFile(path, mode)
      # Required to support read and write operations with CompressedFile.
      new_file.mode = 'rw'

      if mode == 'rb':
        old_file = self.files.get(path, None)
        if old_file is not None:
          if old_file.stat['mode'] == 'dir':
            raise FakeHdfsError('cannot open a directory: %s' % path)
          if old_file.saved_data:
            old_file = self.files[path]
            new_file.write(old_file.saved_data)
            new_file.seek(0)

      self.files[path] = new_file
      return new_file
    else:
      raise FakeHdfsError('unknown mode: %s' % mode)

  def ls(self, path, detail=False):
    result = []
    for file in self.files.itervalues():
      if file.stat['path'].startswith(path):
        result.append({
            'name': file.stat['path'],
            'size': len(file.getvalue()),
        })
    return result

  def makedirs(self, path):
    self.files[path] = FakeFile(path, 'dir')

  def exists(self, path):
    return path in self.files

  def rm(self, path, recursive=True):
    if not recursive:
      raise FakeHdfsError('non-recursive mode not implemented')

    if not self.exists(path):
      raise FakeHdfsError('path not found: %s' % path)

    for filepath in self.files.keys():  # pylint: disable=consider-iterating-dictionary
      if filepath.startswith(path):
        del self.files[filepath]

  def isdir(self, path):
    if not self.exists(path):
      raise FakeHdfsError('path not found: %s' % path)

    return self.files[path].stat['mode'] == 'dir'

  def walk(self, path):
    files = []
    dirs = []
    for fullpath in self.files:
      if fullpath.startswith(path):
        shortpath = posixpath.relpath(fullpath, path)
        if self.isdir(fullpath):
          if shortpath != '.':
            dirs.append(shortpath)
        else:
          files.append(shortpath)

    yield path, dirs, files

  def mv(self, path1, path2):
    if not self.exists(path1):
      raise FakeHdfsError('path1 not found: %s' % path1)

    for fullpath in self.files.keys():  # pylint: disable=consider-iterating-dictionary
      if fullpath == path1 or fullpath.startswith(path1 + '/'):
        f = self.files.pop(fullpath)
        newpath = path2 + fullpath[len(path1):]
        f.stat['path'] = newpath
        self.files[newpath] = f

    return True


class HdFileSystemTest(unittest.TestCase):

  def setUp(self):
    self._fake_hdfs = FakeHdfs()
    hdfilesystem.HDFS3 = lambda *args, **kwargs: self._fake_hdfs
    self.fs = hdfilesystem.HdFileSystem(host=None, port=None)
    self.tmpdir = '/test_dir'

    for filename in ['old_file1', 'old_file2']:
      path = self.fs.join(self.tmpdir, filename)
      self._fake_hdfs.files[path] = FakeFile(path, 'closed')

  def test_scheme(self):
    self.assertEqual(self.fs.scheme(), 'hdfs')
    self.assertEqual(hdfilesystem.HdFileSystem.scheme(), 'hdfs')

  def test_path_join(self):
    self.assertEqual('/tmp/path/to/file',
                     self.fs.join('/tmp/path', 'to', 'file'))
    self.assertEqual('/tmp/path/to/file',
                     self.fs.join('/tmp/path', 'to/file'))

  def test_path_split(self):
    self.assertEqual(('/tmp/path/to', 'file'),
                     self.fs.split('/tmp/path/to/file'))
    self.assertEqual(('/', 'tmp'), self.fs.split('/tmp'))
    self.assertEqual(('', 'tmp'), self.fs.split('tmp'))

  def test_mkdirs(self):
    path = self.fs.join(self.tmpdir, 't1/t2')
    self.fs.mkdirs(path)
    match_results = self.fs.match([path])
    self.assertEqual(1, len(match_results))
    self.assertEqual(1, len(match_results[0].metadata_list))
    metadata = match_results[0].metadata_list[0]
    self.assertEqual(metadata.path, path)
    self.assertTrue(self.fs.exists(path))

  def test_mkdirs_failed(self):
    path = self.fs.join(self.tmpdir, 't1/t2')
    self.fs.mkdirs(path)

    with self.assertRaises(IOError):
      self.fs.mkdirs(path)

  def test_match_file(self):
    files = [self.fs.join(self.tmpdir, filename)
             for filename in ['old_file1', 'old_file2']]
    result = self.fs.match(files)
    returned_files = [f.path
                      for match_result in result
                      for f in match_result.metadata_list]
    self.assertEqual(files, returned_files)

  def test_match_file_with_limits(self):
    expected_files = [self.fs.join(self.tmpdir, filename)
                      for filename in ['old_file1', 'old_file2']]
    result = self.fs.match([self.tmpdir], [1])[0]
    files = [f.path for f in result.metadata_list]
    self.assertEquals(len(files), 1)
    self.assertIn(files[0], expected_files)

  def test_match_file_empty(self):
    path = self.fs.join(self.tmpdir, 'nonexistent_file')
    result = self.fs.match([path])[0]
    files = [f.path for f in result.metadata_list]
    self.assertEqual(files, [])

  def test_match_file_error(self):
    path1 = self.fs.join(self.tmpdir, 'old_file1')
    with self.assertRaisesRegexp(BeamIOError,
                                 r'^Match operation failed .* None'):
      result = self.fs.match([None, path1])[0]
      files = [f.path for f in result.metadata_list]
      self.assertEqual(files, [path1])

  def test_match_directory(self):
    expected_files = [self.fs.join(self.tmpdir, filename)
                      for filename in ['old_file1', 'old_file2']]

    result = self.fs.match([self.tmpdir])[0]
    files = [f.path for f in result.metadata_list]
    self.assertEqual(files, expected_files)

  def test_match_directory_trailing_slash(self):
    expected_files = [self.fs.join(self.tmpdir, filename)
                      for filename in ['old_file1', 'old_file2']]

    result = self.fs.match([self.tmpdir + '/'])[0]
    files = [f.path for f in result.metadata_list]
    self.assertEqual(files, expected_files)

  def test_create_success(self):
    path = self.fs.join(self.tmpdir, 'new_file')
    handle = self.fs.create(path)
    self.assertIsNotNone(handle)
    expected_file = FakeFile(path, 'wb')
    self.assertEqual(self._fake_hdfs.files[path], expected_file)

  def test_create_write_read_compressed(self):
    path = self.fs.join(self.tmpdir, 'new_file.gz')

    handle = self.fs.create(path)
    self.assertIsNotNone(handle)
    expected_file = FakeFile(path, 'wb')
    self.assertEqual(self._fake_hdfs.files[path], expected_file)
    data = 'abc' * 10
    handle.write(data)
    # Compressed data != original data
    self.assertNotEquals(data, self._fake_hdfs.files[path].getvalue())
    handle.close()

    handle = self.fs.open(path)
    read_data = handle.read(len(data))
    self.assertEqual(data, read_data)
    handle.close()

  def test_open(self):
    path = self.fs.join(self.tmpdir, 'old_file1')
    handle = self.fs.open(path)
    self.assertEqual(handle, self._fake_hdfs.files[path])

  def test_open_bad_path(self):
    with self.assertRaises(FakeHdfsError):
      self.fs.open('/nonexistent/path')

  def _cmpfiles(self, path1, path2):
    with self.fs.open(path1) as f1:
      with self.fs.open(path2) as f2:
        data1 = f1.read()
        data2 = f2.read()
        return data1 == data2

  def test_copy_file(self):
    path1 = self.fs.join(self.tmpdir, 'new_file1')
    path2 = self.fs.join(self.tmpdir, 'new_file2')
    path3 = self.fs.join(self.tmpdir, 'new_file3')
    with self.fs.create(path1) as f1:
      f1.write('Hello')
    self.fs.copy([path1, path1], [path2, path3])
    self.assertTrue(self._cmpfiles(path1, path2))
    self.assertTrue(self._cmpfiles(path1, path3))

  def test_copy_file_overwrite(self):
    path1 = self.fs.join(self.tmpdir, 'new_file1')
    path2 = self.fs.join(self.tmpdir, 'new_file2')
    with self.fs.create(path1) as f1:
      f1.write('Hello')
    with self.fs.create(path2) as f2:
      f2.write('nope')
    self.fs.copy([path1], [path2])
    self.assertTrue(self._cmpfiles(path1, path2))

  def test_copy_file_error(self):
    path1 = self.fs.join(self.tmpdir, 'new_file1')
    path2 = self.fs.join(self.tmpdir, 'new_file2')
    path3 = self.fs.join(self.tmpdir, 'new_file3')
    path4 = self.fs.join(self.tmpdir, 'new_file4')
    with self.fs.create(path3) as f:
      f.write('Hello')
    with self.assertRaisesRegexp(
        BeamIOError, r'^Copy operation failed .*%s.*%s.* not found' % (
            path1, path2)):
      self.fs.copy([path1, path3], [path2, path4])
    self.assertTrue(self._cmpfiles(path3, path4))

  def test_copy_directory(self):
    path_t1 = self.fs.join(self.tmpdir, 't1')
    path_t1_inner = self.fs.join(self.tmpdir, 't1/inner')
    path_t2 = self.fs.join(self.tmpdir, 't2')
    path_t2_inner = self.fs.join(self.tmpdir, 't2/inner')
    self.fs.mkdirs(path_t1)
    self.fs.mkdirs(path_t1_inner)
    self.fs.mkdirs(path_t2)

    path1 = self.fs.join(path_t1_inner, 'f1')
    path2 = self.fs.join(path_t2_inner, 'f1')
    with self.fs.create(path1) as f:
      f.write('Hello')

    self.fs.copy([path_t1], [path_t2])
    self.assertTrue(self._cmpfiles(path1, path2))

  def test_copy_directory_overwrite(self):
    path_t1 = self.fs.join(self.tmpdir, 't1')
    path_t1_inner = self.fs.join(self.tmpdir, 't1/inner')
    path_t2 = self.fs.join(self.tmpdir, 't2')
    path_t2_inner = self.fs.join(self.tmpdir, 't2/inner')
    self.fs.mkdirs(path_t1)
    self.fs.mkdirs(path_t1_inner)
    self.fs.mkdirs(path_t2)
    self.fs.mkdirs(path_t2_inner)

    path1 = self.fs.join(path_t1, 'f1')
    path1_inner = self.fs.join(path_t1_inner, 'f2')
    path2 = self.fs.join(path_t2, 'f1')
    path2_inner = self.fs.join(path_t2_inner, 'f2')
    path3_inner = self.fs.join(path_t2_inner, 'f3')
    for path in [path1, path1_inner, path3_inner]:
      with self.fs.create(path) as f:
        f.write('Hello')
    with self.fs.create(path2) as f:
      f.write('nope')

    self.fs.copy([path_t1], [path_t2])
    self.assertTrue(self._cmpfiles(path1, path2))
    self.assertTrue(self._cmpfiles(path1_inner, path2_inner))
    self.assertTrue(self.fs.exists(path3_inner))

  def test_rename_file(self):
    path1 = self.fs.join(self.tmpdir, 'f1')
    path2 = self.fs.join(self.tmpdir, 'f2')
    with self.fs.create(path1) as f:
      f.write('Hello')

    self.fs.rename([path1], [path2])
    self.assertFalse(self.fs.exists(path1))
    self.assertTrue(self.fs.exists(path2))

  def test_rename_file_error(self):
    path1 = self.fs.join(self.tmpdir, 'f1')
    path2 = self.fs.join(self.tmpdir, 'f2')
    path3 = self.fs.join(self.tmpdir, 'f3')
    path4 = self.fs.join(self.tmpdir, 'f4')
    with self.fs.create(path3) as f:
      f.write('Hello')

    with self.assertRaisesRegexp(
        BeamIOError, r'^Rename operation failed .*%s.*%s' % (path1, path2)):
      self.fs.rename([path1, path3], [path2, path4])
    self.assertFalse(self.fs.exists(path3))
    self.assertTrue(self.fs.exists(path4))

  def test_rename_directory(self):
    path_t1 = self.fs.join(self.tmpdir, 't1')
    path_t2 = self.fs.join(self.tmpdir, 't2')
    self.fs.mkdirs(path_t1)
    path1 = self.fs.join(path_t1, 'f1')
    path2 = self.fs.join(path_t2, 'f1')
    with self.fs.create(path1) as f:
      f.write('Hello')

    self.fs.rename([path_t1], [path_t2])
    self.assertFalse(self.fs.exists(path_t1))
    self.assertTrue(self.fs.exists(path_t2))
    self.assertFalse(self.fs.exists(path1))
    self.assertTrue(self.fs.exists(path2))

  def test_exists(self):
    path1 = self.fs.join(self.tmpdir, 'old_file1')
    path2 = self.fs.join(self.tmpdir, 'nonexistent')
    self.assertTrue(self.fs.exists(path1))
    self.assertFalse(self.fs.exists(path2))

  def test_delete_file(self):
    path = self.fs.join(self.tmpdir, 'old_file1')

    self.assertTrue(self.fs.exists(path))
    self.fs.delete([path])
    self.assertFalse(self.fs.exists(path))

  def test_delete_dir(self):
    dir1 = self.fs.join(self.tmpdir, 'new_dir1')
    dir2 = self.fs.join(dir1, 'new_dir2')
    path1 = self.fs.join(dir2, 'new_file1')
    path2 = self.fs.join(dir2, 'new_file2')
    self.fs.mkdirs(dir1)
    self.fs.mkdirs(dir2)
    self.fs.create(path1).close()
    self.fs.create(path2).close()

    self.assertTrue(self.fs.exists(path1))
    self.assertTrue(self.fs.exists(path2))
    self.fs.delete([dir1])
    self.assertFalse(self.fs.exists(dir1))
    self.assertFalse(self.fs.exists(dir2))
    self.assertFalse(self.fs.exists(path2))
    self.assertFalse(self.fs.exists(path1))

  def test_delete_error(self):
    path1 = self.fs.join(self.tmpdir, 'nonexistent')
    path2 = self.fs.join(self.tmpdir, 'old_file1')

    self.assertTrue(self.fs.exists(path2))
    with self.assertRaisesRegexp(BeamIOError,
                                 r'^Delete operation failed .* %s' % path1):
      self.fs.delete([path1, path2])
    self.assertFalse(self.fs.exists(path2))
