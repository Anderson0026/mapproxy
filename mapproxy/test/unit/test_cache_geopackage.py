# This file is part of the MapProxy project.
# Copyright (C) 2011-2013 Omniscale <http://omniscale.de>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import with_statement

import os
import shutil
import threading
import tempfile
import time
import sqlite3

from cStringIO import StringIO

from PIL import Image

from mapproxy.cache.tile import Tile
from mapproxy.cache.geopackage import GeopackageCache
from mapproxy.cache.base import CacheBackendError
from mapproxy.image import ImageSource
from mapproxy.image.opts import ImageOptions
from mapproxy.test.image import create_tmp_image_buf, is_png
from mapproxy.grid import tile_grid

from nose.tools import eq_, assert_raises

class TestGeopackageCache(object):
    def setup(self):
        tg = tile_grid(num_levels=8)
        self.cache = GeopackageCache("visibleEarth3395.gpkg", tg)

    def teardown(self):
        if hasattr(self, 'cache_dir') and os.path.exists(self.cache_dir):
            shutil.rmtree(self.cache_dir)

    def test_is_cached_miss(self):
        assert not self.cache.is_cached(Tile((-1, 0, 4)))

    def test_is_cached_hit(self):
        assert self.cache.is_cached(Tile((0, 0, 4)))

    def test_is_cached_none(self):
        assert self.cache.is_cached(Tile(None))

    def test_load_tile_none(self):
        assert self.cache.load_tile(Tile(None))

    def test_load_tile_not_cached(self):
        tile = Tile((-1, 0, 4))
        assert not self.cache.load_tile(tile)
        assert tile.source is None
        assert tile.is_missing()

    def test_load_tile_cached(self):
        tile = Tile((0, 0, 4))
        assert self.cache.load_tile(tile) == True
        assert not tile.is_missing()

    def test_load_tiles_cached(self):
        tiles = [Tile((0, 0, 1)), Tile((0, 1, 1))]
        assert self.cache.load_tiles(tiles)
        assert not tiles[0].is_missing()
        assert not tiles[1].is_missing()

    def test_load_tiles_mixed(self):
        tiles = [Tile(None), Tile((0, 0, 1)), Tile((-1, 0, 1))]
        assert self.cache.load_tiles(tiles) == False
        assert not tiles[0].is_missing()
        assert not tiles[1].is_missing()
        assert tiles[2].is_missing()

    def test_load_empty_tileset(self):
        assert self.cache.load_tiles([Tile(None)]) == True
        assert self.cache.load_tiles([Tile(None), Tile(None), Tile(None)]) == True

    def test_load_334_tiles(self):
        assert_raises(CacheBackendError, self.cache.load_tiles, [Tile((0, 0, 1))] * 334)
