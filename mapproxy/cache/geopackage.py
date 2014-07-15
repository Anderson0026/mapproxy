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
import time
import sqlite3
import threading
from cStringIO import StringIO

from mapproxy.image import ImageSource
from mapproxy.cache.base import TileCacheBase, FileBasedLocking, tile_buffer, CacheBackendError
from mapproxy.util.fs import ensure_directory
from mapproxy.util.lock import FileLock
from mapproxy.grid import tile_grid

import logging
log = logging.getLogger(__name__)

class GeopackageCache(TileCacheBase, FileBasedLocking):

    def __init__(self, gpkg_file, tile_grid, lock_dir=None):
        if lock_dir:
            self.lock_dir = lock_dir
        else:
            self.lock_dir = gpkg_file + '.locks'
        self.lock_timeout = 60
        self.cache_dir = gpkg_file # for lock_id generation by FileBasedLocking
        self.gpkg_file = gpkg_file
        self._db_conn_cache = threading.local()
        self.tile_grid = tile_grid

    @property
    def db(self):
        if not getattr(self._db_conn_cache, 'db', None):
            self._db_conn_cache.db = sqlite3.connect(self.gpkg_file)
        return self._db_conn_cache.db

    def cleanup(self):
        """
        Close all open connection and remove them from cache.
        """
        if getattr(self._db_conn_cache, 'db', None):
            self._db_conn_cache.db.close()
        self._db_conn_cache.db = None

    def is_cached(self, tile):
        if tile.coord is None:
            return True
        if tile.source:
            return True

        return self.load_tile(tile)

    def load_tile(self, tile, with_metadata=False):
        if tile.source or tile.coord is None:
            return True

        (x, y, z) = tile.coord
        resolution = self.tile_grid.resolutions[z]
        maxDiff = 0.5 * resolution
        cur = self.db.cursor()
        cur.execute('''SELECT zoom_level, pixel_x_size FROM gpkg_tile_matrix
                    WHERE ABS(pixel_x_size - ?) < ?''', (resolution, maxDiff))
        zooms = cur.fetchall()
        if len(zooms) > 1:
            gpkg_zoom = zooms[0][0]
            diff = abs(resolution - zooms[0][1])
            for i in zooms:
                d = abs(resolution - i[1])
                if d < diff:
                    gpkg_zoom = i[0]
                    diff = d
        else:
            gpkg_zoom = zooms[0][0]

        cur.execute('''SELECT tile_data FROM tiles
                    WHERE tile_column = ? AND
                          tile_row = ? AND
                          zoom_level = ?''', (x, y, gpkg_zoom))

        content = cur.fetchone()
        if content:
            tile.source = ImageSource(StringIO(content[0]))
            return True
        else:
            return False

    def load_tiles(self, tiles, with_metadata=False):
        #associate the right tiles with the cursor
        tile_dict = {}
        coords = []
        for tile in tiles:
            if tile.source or tile.coord is None:
                continue
            x, y, level = tile.coord
            coords.append(x)
            coords.append(y)
            coords.append(level)
            tile_dict[(x, y)] = tile

        if not tile_dict:
            # all tiles loaded or coords are None
            return True

        if len(coords) > 1000:
            # SQLite is limited to 1000 args
            raise CacheBackendError('cannot query SQLite for more than 333 tiles')

        zoom_level = coords[2]  # All coords should be at the same level
        resolution = self.tile_grid.resolutions[zoom_level]
        maxDiff = 0.5 * resolution
        cursor = self.db.cursor()
        cursor.execute('''SELECT zoom_level, pixel_x_size FROM gpkg_tile_matrix
                       WHERE ABS(pixel_x_size - ?) < ?''', (resolution, maxDiff))
        zooms = cursor.fetchall()
        if len(zooms) > 1:
            gpkg_zoom = zooms[0][0]
            diff = abs(resolution - zooms[0][1])
            for i in zooms:
                d = abs(resolution - i[1])
                if d < diff:
                    gpkg_zoom = i[0]
                    diff = d
        else:
            gpkg_zoom = zooms[0][0]

        for i in range(2, len(coords), 3):
            coords[i] = gpkg_zoom

        stmt = "SELECT tile_column, tile_row, tile_data FROM tiles WHERE "
        stmt += ' OR '.join(['(tile_column = ? AND tile_row = ? AND zoom_level = ?)'] * (len(coords)//3))

        cursor.execute(stmt, coords)

        loaded_tiles = 0
        for row in cursor:
            loaded_tiles += 1
            tile = tile_dict[(row[0], row[1])]
            data = row[2]
            tile.size = len(data)
            tile.source = ImageSource(StringIO(data))
        cursor.close()
        return loaded_tiles == len(tile_dict)

    def load_tile_metadata(self, tile):
        tile.timestamp = -1
