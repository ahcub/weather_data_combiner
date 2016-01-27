import logging
import os
import sys
from distutils.dir_util import remove_tree
from os.path import join, isdir, isfile, islink, exists
from stat import S_IWUSR, S_IWGRP, S_IWOTH, ST_MODE

__all__ = [
    'configure_logging', 'clear_dir', 'mkpath', 'delete'
]

LOGGING_FORMAT = '%(asctime)-15s %(levelname)s %(message)s'
DATE_FORMAT = '[%Y-%m-%d %H:%M:%S]'

WRITE = S_IWUSR | S_IWGRP | S_IWOTH


def configure_logging(filename='app.log', level=logging.DEBUG):
    logging.basicConfig(datefmt=DATE_FORMAT, format=LOGGING_FORMAT, level=level, stream=sys.stdout)
    file_handler = logging.FileHandler(filename=join(os.getcwd(), filename))
    file_handler.level = level
    formatter = logging.Formatter(datefmt=DATE_FORMAT, fmt=LOGGING_FORMAT)
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)


def clear_dir(path):
    delete(path)
    mkpath(path)


def delete(path):
    if exists(path):
        if isdir(path):
            add_permissions_to_dir_rec(path, WRITE)
            remove_tree(path)
        elif isfile(path):
            add_permissions_to_path(path, WRITE)
            os.remove(path)
        elif islink(path):
            add_permissions_to_path(path, WRITE)
            os.unlink(path)


def add_permissions_to_path(path, permissions):
    return os.chmod(path, os.stat(path)[ST_MODE] | permissions)


def add_permissions_to_multiple_paths(root, paths, permissions):
    for path in paths:
        add_permissions_to_path(join(root, path), permissions)


def add_permissions_to_dir_rec(path, permissions):
    for root, dirs, files in os.walk(path):
        add_permissions_to_multiple_paths(root, dirs + files, permissions)


def mkpath(path):
    if not isdir(path):
        os.makedirs(path)
