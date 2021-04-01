import gzip
import os
import shutil
from logging import Logger
from typing import Optional

import numpy as np


def filter_filepaths(filepath, file_pattern, verbose=False, logger: Optional[Logger] = None) -> np.array:
    """ Recursively searches for all files with the pattern file_pattern
    in the directories within the provided filepath.

    :param logger:
    :param filepath:    Name of the filepath to look into
    :type filepath:     String

    :param file_pattern:   Pattern to match file name
    :type file_name:    String

    :return:    Array of directories
    :rtype:     np.array
    """
    # If the filepath is a firectory
    if logger is None:
        verbose = False

    if verbose:
        logger.info(
            f'Searching for {file_pattern} files in {filepath} started!')
    if os.path.isdir(filepath):

        # Check to see if the file_pattern appears in any
        # of the filepaths.
        files = np.array([
            os.path.join(filepath, f)
            for f in os.listdir(filepath)
            if file_pattern in f
        ])
        if len(files) > 0:
            # If so, return the combined path
            return files
        else:
            # Otherwise, iterate over elements and repeat.
            file_paths = np.array([])
            for path in os.listdir(filepath):
                res = filter_filepaths(os.path.join(filepath, path), file_pattern, verbose)
                if res is not None:
                    file_paths = np.append(file_paths, res)

            return np.array(file_paths)
    else:
        # Stop. Reached a non-directory element
        return None


def gunzip(file_path, output_path):
    """
    Unzip a *.gz file

    :param file_path: gz file path
    :param output_path: output file path
    :return:
    """
    with gzip.open(file_path, "rb") as f_in, open(output_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)