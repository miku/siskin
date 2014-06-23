# coding: utf-8
# pylint: disable=C0103
import tempfile
from tsk.configuration import TskConfig

config = TskConfig.instance()
tempfile.tempdir = config.get('core', 'tempdir', tempfile.gettempdir())
