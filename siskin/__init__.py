# coding: utf-8
# pylint: disable=C0103
import tempfile
from siskin.configuration import Config

config = Config.instance()
tempfile.tempdir = config.get('core', 'tempdir', tempfile.gettempdir())
