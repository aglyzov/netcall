# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

from logging import WARNING
from unittest import TestCase

from shutil   import rmtree
from tempfile import mkdtemp

from netcall.utils import setup_logger

logger = setup_logger(level=WARNING)
#logger = setup_logger()


class BaseCase(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tmp_dir = mkdtemp(prefix='netcall-test-')
        cls.urls    = [
            'ipc://%s/test-%s' % (cls.tmp_dir, i)
            for i in range(3)
        ]
        cls.extra   = ['ipc://%s/extra' % cls.tmp_dir]

        super(BaseCase, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        rmtree(cls.tmp_dir, ignore_errors=True)

        super(BaseCase, cls).tearDownClass()
