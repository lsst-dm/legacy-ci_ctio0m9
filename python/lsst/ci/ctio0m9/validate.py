from __future__ import absolute_import, division, print_function

import sys
import traceback

from lsst.afw.image import ExposureF
from lsst.pipe.base import CmdLineTask, TaskRunner, InputOnlyArgumentParser
from lsst.pex.config import Config

__all__ = ["CalibValidationTask", "ProcessCcdValidationTask"]


class TestTask(CmdLineTask):
    ConfigClass = Config  # Nothing to configure!

    def __init__(self, *args, **kwargs):
        super(TestTask, self).__init__(*args, **kwargs)
        self._failures = 0

    def __del__(self):
        if self._failures > 0:
            self.log.fatal("%d tests failed")
            sys.exit(1)
        CmdLineTask.__del__(self)

    @classmethod
    def _makeArgumentParser(cls):
        parser = InputOnlyArgumentParser(name=cls._DefaultName)
        parser.add_id_argument(name="--id", datasetType="raw",
                               help="data IDs, e.g. --id visit=12345 ccd=1,2^0,3")
        return parser

    def require(self, test, msg):
        if not test:
            self.log.fatal("FAIL: %s", msg)
            self.log.info("Trace of failure:\n" + "".join(traceback.format_stack()[:-1]))
            self._failures += 1
        else:
            self.log.info("OK: %s", msg)

    def _getConfigName(self):
        return None

    def _getMetadataName(self):
        return None


class ValidationTaskRunner(TaskRunner):
    @staticmethod
    def getTargetList(parsedCmd, **kwargs):
        return TaskRunner.getTargetList(parsedCmd, calibType=parsedCmd.calibToTest, **kwargs)


class CalibValidationTask(TestTask):
    RunnerClass = ValidationTaskRunner
    _DefaultName = "calibValidation"

    @classmethod
    def _makeArgumentParser(cls):
        parser = super(CalibValidationTask, cls)._makeArgumentParser()
        parser.add_argument("--calibToTest", choices=["bias", "dark", "flat", "fringe"], required=True,
                            help="Calib type to test")
        return parser

    def run(self, dataRef, calibType):
        filename = dataRef.get(calibType + "_filename")
        self.require("/rerun/" not in filename, "%s has been ingested" % (calibType,))
        calib = dataRef.get(calibType, immediate=True)
        self.require(isinstance(calib, ExposureF), "%s is an ExposureF" % (calibType,))
        self.require(calib.getWidth() > 1 and calib.getHeight() > 1, "%s size is decent" % (calibType,))


class ProcessCcdValidationTask(TestTask):
    _DefaultName = "processCcdValidation"

    def run(self, dataRef):
        for dataset in ("calexp", "calexpBackground", "icSrc", "src", "srcMatch"):
            self.require(dataRef.datasetExists(dataset), "%s exists" % (dataset,))

        catalog = dataRef.get("src")
        self.require(len(catalog) > 10, "src catalog size")

        matches = dataRef.get("srcMatch")
        self.require(len(matches) > 10, "number of matches")

        calexp = dataRef.get("calexp")
        self.require(isinstance(calexp, ExposureF), "calexp is an ExposureF")
        self.require(calexp.getWidth() > 1 and calexp.getHeight() > 1, "calexp size is decent")
