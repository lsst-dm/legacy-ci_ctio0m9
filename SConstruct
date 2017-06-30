# -*- python -*-

import os
from lsst.sconsUtils.utils import libraryLoaderEnvironment
from lsst.pipe.base import Struct
from lsst.utils import getPackageDir
from SCons.Script import SConscript

SConscript(os.path.join(".", "bin.src", "SConscript"))  # build bin scripts

env = Environment(ENV=os.environ)
env["ENV"]["OMP_NUM_THREADS"] = "1"  # Disable threading; we're parallelising at a higher level

def getExecutable(package, script):
    """
    Given the name of a package and a script or other executable which lies
    within its `bin` subdirectory, return an appropriate string which can be
    used to set up an appropriate environment and execute the command.

    This includes:
    * Specifying an explict list of paths to be searched by the dynamic linker;
    * Specifying a Python executable to be run (we assume the one on the default ${PATH} is appropriate);
    * Specifying the complete path to the script.
    """
    return "{} python {}".format(libraryLoaderEnvironment(),
                                 os.path.join(getPackageDir(package), "bin", script))

Execute(Mkdir(".scons"))

root = Dir('.').srcnode().abspath
AddOption("--raw", default=os.path.join(root, "raw-massaged"), help="Path to raw data")
AddOption("--repo", default=os.path.join(root, "DATA"), help="Path for data repository")
AddOption("--calib", default=os.path.join(root, "CALIB"), help="Path for calib repository")
AddOption("--rerun", default="ci_ctio0m9", help="Rerun name")
AddOption("--no-versions", dest="no_versions", default=False, action="store_true",
          help="Add --no-versions for LSST scripts")

RAW = GetOption("raw")
REPO = GetOption("repo")
CALIB = GetOption("calib")
RERUN = GetOption("rerun")
PROC = REPO + " --rerun " + RERUN + " --calib " + CALIB  # Common processing arguments
DATADIR = os.path.join(GetOption("repo"), "rerun", GetOption("rerun"))
STDARGS = "--doraise" + (" --no-versions" if GetOption("no_versions") else "")

def command(target, source, cmd):
    """Run a command and record that we ran it

    The record is in the form of a file in the ".scons" directory.
    """
    name = os.path.join(".scons", target)
    if isinstance(cmd, str):
        cmd = [cmd]
    out = env.Command(name, source, cmd + [Touch(name)])
    env.Alias(target, name)
    return out


# Set up the data repository
mapper = env.Command(os.path.join(REPO, "_mapper"), ["bin"],
                     ["mkdir -p " + REPO,
                      "echo lsst.obs.ctio0m9.Ctio0m9Mapper > " + os.path.join(REPO, "_mapper"),
                      ])
ingest = env.Command(os.path.join(REPO, "registry.sqlite3"), mapper,
                     [getExecutable("pipe_tasks", "ingestImages.py") + " " + REPO + " " + RAW +
                      "/*.fits --mode=link " + STDARGS]
                      )
makeCalibDir = env.Command(CALIB, [], "mkdir -p " + CALIB)

refcatName = "gaia_DR1_v1"
refcatPath = os.path.join(REPO, "ref_cats", refcatName)
refcat = env.Command(refcatPath, mapper,
                     ["rm -f " + refcatPath,  # Delete any existing, perhaps leftover from previous
                      "ln -s %s %s" % (os.path.join(root, refcatName), refcatPath)])

def constructCalib(target, deps, calib, dataIdList):
    assert calib in ("bias", "dark", "flat", "fringe")
    idList = sum([["--id"] + ["%s=%s" % kv for kv in dataId.items()] for dataId in dataIdList], [])
    construct = command(target + "-construction", deps,
                        " ".join([getExecutable("pipe_drivers", "construct" + calib.capitalize() + ".py"),
                                  PROC, STDARGS, "--batch-type=none"] + idList))
    calibGlob = (os.path.join(DATADIR, calib, "*", "*", "*.fits.gz") if calib in ("flat", "fringe") else
                 os.path.join(DATADIR, calib, "*", "*.fits.gz"))  # flat and fringe need filter
    ingest = command(target + "-ingest", construct,
                     " ".join([getExecutable("pipe_tasks", "ingestCalibs.py"), REPO, "--calib", CALIB,
                               STDARGS, "--validity=365", "--mode=move", calibGlob]))
    validate = command(target + "-validate", ingest,
                       " ".join([getExecutable("ci_ctio0m9", "validateCalib.py"), REPO, "--calib", CALIB,
                                 STDARGS, "--calibToTest", calib] + idList))
    return Struct(construct=construct, ingest=ingest, validate=validate)

def processCcd(target, deps, dataId):
    process = command(target + "-process", deps,
                      " ".join([getExecutable("pipe_tasks", "processCcd.py"), PROC, STDARGS,
                                "--id"] + ["%s=%s" % kv for kv in dataId.items()]))
    validate = command(target + "-validate", process,
                       " ".join([getExecutable("ci_ctio0m9", "validateProcessCcd.py"), PROC, STDARGS,
                                "--id"] + ["%s=%s" % kv for kv in dataId.items()]))
    return validate


bias = constructCalib("bias", [ingest, makeCalibDir], "bias", [dict(object="Bias")])
dark = constructCalib("dark", bias.validate, "dark", [dict(object="Dark")])
gFlat = constructCalib("flat-g", dark.validate, "flat", [dict(object="g_flat")])
zFlat = constructCalib("flat-z", dark.validate, "flat", [dict(object="z_flat")])

gObj = processCcd("object-g", [gFlat.validate, refcat], dict(visit=242907779))
zObj = processCcd("object-z", [zFlat.validate, refcat], dict(visit=242715782))

# Ensure ingest only runs one at a time!
env.SideEffect(os.path.join(".scons", "dummy-no-parallel"),
               [getattr(target, "ingest") for target in (bias, dark, gFlat, zFlat)])

everything = [gObj, zObj]

# Add a no-op install target to keep Jenkins happy.
env.Alias("install", "SConstruct")

env.Alias("all", everything)
Default(everything)

env.Clean(everything, [".scons", "DATA", "CALIB"])
