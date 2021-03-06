#!/usr/bin/python3 -u
import os, sys, argparse, stat, subprocess, json, random, traceback, re, time, signal

import argparse

parser = argparse.ArgumentParser(description='tool to synchronize ceph and ZFS volumes', usage='python3 main.py -s backup-test -d backup_pool_1/backup_test_destination')

parser.add_argument('-v', '--verbose', action="store_true", dest='verbose', default=False, help='print verbose output')
parser.add_argument('-vv', '--debug', action="store_true", dest='debug', default=False, help='print debug output')
parser.add_argument('-s', '--source', action="store", dest='source', help='the ceph device to backup', type=str, required=True)
parser.add_argument('-d', '--destination', action="store", dest='destination', help='the zsf device to write into (without /dev/zvol)', type=str, required=True)
parser.add_argument('-p', '--pool', action="store", dest='pool', help='the ceph storage pool', type=str, required=False, default='rbd')
parser.add_argument('-fsync', '--flush-sync', action="store_true", dest='fsync', help='transfers ("flushes") all modified data to the disk device', required=False, default=False)
parser.add_argument('-w', '--whole-object', action="store_true", dest='wholeObject', help='do not diff for intra-object deltas. Dramatically improves diff performance but may result in larger delta backup', required=False, default=True)
parser.add_argument('-healty', '--wait-until-healthy', action="store_true", dest='waitHealthy', help='wait until cluster is healthy', required=False, default=True)
parser.add_argument('-no-scrub', '--no-scrubbing', action="store_true", dest='noScrubbing', help='wait for scrubbing to finnish and disable scrubbing (does re-enable scrubbing automatically). This implies --wait-until-healthy', required=False, default=False)

args = parser.parse_args()

ZFS_DEV_PATH = '/dev/zvol/'

LOGLEVEL_DEBUG = 0
LOGLEVEL_INFO = 1
LOGLEVEL_WARN = 2

BACKUPMODE_INITIAL = 1
BACKUPMODE_INCREMENTAL = 2

INTERNAL_SNAPSHOT_PREFIX = 'backup_snapshot_'

COPY_BLOCKSIZE = (2**20) * 4 # 4MB

destinationPath = None
sourcePath = None

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def logMessage(message, level):
    if level <= LOGLEVEL_INFO and not (args.verbose or args.debug): return
    if level == LOGLEVEL_DEBUG and not args.debug: return
    else:
        print(message)

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Yi', suffix)

def checkZfsVolumeExistence(path):
    logMessage('checking existence of ZFS volume ' + path, LOGLEVEL_INFO)
    if (os.path.exists(path) and stat.S_ISBLK(os.stat(path).st_mode)):
        logMessage('ZFS volume found ' + path, LOGLEVEL_INFO)
        return True
    else:
        logMessage('ZFS volume not found ' + path, LOGLEVEL_INFO)
        return False

def execRaw(command):
    logMessage('exec command "' + command + '"', LOGLEVEL_INFO)
    return str(subprocess.Popen(command, shell=True, stdout=subprocess.PIPE).stdout.read().decode("utf-8")).strip("\n")

def execParseJson(command):
    return json.loads(execRaw(command), encoding='UTF-8')

def getCephVolumeNames():
    return execParseJson('rbd -p ' + args.pool + ' --format json ls')

def cephVolumeExists(volume):
    return volume in getCephVolumeNames()

def getCephSnapshots(volume):
    return execParseJson('rbd -p ' + args.pool + ' snap ls --format json ' + volume)

def countPreviousCephSnapsots(volume):
    logMessage('get ceph snapshot count for volume ' + volume, LOGLEVEL_INFO)
    count = 0
    for snapshot in getCephSnapshots(volume):
        if (snapshot['name'].startswith(INTERNAL_SNAPSHOT_PREFIX, 0, len(INTERNAL_SNAPSHOT_PREFIX))):
            count += 1

    return count

def previousCephSnapsotName(volume):
    logMessage('get ceph snapshot name for volume ' + volume, LOGLEVEL_INFO)
    for snapshot in getCephSnapshots(volume):
        if (snapshot['name'].startswith(INTERNAL_SNAPSHOT_PREFIX, 0, len(INTERNAL_SNAPSHOT_PREFIX))):
            return snapshot['name']
    raise RuntimeError('cannot determine ceph snapshot name, aborting!')


def getBackupMode():
    sourceExists = cephVolumeExists(args.source)

    if (not sourceExists):
        raise RuntimeError('invalid arguments, source volume does not exist ' + args.source)

    targetExsists = checkZfsVolumeExistence(ZFS_DEV_PATH + args.destination)

    previousSnapshotCount = countPreviousCephSnapsots(args.source)

    if (previousSnapshotCount > 1):
        raise RuntimeError('inconsistent state, more than one old snapshot for volume ' + args.source + ' (rbd -p ' + args.pool + ' snap ls ' + args.source + ')')

    if (previousSnapshotCount == 1 and not targetExsists):
        raise RuntimeError('inconsistent state, source snapshot found but target does not exist ' + args.destination + ' (rbd -p ' + args.pool + ' snap ls ' + args.source + ')')

    if (previousSnapshotCount == 0 and targetExsists):
        raise RuntimeError('inconsistent state, source snapshot not found but target does exist' + ' (zfs destroy -R ' + args.destination + ')')

    if (previousSnapshotCount == 0 and not targetExsists):
        return {'mode': BACKUPMODE_INITIAL}
    else:
        return {'mode': BACKUPMODE_INCREMENTAL, 'base_snapshot': previousCephSnapsotName(args.source)}

def createCephSnapshot(volume):
    logMessage('creating ceph snapshot for volume ' + volume, LOGLEVEL_INFO)
    name = INTERNAL_SNAPSHOT_PREFIX + ''.join([random.choice('0123456789abcdef') for _ in range(8)])
    logMessage('exec command "rbd -p ' + args.pool + ' snap create ' + volume + '@' + name + '"', LOGLEVEL_INFO)
    code = subprocess.call(['rbd', '-p', args.pool, 'snap', 'create', volume + '@' + name])
    if (code != 0):
        raise RuntimeError('error creating ceph snapshot code: ' + str(code))
    logMessage('ceph snapshot created ' + name, LOGLEVEL_INFO)
    return name

def removeCephSnapshot(volume, snapshot):
    execRaw('rbd -p ' + args.pool + ' snap rm ' + volume + '@' + snapshot)

def createZfsSnapshot(volume):
    logMessage('creating zfs snapshot for volume ' + volume, LOGLEVEL_INFO)
    name = INTERNAL_SNAPSHOT_PREFIX + ''.join([random.choice('0123456789abcdef') for _ in range(8)])
    logMessage('exec command "zfs snapshot ' + volume + '@' + name + '"', LOGLEVEL_INFO)
    code = subprocess.call(['zfs', 'snapshot', volume + '@' + name])
    if (code != 0):
        raise RuntimeError('error creating zfs snapshot code: ' + str(code))
    logMessage('zfs snapshot created ' + name, LOGLEVEL_INFO)
    return name

def getCephVolumeProperties(volume):
    return execParseJson('rbd -p ' + args.pool + ' --format json info ' + volume)

def createZfsVolume(volume, size):
    logMessage('creating ZFS volume ' + volume, LOGLEVEL_INFO)
    logMessage('exec command "zfs create -V' + str(size) + ' ' + volume + '"', LOGLEVEL_INFO)
    code = subprocess.call(['zfs', 'create', '-V'+str(size), volume])
    if (code != 0):
        raise RuntimeError('error creating ZFS volume code: ' + str(code))

def createZfsDataSet(dataset):
    logMessage('creating ZFS DataSet ' + dataset, LOGLEVEL_INFO)
    logMessage('exec command "zfs create ' + dataset + '"', LOGLEVEL_INFO)
    code = subprocess.call(['zfs', 'create', dataset])
    if (code != 0):
        raise RuntimeError('error creating ZFS DataSet code: ' + str(code))

def mapCephVolume(volume):
    logMessage('mapping ceph volume ' + volume, LOGLEVEL_INFO)
    return execRaw('rbd -p ' + args.pool + ' nbd --read-only map ' + volume)

def unmapCephVolume(dev):
    logMessage('unmapping ceph volume ' + dev, LOGLEVEL_INFO)
    return execRaw('rbd nbd unmap ' + dev)

def getCephSnapshotDelta(volume, snapshot1, snapshot2):
    if args.wholeObject:
        return execParseJson('rbd -p ' + args.pool + ' --format json diff --whole-object ' + volume + ' --from-snap ' + snapshot1 + ' --snap ' + snapshot2)
    return execParseJson('rbd -p ' + args.pool + ' --format json diff ' + volume + ' --from-snap ' + snapshot1 + ' --snap ' + snapshot2)

def compareDeviceSize(dev1, dev2):
    logMessage('compare block device size of ' + dev1 + ' and ' + dev2, LOGLEVEL_INFO)
    sizeDev1 = execRaw('blockdev --getsize64 ' + dev1)
    sizeDev2 = execRaw('blockdev --getsize64 ' + dev2)
    logMessage('source = ' + sizeDev1 + ' (' + sizeof_fmt(int(sizeDev1)) + ') and destination = ' + sizeDev2 + ' (' + sizeof_fmt(int(sizeDev2)) + ')', LOGLEVEL_DEBUG)
    if (sizeDev1 != sizeDev2):
        raise RuntimeError('size mismatch between source and destination ' + sizeDev1 + ' vs ' + sizeDev2)
    return int(sizeDev1)

def setCephScrubbingEnable():
    logMessage('enable ceph scrubbing', LOGLEVEL_INFO)
    execRaw('ceph osd unset nodeep-scrub')
    execRaw('ceph osd unset noscrub')

def setCephScrubbingDisable():
    logMessage('disable ceph scrubbing', LOGLEVEL_INFO)
    execRaw('ceph osd set nodeep-scrub')
    execRaw('ceph osd set noscrub')

def waitForCephHealthy():
    while (execRaw('ceph health detail').startswith('HEALTH_ERR')):
        logMessage('waiting for ceph cluster to become healthy', LOGLEVEL_INFO)
        time.sleep(1)

def waitForCephScrubbingFinnish():
    pattern = re.compile("scrubbing")
    while (pattern.search(execRaw('ceph status'))):
        logMessage('waiting for ceph cluster to complete scrubbing', LOGLEVEL_INFO)
        time.sleep(1)

def cleanup(arg1 = None, arg2 = None):
    logMessage('cleaning up...', LOGLEVEL_INFO)
    if (sourcePath != None):
        unmapCephVolume(sourcePath)
    else:
        logMessage('no ZFS device mapped', LOGLEVEL_INFO)
    if (args.noScrubbing):
        setCephScrubbingEnable()

try:
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    mode = getBackupMode()
    destinationPath = ZFS_DEV_PATH + args.destination

    if (args.waitHealthy or args.noScrubbing):
        waitForCephHealthy()
    if (args.noScrubbing):
        setCephScrubbingDisable()
        waitForCephScrubbingFinnish()

    if (mode['mode'] == BACKUPMODE_INITIAL):
        snapshot = createCephSnapshot(args.source)
        createZfsVolume(args.destination, getCephVolumeProperties(args.source)['size'])
        sourcePath = mapCephVolume(args.source + '@' + snapshot)
        size = compareDeviceSize(sourcePath, destinationPath)

        logMessage('beginning full copy from ' + sourcePath + ' to ' + destinationPath, LOGLEVEL_INFO)

        read = 0
        buffersTransfered = 0
        with open(sourcePath, 'rb') as sfh, open(destinationPath, 'wb') as dfh:
            if (args.debug):
                logMessage('start copy of ' + str(size) + ' bytes (' + sizeof_fmt(size) + ') with buffer size ' + str(COPY_BLOCKSIZE) + ' (' + sizeof_fmt(COPY_BLOCKSIZE) + ')', LOGLEVEL_DEBUG)
            else:
                logMessage('start copy of ' + sizeof_fmt(size) + ' with buffer size ' + sizeof_fmt(COPY_BLOCKSIZE), LOGLEVEL_INFO)
            while (True):
                d = sfh.read(COPY_BLOCKSIZE)
                if not d:
                    break # reached EOF
                read += len(d)
                dfh.write(d)
                if args.fsync:
                    logMessage('flush and fsync fd ' + str(dfh.fileno()), LOGLEVEL_DEBUG)
                    dfh.flush()
                    os.fsync(dfh.fileno())
                buffersTransfered += 1
                if (args.debug):
                    logMessage('transfered ' + str(buffersTransfered) + ' buffers. ' + str(read) + ' bytes (' + sizeof_fmt(read) + ') of ' + str(size) + ' bytes (' + sizeof_fmt(size) + ')', LOGLEVEL_DEBUG)
                else:
                    logMessage('transfered ' + str(buffersTransfered) + ' buffers. ' + sizeof_fmt(read) + ' of ' + sizeof_fmt(size), LOGLEVEL_INFO)

        logMessage('copy finished', LOGLEVEL_INFO)
        if (args.debug):
            logMessage('transfered ' + str(buffersTransfered) + ' buffers. ' + str(read) + ' bytes (' + sizeof_fmt(read) + ') of ' + str(size) + ' bytes (' + sizeof_fmt(size) + ')', LOGLEVEL_DEBUG)
        else:
            logMessage('transfered ' + str(buffersTransfered) + ' buffers. ' + sizeof_fmt(read) + ' of ' + sizeof_fmt(size), LOGLEVEL_INFO)
        createZfsSnapshot(args.destination)

    if (mode['mode'] == BACKUPMODE_INCREMENTAL):
        snapshot1 = mode['base_snapshot']
        snapshot2 = createCephSnapshot(args.source)
        sourcePath = mapCephVolume(args.source + '@' + snapshot2)
        compareDeviceSize(sourcePath, destinationPath)

        delta = getCephSnapshotDelta(args.source, snapshot1, snapshot2)

        if (len(delta) == 0):
            logMessage('no change', LOGLEVEL_INFO)

        totalRead = 0
        size = 0
        blocksTransfered = 0
        blocksTotal = len(delta)
        for block in delta:
            size += block['length']

        with open(sourcePath, 'rb') as sfh, open(destinationPath, 'wb') as dfh:
            if (args.debug):
                logMessage('start copy of ' + str(len(delta)) + ' ceph objects resulting in ' + str(size) + ' bytes (' + sizeof_fmt(size) + ')', LOGLEVEL_DEBUG)
            else:
                logMessage('start copy of ' + str(len(delta)) + ' ceph objects resulting in ' + sizeof_fmt(size), LOGLEVEL_INFO)
            for block in delta:
                length = block['length']
                read = 0

                # seek input and output stream to offset position
                logMessage('seeking ceph block device to offset ' + str(block['offset']) + ' bytes (' + sizeof_fmt(block['offset']) + ')', LOGLEVEL_DEBUG)
                sfh.seek(block['offset'], 0)
                logMessage('seeking zfs block device to offset ' + str(block['offset']) + ' bytes (' + sizeof_fmt(block['offset']) + ')', LOGLEVEL_DEBUG)
                dfh.seek(block['offset'], 0)

                while (read < length):
                    s = min(length - read, COPY_BLOCKSIZE)
                    d = sfh.read(s)
                    read += len(d)
                    if (args.debug):
                        logMessage('transfered ' + str(read) + ' bytes (' + sizeof_fmt(read) + ') of block size ' + str(length) + ' bytes (' + sizeof_fmt(length) + '). Copy buffer size = ' + str(s) + ' bytes (' + sizeof_fmt(s) + ') ', LOGLEVEL_DEBUG)
                    dfh.write(d)
                    if args.fsync:
                        logMessage('flush and fsync fd ' + str(dfh.fileno()), LOGLEVEL_DEBUG)
                        dfh.flush()
                        os.fsync(dfh.fileno())
                totalRead += read
                blocksTransfered += 1
                if (args.debug):
                    logMessage('transfered ' + str(blocksTransfered) + ' blocks out of ' + str(blocksTotal) + ' blocks and ' + str(totalRead) + ' bytes (' + sizeof_fmt(totalRead) + ') of ' + str(size) + ' bytes (' + sizeof_fmt(size) + '). copy delta block with offset = ' + str(block['offset']) + ' bytes (' + sizeof_fmt(block['offset']) + ') and length = ' + str(block['length']) + ' bytes (' + sizeof_fmt(block['length']), LOGLEVEL_DEBUG)
                else:
                    logMessage('transfered ' + str(blocksTransfered) + ' blocks out of ' + str(blocksTotal) + ' blocks and ' + sizeof_fmt(totalRead) + ' of ' + sizeof_fmt(size) + '. copy delta block with offset = ' + sizeof_fmt(block['offset']) + ' and length = ' + sizeof_fmt(block['length']), LOGLEVEL_INFO)

        logMessage('copy finished', LOGLEVEL_INFO)
        if (args.debug):
            logMessage('transfered ' + str(blocksTransfered) + ' blocks out of ' + str(blocksTotal) + ' blocks and ' + str(totalRead) + ' bytes (' + sizeof_fmt(totalRead) + ') of ' + str(size) + ' bytes (' + sizeof_fmt(size) + ')', LOGLEVEL_DEBUG)
        else:
            logMessage('transfered ' + str(blocksTransfered) + ' blocks out of ' + str(blocksTotal) + ' blocks and ' + sizeof_fmt(totalRead) + ' of ' + sizeof_fmt(size), LOGLEVEL_INFO)

        createZfsSnapshot(args.destination)
        removeCephSnapshot(args.source, snapshot1)

    logMessage(bcolors.OKGREEN + 'Done with ' + args.source + ' -> ' + args.destination + bcolors.ENDC, LOGLEVEL_INFO)


except KeyboardInterrupt:
    logMessage(bcolors.WARNING + 'Interrupt, terminating...' + bcolors.ENDC, LOGLEVEL_WARN)

except RuntimeError as e:
    logMessage(bcolors.FAIL + 'runtime exception ' + str(e) + bcolors.ENDC, LOGLEVEL_WARN)

except Exception as e:
    logMessage(bcolors.FAIL + 'unexpected exception (probably a bug): ' + str(e) + bcolors.ENDC, LOGLEVEL_WARN)
    traceback.print_exc()

finally:
    cleanup()
