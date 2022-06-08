
"""
安装python
yum install rh-python36 -y
/opt/rh/rh-python36/root/usr/bin/python3 -V
用例
/opt/rh/rh-python36/root/usr/bin/python3 redis-keys.py
"""

import sys, collections, re, time, json
if __name__ == '__main__':
    sys.dont_write_bytecode = True

import random, math
import os, zlib
import redis

#Redis中的Key分组个数限制
REDIS_MAX_GROUPS = 1000
#遍历Redis中的Key QPS限制
REDIS_RATE_LIMIT = 20000

#REDIS_ADDR = "-h 127.0.0.1 -a 123 -p 25061 -n 11 --tls False"
print("REDIS_ADDR:", REDIS_ADDR)

def parse_redis_addr(s):
    m = {}
    l = s.split()
    for k, v in zip(l[::2], l[1::2]):
        m[k] = v
    return m

REDIS_ADDR = parse_redis_addr(REDIS_ADDR)
print("parse_redis_addr(REDIS_ADDR):", REDIS_ADDR)


red = redis.Redis(
    host=REDIS_ADDR["-h"],
    port=int(REDIS_ADDR.get("-p", 6379)),
    password=REDIS_ADDR.get("-a", None),
    db=int(REDIS_ADDR.get("-n", 0)),
    ssl=(REDIS_ADDR.get("--tls", "") == "True"),
    #ssl_cert_reqs='required', # None 'optional' 'required'
    decode_responses=True,
    encoding_errors="backslashreplace")


HAS_MEMORY_USAGE = 1
try:
    red.memory_usage("aaa")
except Exception:
    HAS_MEMORY_USAGE = 0

class Tick:
    __slots__ = ("interval", "ts", "i", "n")
    # 几秒之内最多几次
    def __init__(self, interval, n=1):
        # 确保启动时会触发
        self.interval = interval
        self.ts = time.monotonic() - interval
        self.i = 0
        self.n = n
    def __bool__(self):
        now = time.monotonic()
        if now - self.ts >= self.interval:
            self.ts = now
            self.i = 0
        self.i += 1
        return self.i <= self.n
    def randomized(self):
        self.ts = time.monotonic() - self.interval * random.random()
        return self
    def delay(self, secs):
        self.ts = time.monotonic() - self.interval + secs
        self.i = 1
        return self


def print_all():
    for k in red.scan_iter():
        print(k)


def del_all():
    vec = [k for k in red.scan_iter()]
    red.delete(*vec)



def parse_iinfo(s):
    m = {}
    for k, v in re.findall(r"(\w+):([^\r\n]+)", s):
        m[k] = v
    return m


def show_info(level=0):
    m = red.info()
    rate_limit(1)
    for k, v in m.items():
        if re.search(r"redis_version|uptime_in_days|used_memory_human|used_memory_peak_human|maxmemory_human|nodecount|db\d+", k):
            print(k, v)
    # 阿里的 iinfo 指令查看集群版各个节点的信息
    if level >= 1 and m.get("nodecount", 1) > 1:
        nodecount = int(m["nodecount"])
        for node in range(nodecount):
            s = red.execute_command("iinfo", node)
            rate_limit(1)
            j = parse_iinfo(s)
            print("node:", node, j["used_memory_human"], j["used_memory_peak_human"], j["maxmemory_human"])
            if level >= 2:
                for k, v in j.items():
                    if re.search(r"db\d+", k):
                        print("node:", node, k, v)
    print("slowlog_len:", red.slowlog_len())
    try:
        print(red.slowlog_get(5))
    except Exception:
        print("python redis bug when passing optional decode_responses argument down to parse_slowlog_get()")
    rate_limit(2)


def key2group(k):
    k = re.sub(r'(?<=[:/{_-])(\d+|[0-9a-zA-Z]{52}|[0-9a-zA-Z]{32})(?=[:/}_-]|$)', r'*', k)
    return k



group2count = collections.defaultdict(lambda: 0)
group2size = collections.defaultdict(lambda: 0)

bigKeySample = [[] for i in range(30)]
bigKeyCount = [0 for i in range(30)]

tickReport = Tick(10).delay(2)
gg = {
    "scanCount": 0,
    "queryCount": 0,
    "processedKeyCount": 0,
}
bootTime = time.time()




def rate_limit(queryCountDelta):
    gg["queryCount"] += queryCountDelta
    elapsed = time.time() - bootTime
    if elapsed < 1:
        elapsed = 1
    qps = gg["queryCount"] / elapsed
    if qps > REDIS_RATE_LIMIT:
        time.sleep((qps - REDIS_RATE_LIMIT) * elapsed / REDIS_RATE_LIMIT)


def stat_report():
    print('-' * 76)
    show_info(1)
    dbSize = red.dbsize()
    print("DBSIZE:", dbSize)
    rate_limit(1)
    groups = list(group2size.keys())
    groups.sort()
    for g in groups:
        print(f"| {g:<30} count={group2count[g]:>11,} size={group2size[g]:>11,}")
    print("GROUPS:", len(groups))
    print("gg:", json.dumps(gg, ensure_ascii=False))
    for i in range(len(bigKeySample)):
        n = bigKeyCount[i]
        if n == 0:
            continue
        print(f"size>={2**i:>11,}: count={n:>11,} samples={json.dumps(bigKeySample[i], ensure_ascii=False)}")
    elapsed = round(time.time() - bootTime, 3)
    elapsedRatio = gg["processedKeyCount"] / max(dbSize, 1)
    if elapsedRatio < 1:
       elapsedRatio = 1
    estimatedTotalTimeCost = elapsed / elapsedRatio
    print(f'processedKeyCount: {gg["processedKeyCount"]:,} elapsed: {elapsed:.0f}s {elapsedRatio * 100:.2f}% of {estimatedTotalTimeCost:.0f}s')
    if elapsed < 1:
        elapsed = 1
    qpsForMakeReport = int(gg["queryCount"] / elapsed)
    print(f'queryCount: {gg["queryCount"]:,} qpsForMakeReport: {qpsForMakeReport}')
    if not HAS_MEMORY_USAGE:
        print("MEMORY USAGE need Redis 4.0, so now size is estimated by (keyLength + elemCount * 16) for non-strings.")
    print('-' * 76)


def estimate_kv_size(l, typeVec):
    pp = red.pipeline(transaction=False)
    # 随意指定元素的大小
    elemSizeVec = [] 
    for k, t in zip(l, typeVec):
        if t == "string":
            pp.strlen(k)
            elemSizeVec.append(1)
        elif t == "set":
            pp.scard(k)
            elemSizeVec.append(16)
        elif t == "list":
            pp.llen(k)
            elemSizeVec.append(16)
        elif t == "hash":
            pp.hlen(k)
            elemSizeVec.append(16)
        elif t == "zset":
            pp.zcard(k)
            elemSizeVec.append(16)
        else:
            pp.exists(k)
            elemSizeVec.append(16)
    res = pp.execute()
    rate_limit(len(l) * 3)
    return [len(k) + elemCount * elemSize for k, elemCount, elemSize in zip(l, res, elemSizeVec)]


def stat_some(l):
    if not l:
        return
    pp = red.pipeline(transaction=False)
    for k in l:
        pp.type(k)
        pp.ttl(k)
        if HAS_MEMORY_USAGE:
            pp.memory_usage(k)
    res = pp.execute()

    if HAS_MEMORY_USAGE:
        rate_limit(len(l) * 3)
        typeVec = res[::3]
        ttlVec = res[1::3]
        sizeVec = res[2::3]
    else:
        rate_limit(len(l) * 2)
        typeVec = res[::2]
        ttlVec = res[1::2]
        sizeVec = estimate_kv_size(l, typeVec)

    for k, t, ttl, size in zip(l, typeVec, ttlVec, sizeVec):
        if t is None or ttl == -2 or size is None:
            continue
        g = key2group(k)
        g += " " + t + (ttl == -1 and " NoTTL" or " HasTTL")
        group2count[g] += 1
        group2size[g] += size
        # 长度按 log2 分组
        if size < 1:
            size = 1
        sizeLog2 = int(math.log2(size))
        bigKeyCount[sizeLog2] += 1
        if len(bigKeySample[sizeLog2]) < 3:
            bigKeySample[sizeLog2].append(k)

        if len(group2count) > REDIS_MAX_GROUPS:
            print(list(group2count.keys()))
            stat_report()
            raise RuntimeError("too many groups, need improve key2group")

# 集群版，如果数据库比较空，会有好多空的 scan
def scan_iter(red):
    gg["scanCount"] = 0
    cursor = '0'
    while cursor != 0:
        cursor, data = red.scan(cursor=cursor)
        for item in data:
            gg["processedKeyCount"] += 1
            yield item

        gg["scanCount"] += 1
        rate_limit(1)
        if tickReport:
            stat_report()

def stat_all():
    l = []
    red.select(REDIS_ADDR.get("-d", 0))
    for k in scan_iter(red):
        l.append(k)
        if len(l) >= 1000:
            stat_some(l)
            l = []
    stat_some(l)
    stat_report()
    print("stat_all FINISHED")


if __name__ == '__main__':
    stat_all()


