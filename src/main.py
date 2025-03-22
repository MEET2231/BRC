import math
import mmap
import multiprocessing
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Tuple

def r(value: float) -> float:
    """Round up a value to one decimal place."""
    return math.ceil(value * 10) / 10

def i() -> List[float]:
    """Initialize city statistics: [min, max, total, count]."""
    return [math.inf, -math.inf, 0.0, 0]

def p(f: str, s: int, e: int) -> Dict[bytes, List[float]]:
    """Process a chunk of the file and return city statistics."""
    d = defaultdict(i)
    with open(f, "rb") as fi:
        with mmap.mmap(fi.fileno(), 0, access=mmap.ACCESS_READ) as memory_map:
            if s != 0:
                while s < len(memory_map) and memory_map[s] != ord('\n'):
                    s += 1
                s += 1
            en = e
            while en < len(memory_map) and memory_map[en] != ord('\n'):
                en += 1
            if en < len(memory_map):
                en += 1
            c = memory_map[s:en]
    for l in c.splitlines():
        if not l:
            continue
        ci, _, sc = l.partition(b';')
        if not sc:
            continue
        try:
            sc = float(sc)
        except ValueError:
            continue
        st = d[ci]
        st[0] = min(st[0], sc)
        st[1] = max(st[1], sc)
        st[2] += sc
        st[3] += 1
    return d

def m(dl: List[Dict[bytes, List[float]]]) -> Dict[bytes, List[float]]:
    """Merge city statistics from multiple chunks."""
    fd = defaultdict(i)
    for d in dl:
        for ci, st in d.items():
            fs = fd[ci]
            fs[0] = min(fs[0], st[0])
            fs[1] = max(fs[1], st[1])
            fs[2] += st[2]
            fs[3] += st[3]
    return fd

def main(in_f: str = "testcase.txt", out_f: str = "output.txt") -> None:
    """Main function to process the file and generate output."""
    with open(in_f, "rb") as fi:
        with mmap.mmap(fi.fileno(), 0, access=mmap.ACCESS_READ) as memory_map:
            fs = len(memory_map)
    np = multiprocessing.cpu_count() * 2
    cs = fs // np
    ch = [(in_f, i * cs, (i + 1) * cs if i < np - 1 else fs) for i in range(np)]
    with ProcessPoolExecutor(max_workers=np) as ex:
        fu = [ex.submit(p, *c) for c in ch]
        re = [f.result() for f in as_completed(fu)]
    fd = m(re)
    ol = []
    for ci in sorted(fd.keys(), key=lambda c: c.decode()):
        mi, ma, to, co = fd[ci]
        av = r(to / co)
        ol.append(f"{ci.decode()}={r(mi):.1f}/{av:.1f}/{r(ma):.1f}\n")
    with open(out_f, "w") as fi:
        fi.writelines(ol)

if __name__ == "__main__":
    main()
