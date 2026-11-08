
import multiprocessing as MP
import os
import sys
import traceback
# CRITICAL: preload psrchive
import psrchive
# ---------------------------------------

# --- SAFE FOR PY2 + PY3 ---
if sys.version_info[0] >= 3:
    try:
        MP.set_start_method("fork")
    except RuntimeError:
        pass

# --- FIX CPU COUNT ---
num_cpus = max(1, MP.cpu_count() - 1)

def threadit(func, arglist, OnOffSwitch={'state':False}, num_threads=40):
    """
    A wrapper for multi-threading any function (func) given an argument list (arglist).
    """

    num_workers = min(num_threads, num_cpus)

    def worker(q, retq, pipe, func, arglist):
        # --- ENSURE PSRCHIVE WORKS IN WORKERS ---
        import os
        os.environ["PYTHONPATH"] = (
            "/usr/local/lib/python2.7/site-packages:"
            "/lustre_archive/apps/tdsoft/usr/src/presto_old/lib/python"
        )
        os.environ["LD_LIBRARY_PATH"] = (
            "/usr/local/lib:" + os.environ.get("LD_LIBRARY_PATH", "")
        )

        while True:
            idx = q.get()
            if idx is None:
                q.task_done()
                break

            try:
                retq.put({idx: func(*(arglist[idx]))})
            except Exception:
                except_type, except_class, tb = sys.exc_info()
                pipe.send((except_type, except_class, traceback.extract_tb(tb)))
                retq.put(None)

            q.task_done()

    if OnOffSwitch['state'] == False or len(arglist) <= 3:
        OnOffSwitch['state'] = True

        q = MP.JoinableQueue()
        to_child, to_self = MP.Pipe()
        retq = MP.Queue()
        procs = []

        for _ in range(num_workers):
            p = MP.Process(target=worker, args=(q, retq, to_self, func, arglist))
            p.start()
            procs.append(p)

        for i in range(len(arglist)):
            q.put(i)

        for _ in range(num_workers):
            q.put(None)

        q.join()

        resultdict = {}
        for _ in range(len(arglist)):
            res = retq.get()
            if res is not None:
                resultdict.update(res)
            else:
                exc_info = to_child.recv()
                print(exc_info)
                raise exc_info[1]

        for p in procs:
            p.join()

        OnOffSwitch['state'] = False
        return resultdict

    else:
        # Fallback: no threading
        return {i: func(*(arglist[i])) for i in range(len(arglist))}