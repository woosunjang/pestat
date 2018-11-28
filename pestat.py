#!/home/woosunjang/Programs/Python3/bin/python

from collections import defaultdict
import subprocess
import argparse
import sys
import os
import re
from colored import stylize, fg


class pestat(object):
    def __init__(self):
        self.nodes = defaultdict()
        self.job_r = defaultdict()
        self.job_w = defaultdict()
        return

    class pestat_yfhix(object):
        def __init__(self):
            tag_r = '''qstat -u "${*:-*}" -g t -s r -r'''
            tag_w = '''qstat -u "${*:-*}" -g d -s p -r'''
            self.nodes = self.node_status()
            self.job_r = self.qstat_check(tag_r)
            self.job_w = self.qstat_check(tag_w)
            return

        @staticmethod
        def node_status():
            command = subprocess.Popen(["qhost", "-q"], stdout=subprocess.PIPE, universal_newlines=True)
            nodes = defaultdict()
            tmplist = []
            line = command.communicate()[0]
            numcheck = re.compile('\d')
            for x in line.split(os.linesep):
                if "global" in x:
                    pass
                elif "-1-" in x:
                    pass
                elif "HOSTNAME" in x:
                    pass
                elif "---------" in x:
                    pass
                else:
                    tmplist.append(x)
            for i in range(len(tmplist) - 1):
                if ".q" in tmplist[i]:
                    pass
                else:
                    nodes[int(tmplist[i].split()[0].split("-")[-1])] = {"host": tmplist[i].split()[0],
                                                                        "arch": tmplist[i].split()[1],
                                                                        "ncpu": tmplist[i].split()[2],
                                                                        "load": tmplist[i].split()[6],
                                                                        "tmem": tmplist[i].split()[7].strip("G"),
                                                                        "umem": tmplist[i].split()[8].strip("G"),
                                                                        "queue": tmplist[i + 1].split()[0],
                                                                        "resv": tmplist[i + 1].split()[2].split("/")[0],
                                                                        "used": tmplist[i + 1].split()[2].split("/")[1],
                                                                        "totl": tmplist[i + 1].split()[2].split("/")[2],
                                                                        }
                    if numcheck.match(tmplist[i + 1].split()[-1].split("/")[-1]) is None:
                        nodes[int(tmplist[i].split()[0].split("-")[-1])]["note"] = tmplist[i + 1].split()[-1].split("/")[-1]
                    else:
                        nodes[int(tmplist[i].split()[0].split("-")[-1])]["note"] = ""

            return nodes

        @staticmethod
        def qstat_check(tags):
            qstatdic = defaultdict()
            command = subprocess.Popen(tags, shell=True, stdout=subprocess.PIPE, universal_newlines=True)
            line = command.communicate()[0]
            for x in line.split(os.linesep):
                if "MASTER" in x:
                    qstatdic[x.split()[7].split("@")[1].split(".")[0]] = {"jobid": x.split()[0],
                                                                          "prior": x.split()[1],
                                                                          "name": x.split()[2],
                                                                          "user": x.split()[3],
                                                                          "sdate": x.split()[5],
                                                                          "stime": x.split()[6],
                                                                          "queue": x.split()[7].split("@")[0],
                                                                          }
            return qstatdic


def print_info(nodeinfo, runningjob, waitingjob, free):
    cpu_thrshold_low = 0.5
    cpu_thrshold_high = 2.0
    mem_thrshold_low = 0.2
    mem_thrshold_high = 0.1
    freenode = []

    print("-------------------------------------------------------------------------------------")
    print(" Host     Queue   State  Used  Total    Load    Memory    Free    JobID         User ")
    print("-------------------------------------------------------------------------------------")

    to_print = []
    for x in sorted(nodeinfo.items()):
        to_print.append(x)
    for x in to_print:
        jobidlist = []
        userlist = []

        if runningjob.get(x[1]["host"]) is not None:
            jobidlist.append(runningjob[x[1]["host"]]["jobid"])
            userlist.append(runningjob[x[1]["host"]]["user"])
        else:
            jobidlist.append("")
            userlist.append("")

        if x[1]["umem"] == "-":
            fmem = x[1]["tmem"]
        else:
            fmem = str(round(float(x[1]["tmem"]) - float(x[1]["umem"]), 2))

        if mem_thrshold_high * float(x[1]["tmem"]) < float(fmem) <= mem_thrshold_low * float(x[1]["tmem"]):
            mem_color = 11
        elif float(fmem) <= mem_thrshold_high * float(x[1]["tmem"]):
            mem_color = 1
        else:
            mem_color = 249

        if x[1]["load"] == "-":
            cpu_color = 1
        elif float(x[1]["totl"]) + cpu_thrshold_high > float(x[1]["load"]) > float(x[1]["totl"]) + cpu_thrshold_low:
            cpu_color = 11
        elif float(x[1]["load"]) >= float(x[1]["totl"]) + cpu_thrshold_high:
            cpu_color = 1
        else:
            cpu_color = 249

        empty = int(x[1]["totl"]) - int(x[1]["used"])
        if x[1]["note"] != "":
            if "E" in x[1]["note"]:
                state = "DOWN"
                state_color = 1
                host_color = 1
                queue_color = 1
                used_color = 1
                totl_color = 1
                tmem_color = 1
                mem_color = 1
                cpu_color = 1
            elif "u" in x[1]["note"]:
                state = "UNKNOWN"
                state_color = 93
                host_color = 93
                queue_color = 93
                used_color = 93
                totl_color = 93
                tmem_color = 93
                mem_color = 93
                cpu_color = 93
            elif "a" in x[1]["note"]:
                state = "ALARM"
                state_color = 93
                host_color = 93
                queue_color = 93
                used_color = 93
                totl_color = 93
                tmem_color = 93
                mem_color = 93
            else:
                state = x[1]["note"]
                state_color = 93
                host_color = 93
                queue_color = 93
                used_color = 93
                totl_color = 93
                tmem_color = 93
                mem_color = 93
                cpu_color = 93
        elif empty == 0:
            state = "Full"
            state_color = 249
            host_color = 249
            queue_color = 249
            used_color = 249
            totl_color = 249
            tmem_color = 249
        elif empty == int(x[1]["totl"]):
            state = "Idle"
            state_color = 2
            host_color = 2
            queue_color = 2
            used_color = 2
            totl_color = 2
            tmem_color = 2
            mem_color = 2
            cpu_color = 2
            if free is True:
                freenode.append(x[1])
            else:
                pass
        else:
            state = "Occup"
            state_color = 249
            host_color = 249
            queue_color = 249
            used_color = 249
            totl_color = 249
            tmem_color = 249

        print("%5s   %7s   %7s   %2s   %2s    %5s    %5s    %5s    %5s  %12s"
              % (stylize('{:>5}'.format(x[1]["host"].strip("compute-")), fg(host_color)),
                 stylize('{:>7}'.format(x[1]["queue"].strip(".q")), fg(queue_color)),
                 stylize('{:>7}'.format(state), fg(state_color)),
                 stylize('{:>2}'.format(x[1]["used"]), fg(used_color)),
                 stylize('{:>2}'.format(x[1]["totl"]), fg(totl_color)),
                 stylize('{:>5}'.format(x[1]["load"]), fg(cpu_color)),
                 stylize('{:>5}'.format(x[1]["tmem"]), fg(tmem_color)),
                 stylize('{:>5}'.format(fmem), fg(mem_color)),
                 jobidlist[0], userlist[0]))

        if len(jobidlist) >= 2:
            for i in range(len(jobidlist)):
                if i == 0:
                    pass
                else:
                    print("%5s   %7s   %7s    %2s    %2s    %5s    %5s    %5s    %5s  %12s"
                          % ("", "", "", "", "", "", "", "", jobidlist[i], userlist[i]))

    if free is True:
        print("")
        print("-----------------------------------  Free Nodes  ------------------------------------")
        print("           Hostname             Queue           Total CPU         Memory (GB)        ")
        print("-------------------------------------------------------------------------------------")
        for x in freenode:
            print("      %15s          %7s           %5s               %5s"
                  % (stylize('{:>15}'.format(x["host"]), fg(2)),
                     stylize('{:>7}'.format(x["queue"].strip(".q")), fg(2)),
                     stylize('{:>5}'.format(x["totl"]), fg(2)),
                     stylize('{:>5}'.format(x["tmem"]), fg(2))
                     ))


    # if len(waitingjob.keys()) != 0:
    #     pendinglist = []
    #     for x in sorted(waitingjob.items):
    #         pendinglist.append(x)
    #     print("----------------------------------  PENDING JOBS  -----------------------------------")
    #     print("   Queue       JobID           Job          User                 Time                ")
    #     print("-------------------------------------------------------------------------------------")
    #     for x in pendinglist:
    #         print("%5s   %5s   %15s   %12s   %10s %8s"
    #               % (x[1]["queue"].strip(".q"),
    #                  x[1]["jobid"].strip(".q"),
    #                  x[1]["name"].strip(".q"),
    #                  x[1]["user"].strip(".q"),
    #                  x[1]["sdate"].strip(".q"),
    #                  x[1]["stime"].strip(".q")
    #                  ))


def run_pestat(args):
    if args.machine == "yfhix":
        p = pestat.pestat_yfhix()
        print_info(p.nodes, p.job_r, p.job_w, args.free)
    return


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-m", dest="machine", type=str, required=True)
    parser.add_argument("-f", dest="free", action="store_true")
    parser.set_defaults(func=run_pestat)
    args = parser.parse_args()

    try:
        getattr(args, "func")
    except AttributeError:
        parser.print_help()
        sys.exit(0)
    args.func(args)


if __name__ == "__main__":
    main()
