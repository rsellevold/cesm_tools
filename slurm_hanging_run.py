#!/bin/python3 -u
#SBATCH -N 1
#SBATCH -p short
#SBATCH -t 10:00

"""
This script check if a model run is hanging, and works with the
slurm workload manager.

If a model run is hanging, the script will automatically cancel it and
resubmit the model run.
"""

import os, sys, time
import multiprocessing as mp


def job_info(jobid):
    # Retrieve casename and whether the job is currently running
    jobstat = os.popen(f"job-statistics -j {jobid}").read().split("\n")
    try:
        case = jobstat[2][24:].replace("run.","")
    except:
        case = "IDK"

    try:
        starttime = jobstat[4][24:]
    except:
        starttime = "Unknown"
        
    if starttime=="Unknown":
        running = False
    else:
        running = True
    return case, running



def is_job_running(casename):
    # This function finds the job corresponding to the case in the squeue
    # and checks if it is running.

    job_is_running = False
    jobid_case = -1
    jobs = os.popen("squeue").read().split("\n")
    for i in range(1,len(jobs)-1):
        jobid = int(jobs[i][9:18])
        case, running = job_info(jobid)
        if case==casename:
            jobid_case = jobid
            if running:
                job_is_running = True

    return job_is_running, jobid_case



def get_last_line(f):
    # Get the last line of the file. Also check if 
    # the model has completed successfully.
    run_success = False
    with open(f, "r") as file:
        first_line = file.readline()
        for line in file:
            if line == "(seq_mct_drv): ===============          SUCCESSFUL TERMINATION OF CPL7-cesm ===============":
                run_success = True

    return line, run_success



def cancel(casedir,casename,jobid):
    os.system(f"scancel {jobid}")



def watch(casename, rundir, casedir):
    while True:

        # Check if the job is running
        job_is_running = False
        while not(job_is_running):
            job_is_running, jobid = is_job_running(casename)
            print(f"{casename} is running: {job_is_running} ({jobid})")
            if not(job_is_running):
                time.sleep(60*10) # Wait 10 min
            else:
                time.sleep(60) # Wait 1 min

        # Get name of coupler file
        cplfile = os.popen(f"ls {rundir}/{casename}/run/cpl.log.{jobid}.*").read().split("\n")[0]
        print(f"Reading cpl file: {cplfile}")

        # Check if last line of cpl is the same after 10 min
        equal_line = False
        cpl_last_line, success = get_last_line(cplfile)
        print(f"{cpl_last_line[:-1]}")
        while not(equal_line):
            time.sleep(60*5) # Wait 5 min
            cplfileexist = os.path.isfile(cplfile)
            if cplfileexist:
                new_last_line, success = get_last_line(cplfile)
                print(f"{new_last_line[:-1]}")
            else: # Cpl file moved after successfull run
                equal_line = True
                success = True

            if new_last_line[:-5] == "(component_init_cc:mct) : Initialize component":
                None
            elif new_last_line == cpl_last_line:
                equal_line = True
            else:
                cpl_last_line = new_last_line

        if not(success):
            print("Run is hanging, cancel!....")
            cancel(casedir,casename,jobid)
        else:
            print("Job finished successfully!")

        print("\n\n\n")

def main():
    cases = []
    cases.append(["b.e21.B1850G.f09_g17_gl4.CMIP6-1pctCO2-2xCO2.002","/scratch-shared/raymond","/home/raymond/cases/CO2"])
    cases.append(["b.e21.B1850G.f09_g17_gl4.CMIP6-1pctCO2-2.5xCO2.001","/scratch-shared/raymond","/home/raymond/cases/CO2"])
    cases.append(["b.e21.B1850G.f09_g17_gl4.CMIP6-1pctCO2-3xCO2.001","/scratch-shared/raymond","/home/raymond/cases/CO2"])
    cases.append(["b.e21.B1850G.f09_g17_gl4.CMIP6-1pctCO2-recovery.001","/scratch-shared/raymond","/home/raymond/cases/CO2"])

    jobs = []
    for i in range(len(cases)):
        p = mp.Process(target=watch, args=(cases[i][0], cases[i][1], cases[i][2],))
        jobs.append(p)
        p.start()

if __name__ == "__main__":
    main()
