"""
This script check if a model run is hanging, and works with the
slurm workload manager.

If a model run is hanging, the script will automatically cancel it and
resubmit the model run.
"""

import os, sys, time



def job_info(jobid):
    # Retrieve casename and whether the job is currently running
    jobstat = os.popen(f"job-statistics -j {jobid}").read().split("\n")
    case = jobstat[2][24:].replace("run.","")
    starttime = jobstat[4][24:]
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
        jobid = int(jobs[i][11:18])
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



def resubmit(casedir,casename,jobid):
    os.system(f"scancel {jobid}")
    os.system(f"./{casedir}/{casename}/case.submit")



def main():
    casename = "b.e21.B1850CAM5.f09_g17.26ka-spinup.001"
    rundir = "/scratch-shared/raymond"
    casedir = "/projects/0/couplice/cases/cases_LGM_B-I"

    while True:

        # Check if the job is running
        job_is_running = False
        while not(job_is_running):
            job_is_running, jobid = is_job_running(casename)
            print(f"{casename} is running: {job_is_running} ({jobid})")
            time.sleep(60*10) # Wait 10 min

        # Get name of coupler file
        cplfile = os.popen("ls {rundir}/{casename}/run/cpl.log.{jobid}.*").read().split("\n")[0]
        print(f"Reading cpl file: {cplfile}")

        # Check if last line of cpl is the same after 10 min
        equal_line = False
        cpl_last_line, success = get_last_line(cplfile)
        print(f"{cpl_last_line}")
        while not(equal_line):
            time.sleep(60*10) # Wait 10 min
            new_last_line, success = get_last_line(cplfile)
            print(f"{new_last_line}")
            if new_last_line[:-4] == "(component_init_cc:mct) : Initialize component":
                None
            elif new_last_line == cpl_last_line:
                equal_line = True
            else:
                cpl_last_line = new_last_line

        if not(success):
            print("Run is hanging, cancel and resubmit....")
            resubmit(casedir,jobid)
        else:
            print("Job finished successfully!")

        print("\n\n\n")

main()
