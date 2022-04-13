using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

namespace REDFS_ClusterMode
{
    public class BackupJob
    {
        public string job_start;
        public string job_end;
        public int backupTaskNumber; //Backup task id
        public int backupJobNumber;

        public long dataCopied;
        public long numInodes;

        public BackupJob(int tid, int jid, string st, string et, long dc, long inos)
        {
            backupTaskNumber = tid;
            backupJobNumber = jid;
            job_start = st;
            job_end = et;
            dataCopied = dc;
            numInodes = inos;
        }
    }

    public class BackupTask
    {
        public int serial;
        public string taskName;
        public int numJobsRun;
        public string lastJobTime;
        public string backupSpaceUsage;
        public string status;
        public LinkedList<string> files;
        public LinkedList<string> directories;

        public LinkedList<BackupJob> backupJobs;

        public Boolean isJobActive = false;

        public BackupTask(int s, string tName, int jr, string ljt, string usage, string status1, LinkedList<string> flist, LinkedList<string> dlist)
        {
            serial = s;
            taskName = tName;
            numJobsRun = jr;
            lastJobTime = ljt;
            backupSpaceUsage = usage;
            status = status1;

            files = flist;
            directories = dlist;

            backupJobs = new LinkedList<BackupJob>();
        }

        public void InsertNewCompletedJob(int jobid, string start_time, string end_time, long dataCopied, long numInodes)
        {
            backupJobs.AddLast(new BackupJob(serial, jobid, start_time, end_time, dataCopied, numInodes));
        }
    }

    public class BackupTaskManager
    {
        public LinkedList<BackupTask> listOfTasks = new LinkedList<BackupTask>();
        public string totalSpaceUsage = "111 GB";
        public string containerFolderPath;

        public BackupTaskManager(string containerFolderPath1)
        {
            containerFolderPath = containerFolderPath1;
            LoadBackupListFromDisk();
        }

        public Boolean SetBackupTaskStatus(int taskid, bool isRunning, string msg)
        {
            foreach (BackupTask j in listOfTasks)
            {
                if (j.serial == taskid)
                {
                    j.isJobActive = isRunning;
                    j.status = msg;
                    return true;
                }
            }
            return false;
        }

        public Boolean SetBackupTaskCompletion(int taskid, int jid, bool jobSucesfull, string start, string lastJobTime)
        {
            foreach (BackupTask j in listOfTasks)
            {
                if (j.serial == taskid)
                {
                    j.InsertNewCompletedJob(jid, start, lastJobTime, 0, 0);
                    j.lastJobTime = lastJobTime;
                    j.numJobsRun += jobSucesfull? 1 : 0;
                    return SaveBackUpListToDisk();
                }
            }
            return false;
        }
        public Boolean CreateNewBackupTask(string taskName, LinkedList<string> listOfFiles, LinkedList<string> listOfDirectories)
        {
            listOfTasks.AddLast(new BackupTask(listOfTasks.Count + 1, taskName, 0, "Never", "0 GB", "-", listOfFiles, listOfDirectories));
            return SaveBackUpListToDisk();
        }

        public Boolean DeleteBackupTask(int serial)
        {
            int opid = REDFS.redfsContainer.containerOperations.FindRunningOperation(ALLOWED_OPERATIONS.BACKUP_OP, serial);
            if (opid != -1)
            {
                while (!REDFS.redfsContainer.containerOperations.StopSpecificRunningOperations(opid))
                {
                    Thread.Sleep(100);
                }
            }
            foreach (BackupTask j in listOfTasks)
            {
                if (j.serial == serial)
                {
                    listOfTasks.Remove(j);
                    SaveBackUpListToDisk();
                    return true;
                }
            }
            return false;
        }
        private Boolean SaveBackUpListToDisk()
        {
            try
            {
                string backuplistFile = containerFolderPath + "\\backups.json";
                Console.WriteLine("Saving backuplist for container " + containerFolderPath + " @ location " + backuplistFile);

                using (StreamWriter sw = new StreamWriter(backuplistFile))
                {
                    foreach (var task in listOfTasks)
                    {
                        String vstr = JsonConvert.SerializeObject(task, Formatting.None);
                        Console.WriteLine(vstr);
                        sw.WriteLine(vstr);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("The file could not be read:");
                Console.WriteLine(e.Message);
                return false;
            }
            return true;
        }

        public void LoadBackupListFromDisk()
        {
            try
            {
                string backuplistFile = containerFolderPath + "\\backups.json";
                Console.WriteLine("Reading backup list for container " + containerFolderPath + " @ location " + backuplistFile);
                using (StreamReader sr = new StreamReader(backuplistFile))
                {
                    string line;
                    while ((line = sr.ReadLine()) != null)
                    {
                        Console.WriteLine(line);
                        BackupTask b2 = JsonConvert.DeserializeObject<BackupTask>(line);
                        listOfTasks.AddLast(b2);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("The file could not be read:");
                Console.WriteLine(e.Message);
            }
            Console.WriteLine("Finished reading backu task list from disk");
        }
    }
}
