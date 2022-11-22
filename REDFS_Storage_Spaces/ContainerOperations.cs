using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

/*
 * Operation that is performed on an aggregate, that is the REDFS Aggregate as a whole.
 * These operations are global and affect all the volumes.
 **/
namespace REDFS_ClusterMode
{
    public enum ALLOWED_OPERATIONS
    {
        DUMMY,
        CHUNK_CREATE,
        CHUNK_MOVE,
        CHUNK_PREPARE_REMOVAL,
        COMPRESS_CONTAINER,
        DEDUPE_CONTAINER,
        BACKUP_OP
    }

    public interface OperationsInternalData
    {
        public Boolean makeUnitProgress();
        public Boolean IsComplete();
        public int GetPercentProgress();
    }

    public class OPBackupJob : OperationsInternalData
    {
        int currentBlock = 0;
        int numBlocks = 1000;

        public int backupTaskId;
        public string jobName;

        public DateTime start;
        public DateTime end;

        public OPBackupJob(int id, string name)
        {
            backupTaskId = id;
            jobName = name;
            start = DateTime.Now;
        }

        public Boolean makeUnitProgress()
        {
            currentBlock+=4;
            if (currentBlock > numBlocks)
            {
                currentBlock = numBlocks - 1;
                end = DateTime.Now;
            }
            return true;
        }

        public Boolean IsComplete()
        {
            return (currentBlock == (numBlocks - 1));
        }

        public int GetPercentProgress()
        {
            return (int)(currentBlock * 100) / numBlocks;
        }
    }


    public class OPCompressContainer : OperationsInternalData
    {
        int currentBlock = 0;
        int numBlocks = 1000;

        public OPCompressContainer()
        {

        }

        public Boolean makeUnitProgress()
        {
            currentBlock++;
            return true;
        }

        public Boolean IsComplete()
        {
            return (currentBlock == (numBlocks - 1));
        }

        public int GetPercentProgress()
        {
            return (int)(currentBlock * 100) / numBlocks;
        }
    }

    public class OPDedupeContainer: OperationsInternalData
    {
        int currentBlock =0;
        int numBlocks = 1000;

        public OPDedupeContainer()
        {

        }

        public Boolean makeUnitProgress()
        {
            currentBlock++;
            return true;
        }

        public Boolean IsComplete()
        {
            return (currentBlock == (numBlocks - 1));
        }

        public int GetPercentProgress()
        {
            return (int)(currentBlock * 100) / numBlocks;
        }
    }

    public class OPChunkPrepareForRemoval : OperationsInternalData
    {
        int currentBlock = 0;
        int numBlocks = 1000;

        public OPChunkPrepareForRemoval()
        {

        }

        public Boolean makeUnitProgress()
        {
            currentBlock++;
            return true;
        }

        public Boolean IsComplete()
        {
            return (currentBlock == (numBlocks - 1));
        }

        public int GetPercentProgress()
        {
            return (int)(currentBlock * 100) / numBlocks;
        }
    }

    public class OPChunkMove : OperationsInternalData
    {
        int currentBlock = 0;
        int numBlocks = 1000;

        public OPChunkMove()
        {

        }

        public Boolean makeUnitProgress()
        {
            currentBlock++;
            return true;
        }

        public Boolean IsComplete()
        {
            return (currentBlock == (numBlocks - 1));
        }

        public int GetPercentProgress()
        {
            return (int)(currentBlock * 100) / numBlocks;
        }
    }

    public class OPChunkCreate: OperationsInternalData
    {
        BinaryWriter bw;
        byte[] OneMBBlock = new byte[1024 * 1024];
        int currentBlock;
        int numBlocks;
        public int chunkid;
        public SPAN_TYPE spanTypesAllowed;

        public Boolean doSparseCreate = false;

        public OPChunkCreate(int cid, string path, int Num1MBBlocks, SPAN_TYPE types, bool sparse)
        {
            currentBlock = 0;
            chunkid = cid;
            numBlocks = Num1MBBlocks;
            doSparseCreate = sparse;

            try
            {
                if (doSparseCreate)
                {
                    FileStream f = new FileStream(path, FileMode.Create);
                    f.SetLength((long)Num1MBBlocks * (1024 * 1024));
                    f.Flush();
                    f.Close();

                    currentBlock = numBlocks;
                }
                else
                {
                    bw = new BinaryWriter(new FileStream(path, FileMode.Create));
                }
                spanTypesAllowed = types;
            } 
            catch (Exception e)
            {
                Console.WriteLine("failed to open file, exception : " + e.Message);
            }
            
        }

        public Boolean makeUnitProgress()
        {
            if (!doSparseCreate)
            {
                bw.Write(OneMBBlock);
                bw.Flush();
                currentBlock++;
            }
            return true;
        }
        
        public void CleanupForForceAbort()
        {
            if (bw != null)
            {
                bw.Flush();
                bw.Close();
                bw.Dispose();
                bw = null;
            }
        }

        public Boolean IsComplete()
        {
            if (doSparseCreate)
            {
                return true;
            }

            if (currentBlock == numBlocks)
            {
                if (bw != null)
                {
                    bw.Flush();
                    bw.Close();
                    bw.Dispose();
                    bw = null;
                }
                return true;
            }
            else
            {
                return false;
            }
        }

        public int GetPercentProgress()
        {
            return (int)(currentBlock * 100) / numBlocks;
        }
    }

    public class OperationData
    {
        //incoming data required
        public string OpName;
        public ALLOWED_OPERATIONS OpType;
        public int id;
        public string OpDescription;
        public string OpInputs;
        
        public float progress { get; set; }
        public string result { get; set; }
        public string opmessage { get; set; }


        public OperationsInternalData internalData;

        //For ux, outgoing
        public int width = 0;
        public string color = "#000000";

        public ContainerOperationsThread runningThread;

        //For local tracking.
        DateTime startTime;
        DateTime endTime;

        public OperationData(ALLOWED_OPERATIONS type, string n, int id1, string desc, string msg, string input)
        {
            OpType = type;
            OpName = n;
            id = id1;
            OpDescription = desc;
            OpInputs = input;

            opmessage = msg;
            result = "Pending";

            startTime = DateTime.Now;
        }

        public void updateProgress(float percent, string msg)
        {
            opmessage = msg;
            width = (int)percent;
            progress = percent;

            if (percent > 0)
            {
                result = "Running";
            }
        }

        public void completeOperation(string msg)
        {
            opmessage = msg;
            result = "Completed";
            endTime = DateTime.Now;
        }
    }

    public class ContainerOperationsThread
    {
        public Boolean abortOp = false;
        public Boolean threadNotRunning = true;
        public int id;
        ContainerOperations cops;
        public int delay_ms = 10;

        public void StartOperation(ContainerOperations co, int idx, int delay)
        {
            cops = co;
            id = idx;
            delay_ms = delay;

            Thread tc = new Thread(new ThreadStart(workerThread));
            tc.Start();
            threadNotRunning = false;
        }

        public void AbortOperation()
        {
            abortOp = true;
            OperationData d = cops.GetOperationStruct(id);
            d.updateProgress(100, "Aborted");
        }

        public void workerThread()
        {
            OperationData d = cops.GetOperationStruct(id);

            //Initialize
            switch(d.OpType)
            {
                case ALLOWED_OPERATIONS.CHUNK_CREATE:
                    string[] inputs = d.OpInputs.Split(",");
                    int chunkid = int.Parse(inputs[0]);
                    string fileName = inputs[1]; //file name where chunk is
                    int sizeInGB = int.Parse(inputs[2]);

                    Enum.TryParse(inputs[3], out SPAN_TYPE segTypes);

                    d.internalData = new OPChunkCreate(chunkid, fileName, sizeInGB * 1024, segTypes, true);
                    break;
                case ALLOWED_OPERATIONS.CHUNK_PREPARE_REMOVAL:
                    d.internalData = new OPChunkPrepareForRemoval();
                    break;
                case ALLOWED_OPERATIONS.CHUNK_MOVE:
                    d.internalData = new OPChunkMove();
                    string[] inputs1 = d.OpInputs.Split(",");
                    int chunkid1 = int.Parse(inputs1[0]);
                    ((ChunkInfo)REDFS.redfsContainer.redfsChunks[chunkid1]).status = "Moving..";
                    ((ChunkInfo)REDFS.redfsContainer.redfsChunks[chunkid1]).isReadOnly = true;
                    ((ChunkInfo)REDFS.redfsContainer.redfsChunks[chunkid1]).isBeingPreparedForRemoval = true;
                    break;
                case ALLOWED_OPERATIONS.COMPRESS_CONTAINER:
                    d.internalData = new OPCompressContainer();
                    break;
                case ALLOWED_OPERATIONS.DEDUPE_CONTAINER:
                    d.internalData = new OPDedupeContainer();
                    break;
                case ALLOWED_OPERATIONS.BACKUP_OP:
                    
                    string[] inputs2 = d.OpInputs.Split(",");
                    int taskid = int.Parse(inputs2[0]);
                    string jobname = inputs2[1];
                    d.internalData = new OPBackupJob(taskid, jobname);
                    REDFS.redfsContainer.backupManager.SetBackupTaskStatus(taskid, true, "Running");
                    break;
                default:
                    break;
            } 
            
            while (!abortOp)
            {
                Thread.Sleep(delay_ms);
                switch (d.OpType)
                {
                    case ALLOWED_OPERATIONS.CHUNK_CREATE:
                        int chunkid = ((OPChunkCreate)d.internalData).chunkid;
                        d.internalData.makeUnitProgress();
                        d.updateProgress(d.internalData.GetPercentProgress(), "In progress");

                        if (d.internalData.IsComplete())
                        {
                            d.updateProgress(100, "Completed");
                            ((ChunkInfo)REDFS.redfsContainer.redfsChunks[chunkid]).creationInProgress = false;
                            ((ChunkInfo)REDFS.redfsContainer.redfsChunks[chunkid]).status = "Ready";
                            ((ChunkInfo)REDFS.redfsContainer.redfsChunks[chunkid]).chunkIsAccessible = true;
                            REDFS.redfsContainer.SaveChunkListToDisk("save chunk create");

                            //Make this live and usable.
                            REDFS.redfsContainer.ifsd_mux.redfsCore.InsertChunk((ChunkInfo)REDFS.redfsContainer.redfsChunks[chunkid]);
                            abortOp = true;
                        }
                        break;
                    case ALLOWED_OPERATIONS.CHUNK_PREPARE_REMOVAL:
                        d.internalData.makeUnitProgress();
                        d.updateProgress(d.internalData.GetPercentProgress(), "In progress");

                        if (d.internalData.IsComplete())
                        {
                            d.updateProgress(100, "Completed");

                            abortOp = true;
                        }
                        break;
                    case ALLOWED_OPERATIONS.CHUNK_MOVE:
                        int chunkid1 = ((OPChunkCreate)d.internalData).chunkid;
                        d.internalData.makeUnitProgress();
                        d.updateProgress(d.internalData.GetPercentProgress(), "In progress");

                        if (d.internalData.IsComplete())
                        {
                            d.updateProgress(100, "Completed");
                            ((ChunkInfo)REDFS.redfsContainer.redfsChunks[chunkid1]).canDeleteChunk = true;
                            ((ChunkInfo)REDFS.redfsContainer.redfsChunks[chunkid1]).status = "Moved";
                            ((ChunkInfo)REDFS.redfsContainer.redfsChunks[chunkid1]).isReadOnly = true;
                            ((ChunkInfo)REDFS.redfsContainer.redfsChunks[chunkid1]).isBeingPreparedForRemoval = true;
                            REDFS.redfsContainer.SaveChunkListToDisk("from move chunk");

                            //We also have to update the persistant storage
                            abortOp = true;
                        }
                        break;
                    case ALLOWED_OPERATIONS.COMPRESS_CONTAINER:
                        d.internalData.makeUnitProgress();
                        d.updateProgress(d.internalData.GetPercentProgress(), "In progress");

                        if (d.internalData.IsComplete())
                        {
                            d.updateProgress(100, "Completed");

                            abortOp = true;
                        }
                        break;
                    case ALLOWED_OPERATIONS.DEDUPE_CONTAINER:
                        d.internalData.makeUnitProgress();
                        d.updateProgress(d.internalData.GetPercentProgress(), "In progress");

                        if (d.internalData.IsComplete())
                        {
                            d.updateProgress(100, "Completed");

                            abortOp = true;
                        }
                        break;
                    case ALLOWED_OPERATIONS.BACKUP_OP:
                        d.internalData.makeUnitProgress();
                        d.updateProgress(d.internalData.GetPercentProgress(), "In Progress");

                        if (d.internalData.IsComplete())
                        {
                            REDFS.redfsContainer.backupManager.SetBackupTaskStatus(((OPBackupJob)d.internalData).backupTaskId, false, "Idle");
                            REDFS.redfsContainer.backupManager.SetBackupTaskCompletion(((OPBackupJob)d.internalData).backupTaskId, 0, true,
                                    ((OPBackupJob)d.internalData).start.ToString(), ((OPBackupJob)d.internalData).end.ToString());

                            d.updateProgress(100, "Completed");
                            abortOp = true;
                        }
                        break;
                    default:
                        break;
                }
            }

            //Now that we are aborting the thread, lets see what we need to do for cleanup in each case
            if (abortOp)
            {
                switch(d.OpType)
                {
                    case ALLOWED_OPERATIONS.CHUNK_CREATE:
                        if (!d.internalData.IsComplete())
                        {   
                            ((OPChunkCreate)d.internalData).CleanupForForceAbort();
                            d.updateProgress(100, "Aborted");
                        }
                        break;
                    case ALLOWED_OPERATIONS.BACKUP_OP:
                        if (!d.internalData.IsComplete())
                        {
                            //Backup is not completed fully, what should we do?. Delete incomplete backup?
                        }
                        break;
                    default:
                        break;
                }
            }
            threadNotRunning = true;
        }
    }

    public class ContainerOperations
    {
        //Zero volume is not mounted anyway
        public long currentlyMountedVolume = 0;

        public IDictionary inProgressOps = new Dictionary<int, OperationData>();
        public List<OperationData> inProgressOpsList = new List<OperationData>();

        public void updateProgress()
        {
            PopulateListFromDictionary();
        }

        /* Returns true ones all the threads are stopped */
        public bool StopAllRunningOperations()
        {
            bool isSomeThreadRunning = false;
            foreach (OperationData d in inProgressOps.Values)
            {
                d.runningThread.abortOp = true;
                if (d.runningThread.threadNotRunning == false)
                {
                    isSomeThreadRunning = true;
                }
            }
            return (isSomeThreadRunning)? false : true;
        }

        public bool StopSpecificRunningOperations(int id)
        {
            foreach (OperationData d in inProgressOps.Values)
            {
                if (d.id == id)
                {
                    d.runningThread.abortOp = true;
                    return (d.runningThread.threadNotRunning);
                }
            }
            return false;
        }

        public ContainerOperations()
        {

        }

        public OperationData GetOperationStruct(int id)
        {
            return (OperationData)inProgressOps[id];
        }

        public Boolean RemoveCompletedOperation(int id)
        {
            if(inProgressOps.Contains(id))
            {
                inProgressOps.Remove(id);
                return true;
            }
            return false;
        }

        public int FindRunningOperation(ALLOWED_OPERATIONS type, int id)
        {
            foreach (OperationData d in inProgressOps.Values)
            {
                if (d.OpType == type)
                {
                    switch (type)
                    {
                        case ALLOWED_OPERATIONS.CHUNK_CREATE:
                            OPChunkCreate oc = (OPChunkCreate)d.internalData;
                            if (oc.chunkid == id)
                            {
                                return d.id;
                            }
                            break;
                        case ALLOWED_OPERATIONS.BACKUP_OP:
                            OPBackupJob ob = (OPBackupJob)d.internalData;
                            if (ob.backupTaskId == id)
                            {
                                return d.id;
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
            return -1;
        }

        public int CreateNewOperation(ALLOWED_OPERATIONS type, string name, string description, string input)
        {
            int maxid = 0;
            foreach (OperationData d in inProgressOps.Values)
            {
                if (maxid < ((OperationData)d).id)
                {
                    maxid = ((OperationData)d).id;
                }
            }

            OperationData od = new OperationData(type, name, maxid + 1, description, "", input);
            inProgressOps.Add(maxid + 1, od);
            
            ContainerOperationsThread ct = new ContainerOperationsThread();
            od.runningThread = ct;
            ct.StartOperation(this, maxid + 1, 10);

            return maxid + 1;
        }

        public void PopulateListFromDictionary()
        {
            inProgressOpsList.Clear();
            foreach (var op in inProgressOps.Values)
            {
                inProgressOpsList.Add((OperationData)op);
            }
        }
    }
}
