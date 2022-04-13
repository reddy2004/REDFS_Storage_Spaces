using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace REDFS_ClusterMode
{
    public class ChunkFileStorage
    {
        int         chunkID;
        string      chunkFilePath;
        FileStream  chunkFileHandle;
        Boolean     initialized = false;

        long bytesRead;
        long bytesWritten;
        long chunkSizeInGB;

        public ChunkFileStorage(int id, string path, long sizeingb)
        {
            bytesRead = bytesWritten = 0;
            chunkID = id;
            chunkFilePath = path;
            chunkSizeInGB = sizeingb;
        }

        public Boolean Init()
        {
            if (File.Exists(chunkFilePath))
            {
                chunkFileHandle = new FileStream(chunkFilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite);
                int sizeInGB = (int)((long)chunkFileHandle.Length / (1024 * 1024 * 1024));
                Console.WriteLine("size = " + sizeInGB);
                if (sizeInGB == chunkSizeInGB)
                {
                    initialized = true;
                    return true;
                }
                return false;
            }
            return false;
        }

        public void Flush()
        {
            chunkFileHandle.Flush();
        }

        public void Close()
        {
            chunkFileHandle.Flush();
            chunkFileHandle.Close();
            initialized = false;
        }

        public int Read(byte[] buffer, long fileOffset, int bufferOffset, int size)
        {
            if (!initialized)
            {
                throw new SystemException("File not ready for reading!");
            }
            chunkFileHandle.Seek(fileOffset, SeekOrigin.Begin);
            int amt = chunkFileHandle.Read(buffer, bufferOffset, size);
            bytesRead += size;
            return amt;
        }

        public void Write(byte[] buffer, long fileOffset, int bufferOffset, int size)
        {
            if (!initialized)
            {
                throw new SystemException("File not ready for writing!");
            }
            chunkFileHandle.Seek(fileOffset, SeekOrigin.Begin);
            chunkFileHandle.Write(buffer, bufferOffset, size);
            chunkFileHandle.Flush();
            bytesWritten += size;
        }
    }

    /*
     * Writes:
     * The cleaner writes all the data which is dirty in memory. Otherwise
     * the write cost goes directly to the client write io path.
     * 
     * Reads:
     * Reads happen in the client io path. If its already in memory we just
     * serve from the cache. Cleaner also removes read buffers to keep the
     * cache utilization under control.
     */ 
    public class RedFSPersistantStorage
    {
        private FileStream clogfile;

        private bool initialized;

        public long total_disk_reads;
        public long total_disk_writes;

        private Item fptemp = new fingerprintCLOG(0);
        private int fpcache_cnt = 0;
        private byte[] fpcache_buf = new byte[36 * 1024];
        private MD5 md5 = System.Security.Cryptography.MD5.Create();

        public IDictionary chunkFileHandles = new Dictionary<int, ChunkFileStorage>();

        public void swap_clog()
        {
            flush_clog();
            lock (fpcache_buf)
            {
                clogfile.Flush();
                clogfile.Close();

                Directory.CreateDirectory(REDFS.getAbsoluteContainerPath() + "\\dedupe\\");
                File.Move(REDFS.getAbsoluteContainerPath() + "\\dedupe\\clog", REDFS.getAbsoluteContainerPath() + "\\dedupe\\clog1");
                clogfile = new FileStream(REDFS.getAbsoluteContainerPath() + "\\dedupe\\clog",
                        FileMode.OpenOrCreate, FileAccess.ReadWrite);
            }
        }

        public void flush_clog()
        {
            lock (fpcache_buf)
            {
                if (fpcache_cnt > 0)
                {
                    clogfile.Write(fpcache_buf, 0, fpcache_cnt * 36);
                    fpcache_cnt = 0;
                }
            }
        }

        private void CheckSumBuf(RedFS_Inode wip, int fbn, int dbn, byte[] buffer, int offset)
        {
            lock (fpcache_buf)
            {
                if (fpcache_cnt == 1024)
                {
                    fpcache_cnt = 0;
                    clogfile.Write(fpcache_buf, 0, fpcache_buf.Length);
                    clogfile.Flush();
                }

                if (wip.get_wiptype() == WIP_TYPE.REGULAR_FILE)
                {
                    fingerprintCLOG fpt = (fingerprintCLOG)fptemp;

                    fpt.fsid = wip.get_filefsid();
                    fpt.inode = wip.get_ino();
                    fpt.fbn = fbn;
                    fpt.dbn = dbn;
                    fpt.cnt = (int)clogfile.Position;

                    byte[] hash = md5.ComputeHash(buffer, offset, 4096);
                    for (int i = 0; i < 16; i++)
                    {
                        fpt.fp[i] = hash[i];
                    }

                    fptemp.get_bytes(fpcache_buf, fpcache_cnt * fptemp.get_size());
                    fpcache_cnt++;
                }
            }
        }
        private void CheckSumBuf(RedFS_Inode wip, Red_Buffer wb)
        {
            lock (fpcache_buf)
            {
                if (fpcache_cnt == 1024)
                {
                    fpcache_cnt = 0;
                    clogfile.Write(fpcache_buf, 0, fpcache_buf.Length);
                    clogfile.Flush();
                }

                if (wb.get_level() == 0 && wip.get_wiptype() == WIP_TYPE.REGULAR_FILE)
                {
                    fingerprintCLOG fpt = (fingerprintCLOG)fptemp;

                    fpt.fsid = wip.get_filefsid();
                    fpt.inode = wip.get_ino();
                    fpt.fbn = (int)wb.get_start_fbn();
                    fpt.dbn = wb.get_ondisk_dbn();
                    fpt.cnt = (int)clogfile.Position;

                    byte[] hash = md5.ComputeHash(wb.buf_to_data());
                    for (int i = 0; i < 16; i++)
                    {
                        fpt.fp[i] = hash[i];
                    }

                    fptemp.get_bytes(fpcache_buf, fpcache_cnt * fptemp.get_size());
                    fpcache_cnt++;
                }
            }
        }

        public RedFSPersistantStorage(string containerName)
        {
            Directory.CreateDirectory(REDFS.getInterpretedContainerPath(containerName) + "\\dedupe");
            clogfile = new FileStream(REDFS.getInterpretedContainerPath(containerName) + "\\dedupe\\clog",
                    FileMode.OpenOrCreate, FileAccess.ReadWrite);

            initialized = true;
        }
        
        /*
         * The container might not insert all the known chunks if it knows for ex, that some of them may not be written to.
         * It could also be possible that a new chunk is inserted dynamically and after that we start seeing read/writes to it
         */ 
        public void InsertChunk(ChunkInfo ci)
        {
            ChunkFileStorage cfs = new ChunkFileStorage(ci.id, ci.path, ci.size);
            cfs.Init();
            chunkFileHandles.Add(ci.id, cfs);
        }

        public RedFS_FSID read_fsid(int fsid)
        {
            if (!initialized) return null;

            ChunkFileStorage cfs = (ChunkFileStorage)chunkFileHandles[0];

            lock (cfs)
            {
                byte[] buffer = new byte[OPS.FSID_BLOCK_SIZE];
                cfs.Read(buffer, fsid * OPS.FSID_BLOCK_SIZE, 0, OPS.FSID_BLOCK_SIZE);
                RedFS_FSID fs = new RedFS_FSID(fsid, buffer);
                return fs;
            }
        }

        public bool write_fsid(RedFS_FSID wbfsid)
        {
            if (!initialized) return false;

            ChunkFileStorage cfs = (ChunkFileStorage)chunkFileHandles[0];

            lock (cfs)
            {
                cfs.Write(wbfsid.data, (long)wbfsid.get_fsid() * OPS.FSID_BLOCK_SIZE, 0, OPS.FSID_BLOCK_SIZE);
                cfs.Flush();
                wbfsid.set_dirty(false);
            }
            return true;
        }

        public void ExecuteReadPlanSingle(ReadPlanElement r,Red_Buffer wb)
        {
            ChunkFileStorage cfs = (ChunkFileStorage)chunkFileHandles[r.chunkId];
            lock (cfs)
            {
                cfs.Read(wb.buf_to_data(), r.readOffset, 0, wb.buf_to_data().Length);
            }
        }

        public void ExecuteReadPlanSingle(ReadPlanElement r, byte[] buffer, int offset, int size)
        {
            ChunkFileStorage cfs = (ChunkFileStorage)chunkFileHandles[r.chunkId];
            lock (cfs)
            {
                //calling public int Read(byte[] buffer, long fileOffset, int bufferOffset, int size)
                cfs.Read(buffer, r.readOffset, offset, size);
            }
        }

        /*
         * Reads can be coaleased, but writes cannot
         */
        public bool ExecuteReadPlan(List<ReadPlanElement> rpe, List<Red_Buffer> wbList)
        {
            DEFS.ASSERT(rpe.Count == wbList.Count, "Size must match!");

            for (int i=0;i<rpe.Count; i++)
            {
                ReadPlanElement r = rpe[i];
                Red_Buffer wb = wbList[i];
                DEFS.ASSERT(wb.get_ondisk_dbn() == r.dbn, "DBNS must be in the same order");

                ChunkFileStorage cfs = (ChunkFileStorage)chunkFileHandles[r.chunkId];
                DEFS.ASSERT(cfs != null, "Chunk must exist");
                lock (cfs)
                {
                    cfs.Read(wb.buf_to_data(), r.readOffset, 0, wb.buf_to_data().Length);
                }

            }
            return true;
        }


        public void ExecuteWritePlanSingle(WritePlanElement r, RedFS_Inode wip, Red_Buffer wb)
        {
            ChunkFileStorage cfs1 = (ChunkFileStorage)chunkFileHandles[r.dataChunkIds[0]];
            DEFS.ASSERT(cfs1 != null, "Chunk must exist");
            lock (cfs1)
            {
                cfs1.Write(wb.buf_to_data(), r.writeOffsets[0], 0, wb.buf_to_data().Length);
            }

            if (r.dataChunkIds.Length == 2)
            {
                ChunkFileStorage cfs2 = (ChunkFileStorage)chunkFileHandles[r.dataChunkIds[1]];
                DEFS.ASSERT(cfs2 != null, "Chunk must exist");
                lock (cfs2)
                {
                    cfs2.Write(wb.buf_to_data(), r.writeOffsets[1], 0, wb.buf_to_data().Length);
                }
            }

            wb.set_dirty(false);
        }

        public void ExecuteWritePlanSingle(WritePlanElement r, RedFS_Inode wip, byte[] buffer)
        {
            ChunkFileStorage cfs1 = (ChunkFileStorage)chunkFileHandles[r.dataChunkIds[0]];
            DEFS.ASSERT(cfs1 != null, "Chunk must exist");
            lock (cfs1)
            {
                cfs1.Write(buffer, r.writeOffsets[0], 0, buffer.Length);
            }

            if (r.dataChunkIds.Length == 2)
            {
                ChunkFileStorage cfs2 = (ChunkFileStorage)chunkFileHandles[r.dataChunkIds[1]];
                DEFS.ASSERT(cfs2 != null, "Chunk must exist");
                lock (cfs2)
                {
                    cfs2.Write(buffer, r.writeOffsets[0], 0, buffer.Length);
                }
            }
        }

        /*
         * Note reads are  coalesed, but writes are not. We have only one writeplan element for only
         * one file aka wip
         */
        public bool ExecuteWritePlan(WritePlanElement wpe, RedFS_Inode wip, List<Red_Buffer> wbList)
        {
            //Default
            if (wpe.dbns.Length == 1) //Default
            {
                ChunkFileStorage cfs = (ChunkFileStorage)chunkFileHandles[wpe.dataChunkIds[0]];
                DEFS.ASSERT(cfs != null, "Chunk must exist");
                lock (cfs)
                {
                    cfs.Write(wbList[0].buf_to_data(), wpe.writeOffsets[0], 0, wbList[0].buf_to_data().Length);
                    //Console.WriteLine(OPS.HashToString(wbList[0].buf_to_data()));
                    Console.WriteLine("Writing to loc ..... " + wpe.writeOffsets[0]);
                }
            }
            else if (wpe.dbns.Length == 2) //Mirror
            {
                ChunkFileStorage cfs0 = (ChunkFileStorage)chunkFileHandles[wpe.dataChunkIds[0]];
                DEFS.ASSERT(cfs0 != null, "Chunk must exist");
                lock (cfs0)
                {
                    cfs0.Write(wbList[0].buf_to_data(), wpe.writeOffsets[0], 0, wbList[0].buf_to_data().Length);
                }
                ChunkFileStorage cfs1 = (ChunkFileStorage)chunkFileHandles[wpe.dataChunkIds[1]];
                DEFS.ASSERT(cfs1 != null, "Chunk must exist");
                lock (cfs1)
                {
                    cfs1.Write(wbList[0].buf_to_data(), wpe.writeOffsets[1], 0, wbList[0].buf_to_data().Length);
                }
            }
            else if (wpe.dbns.Length == 5) //4D+1P
            {
                ChunkFileStorage cfs0 = (ChunkFileStorage)chunkFileHandles[wpe.dataChunkIds[0]];
                DEFS.ASSERT(cfs0 != null, "Chunk must exist");
                lock (cfs0)
                {
                    cfs0.Write(wbList[0].buf_to_data(), wpe.writeOffsets[0], 0, wbList[0].buf_to_data().Length);
                }
                ChunkFileStorage cfs1 = (ChunkFileStorage)chunkFileHandles[wpe.dataChunkIds[1]];
                DEFS.ASSERT(cfs1 != null, "Chunk must exist");
                lock (cfs1)
                {
                    cfs1.Write(wbList[1].buf_to_data(), wpe.writeOffsets[1], 0, wbList[1].buf_to_data().Length);
                }
                ChunkFileStorage cfs2 = (ChunkFileStorage)chunkFileHandles[wpe.dataChunkIds[2]];
                DEFS.ASSERT(cfs2 != null, "Chunk must exist");
                lock (cfs2)
                {
                    cfs2.Write(wbList[2].buf_to_data(), wpe.writeOffsets[2], 0, wbList[2].buf_to_data().Length);
                }
                ChunkFileStorage cfs3 = (ChunkFileStorage)chunkFileHandles[wpe.dataChunkIds[3]];
                DEFS.ASSERT(cfs3 != null, "Chunk must exist");
                lock (cfs3)
                {
                    cfs3.Write(wbList[3].buf_to_data(), wpe.writeOffsets[3], 0, wbList[3].buf_to_data().Length);
                }

                ChunkFileStorage cfsp = (ChunkFileStorage)chunkFileHandles[wpe.parityChunkIds[0]];
                DEFS.ASSERT(cfsp != null, "Chunk must exist");
                lock (cfsp)
                {
                    //Should checksum and write out the checksum
                    cfsp.Write(wbList[4].buf_to_data(), wpe.writeOffsets[4], 0, wbList[4].buf_to_data().Length);
                }
            }
            return false;
        }


        public void shut_down()
        {
            if (initialized == false) return;

            clogfile.Flush();
            clogfile.Close();
            foreach(ChunkFileStorage chunk in chunkFileHandles.Values)
            {
                chunk.Close();
            }
            initialized = false;
        }
    }
}
