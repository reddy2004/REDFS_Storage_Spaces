using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;

/*
 * This class manages two files.
 * allocMap
 * allocMap.x
 * 
 * allocMap is a 1GB file that has refcount information of all the blocks in the filesystem. Totally amounting to 64TB user data refcounts.
 * allocMap.x simply keeps a 4 byte summary of the information of refcount file
 */
namespace REDFS_ClusterMode
{
    public class MapBuffer
    {
        private long m_creation_time;
        public bool is_dirty;
        public byte[] data = new byte[OPS.SIZE_OF_MAPBUFS]; /* 256 kb, i.e 256 * 1024 * 8 bits => 16 GB worth of userdata refcounts */
        public static int NUM_BITS_IN_MBUF = OPS.SIZE_OF_MAPBUFS * 8;
        
        public MapBuffer() { 
            m_creation_time = DateTime.Now.ToUniversalTime().Ticks; 
        }

        public Boolean Equals(MapBuffer other)
        {
            for (int i = 0; i < (OPS.SIZE_OF_MAPBUFS); i++)
            {
                if (this.data[i] != other.data[i])
                {
                    return false;
                }
            }
            return true;
        }

        public int get_buf_age()
        {
            long elapsed = (DateTime.Now.ToUniversalTime().Ticks - m_creation_time);
            return (int)(elapsed / 10000000);
        }

        public void touch_buf()
        {
            m_creation_time = DateTime.Now.ToUniversalTime().Ticks;
        }
    }

    public class Map128TB
    {
        private FileStream mfile; /* Actual bitmap file indicating the refcount for each dbn in the filesystem */
        private FileStream xfile; /* 4 byte file which says how many blocks are free */

        /*
         * 256K * 8192 => 2GB file, Each 8k block needs 1 bit, so 128 TB file system.
         * So we could have 4096 MapBuffers() where each MapBuffer has allocbit for 16GB of DBN space.
         */
        private MapBuffer[] mbufs = new MapBuffer[OPS.NUM_MAPBUFS_FOR_CONTAINER];
        private Boolean mTerminateThread = false;
        private Boolean mThreadTerminated = true;

        public Boolean Equals(Map128TB other)
        {
            for (int idx = 0; idx < OPS.NUM_MAPBUFS_FOR_CONTAINER; idx++)
            {
                load_mbx(idx, true);
                other.load_mbx(idx, true);

                if (!mbufs[idx].Equals(other.mbufs[idx]))
                {
                    return false;
                }
            }
            return true;
        }

        private int dbn_to_mbufidx(long dbn)
        {
            //return (int)(dbn / OPS.SIZE_OF_MAPBUFS);  /*For each 256kb MapBuffer */
            return (int)(dbn / MapBuffer.NUM_BITS_IN_MBUF);
        }

        private int dbn_to_bitidx(long dbn) 
        { 
            return (int)(dbn % 8);
        }

        private int dbn_to_mbufoffset(long dbn) 
        {
            return (int)(dbn % MapBuffer.NUM_BITS_IN_MBUF) / 8;
        }

        private void load_mbx(int idx, bool read_only)
        {
            if (mbufs[idx] == null)
            {
                mbufs[idx] = new MapBuffer();
                mfile.Seek((long)idx * (OPS.SIZE_OF_MAPBUFS), SeekOrigin.Begin);
                mfile.Read(mbufs[idx].data, 0, (256 * 1024));
            }
            if (!read_only)
            {
                mbufs[idx].touch_buf();
            }
        }

        private bool initialized = false;
        public long USED_BLK_COUNT = 0;

        private long start_dbn_single = 1024; /* Where the first free dbn for user data is located */

        public Map128TB(string name)
        {
            mfile = new FileStream(REDFS.getAbsoluteContainerPath() + "\\" + name,
                    FileMode.OpenOrCreate, FileAccess.ReadWrite);
            xfile = new FileStream(REDFS.getAbsoluteContainerPath() + "\\" + name + ".x",
                FileMode.OpenOrCreate, FileAccess.ReadWrite);
            initialized = true;

            mfile.SetLength(OPS.MAPFILE_SIZE_IN_BYTES); //2 GB
            xfile.SetLength(8);
            byte[] buf = new byte[8];
            xfile.Read(buf, 0, 8);
            USED_BLK_COUNT = OPS.get_dbn(buf, 0);
        }

        public void init()
        {
            //Now lets start the delete log thread. this thread basically frees up blocks that are already queue for being reclaimed
            Thread tc = new Thread(new ThreadStart(tServiceThread));
            tc.Start();
        }

        public void tServiceThread()
        {
            while(!mTerminateThread)
            {
                int count = GLOBALQ.m_deletelog2.Count;
                if (count < 1000)
                {
                    Thread.Sleep(10);
                }
                lock (GLOBALQ.m_deletelog2)
                {
                    if (count > 1000) count = 1000;

                    try
                    {
                        for (int i = 0; i < count; i++)
                        {
                            long dbn = GLOBALQ.m_deletelog2.ElementAt(0);
                            GLOBALQ.m_deletelog2.RemoveAt(0);

                            free_bit(dbn);

                            GLOBALQ.m_deletelog_spanmap.Add(dbn);
                            REDFSCoreSideMetrics.m.InsertMetric(METRIC_NAME.USED_BLOCK_COUNT, USED_BLK_COUNT); //trails by 1 unit

                        }
                    }
                    catch (Exception e)
                    {
                        throw new SystemException("EXEPTION : Caught in dellog : cnt = " + count + " and size = " +
                            GLOBALQ.m_deletelog2.Count + " e.msg = " + e.Message);
                    }
                }
            }
            mThreadTerminated = true;
        }
        private void dealloc_bit_internal(long dbn)
        {
            int idx = dbn_to_mbufidx(dbn);
            load_mbx(idx, false);

            int offset = dbn_to_mbufoffset(dbn);
            int bitshift = dbn_to_bitidx(dbn);

            mbufs[idx].data[offset] &= (byte)(~(1 << bitshift));
            mbufs[idx].is_dirty = true;
            mbufs[idx].touch_buf();
            start_dbn_single = dbn;

            USED_BLK_COUNT--;
        }
        private void set_bit_internal(long dbn)
        {
            int idx = dbn_to_mbufidx(dbn);
            load_mbx(idx, false);

            int offset = dbn_to_mbufoffset(dbn);
            int bitshift = dbn_to_bitidx(dbn);

            mbufs[idx].data[offset] |= (byte)(1 << bitshift);
            mbufs[idx].touch_buf();
            mbufs[idx].is_dirty = true;

            USED_BLK_COUNT++;
        }

        private bool get_bit_internal(long dbn)
        {
            int idx = dbn_to_mbufidx(dbn);
            load_mbx(idx, false);

            int offset = dbn_to_mbufoffset(dbn);
            int bitshift = dbn_to_bitidx(dbn);

            int ret = (mbufs[idx].data[offset] >> bitshift) & 0x01;
            mbufs[idx].touch_buf();
            return (ret == 0x01) ? true : false;
        }

        private bool alloc_bit_internal(long dbn)
        {
            if (get_bit_internal(dbn))
            {
                return false;
            }
            else
            {
                set_bit_internal(dbn);
                return true;
            }
        }

        /*
         * External interface, uses locks
         */
        public void fsid_setbit(int dbn)
        {

            if (!initialized) return;

            lock (mfile)
            {
                set_bit_internal(dbn);
            }
        }

        public bool fsid_checkbit(long dbn)
        {
            if (!initialized) return false;

            lock (mfile)
            {
                return get_bit_internal(dbn);
            }
        }

        public bool is_block_free(int dbn)
        {
            if (!initialized) return false;
            lock (mfile)
            {
                return !get_bit_internal(dbn);
            }
        }

        public void free_bit(long dbn)
        {
            if (!initialized) return;

            lock (mfile)
            {
                dealloc_bit_internal(dbn);
            }
        }

        public bool try_alloc_bit(int dbn)
        {
            if (!initialized) return false;

            lock (mfile)
            {
                return alloc_bit_internal(dbn);
            }
        }

        //int test_quick_alloc = 1024;

        public long[] allocate_Dbns(long start_dbn_computed, long start_dbn, long end_dbn, int num_dbns)
        {
            if (!initialized) return Array.Empty<long>();

            start_dbn = (start_dbn == 0) ? OPS.MIN_ALLOCATABLE_DBN : start_dbn;

            long[] allocatedDbns = new long[num_dbns];
            int counter = 0;

            lock (mfile)
            {
                Boolean stripe = (num_dbns >= 4 && num_dbns % 4 == 0) ? true : false;

                int sidx = dbn_to_mbufidx(start_dbn);
                int eidx = dbn_to_mbufidx(end_dbn);

                REDFSCoreSideMetrics.m.StartMetric(METRIC_NAME.DBN_ALLOC_MS_1, 1);

                for (int idx = sidx; idx <= eidx; idx++)
                {
                    long sdbn = start_dbn + idx*MapBuffer.NUM_BITS_IN_MBUF;
                    load_mbx(idx, false);

                    if (start_dbn_computed - sdbn < MapBuffer.NUM_BITS_IN_MBUF)
                    {
                        sdbn = start_dbn_computed; //start higher
                    }

                    for (long dbn = sdbn; dbn < ((idx + 1) * MapBuffer.NUM_BITS_IN_MBUF); dbn++)
                    {
                        if (stripe && (dbn % 4) == 0 && counter < num_dbns && (num_dbns - counter >= 4))
                        {
                            bool firstBit = alloc_bit_internal(dbn);
                            bool secondBit = alloc_bit_internal(dbn + 1);
                            bool thirdBit = alloc_bit_internal(dbn + 2);
                            bool fourthBit = alloc_bit_internal(dbn + 3);

                            if (firstBit && secondBit && thirdBit && fourthBit)
                            {
                                allocatedDbns[counter++] = dbn;
                                allocatedDbns[counter++] = dbn+1;
                                allocatedDbns[counter++] = dbn+2;
                                allocatedDbns[counter++] = dbn+3;
                                dbn += 3; //we do dbn++ in loop as well
                            }
                            else
                            {
                                //cound'nt find 4 continguious entries. :(
                                if (firstBit) dealloc_bit_internal(dbn);
                                if (secondBit) dealloc_bit_internal(dbn+1);
                                if (thirdBit) dealloc_bit_internal(dbn+2);
                                if (fourthBit) dealloc_bit_internal(dbn+3);
                            }
                        }
                        else
                        {
                            REDFSCoreSideMetrics.m.StartMetric(METRIC_NAME.DBN_ALLOC_MS_2, 1);
                            //go dbn by dbn
                            if (alloc_bit_internal(dbn))
                            {
                                allocatedDbns[counter++] = dbn;
                            }
                            REDFSCoreSideMetrics.m.StopMetric(METRIC_NAME.DBN_ALLOC_MS_2, 1);
                        }
                        if (counter >= num_dbns)
                        {
                            return allocatedDbns;
                        }
                    }
                }

                //We failed to allocate dbns. Lets free everything and return, XXX todo
                for (int i=0;i<counter;i++)
                {
                    dealloc_bit_internal(allocatedDbns[i]);
                }

                REDFSCoreSideMetrics.m.StopMetric(METRIC_NAME.DBN_ALLOC_MS_1, 1);
            }
            return Array.Empty<long>();
        }

        public long allocate_bit()
        {
            //If this path is fast, we can touch upto 70Mbps!.
            //if (test_quick_alloc != 0)
            //    return test_quick_alloc++;

            if (!initialized) return -1;

            lock (mfile)
            {
                int sidx = dbn_to_mbufidx(start_dbn_single);

                for (int idx = sidx; idx < 1024; idx++)
                {
                    DEFS.ASSERT(start_dbn_single >= (OPS.SIZE_OF_MAPBUFS * sidx), "Incorrect starting point in allocate_bit");

                    long sdbn = start_dbn_single;
                    load_mbx(idx, false);

                    for (long dbn = sdbn; dbn < ((idx + 1) * OPS.SIZE_OF_MAPBUFS); dbn++)
                    {
                        if (alloc_bit_internal(dbn)) return dbn;
                        start_dbn_single = dbn + 1;
                    }
                }
            }
            DEFS.ASSERT(false, "Count not allocate even a single bit!!");
            return -1;
        }

        public void Sync()
        {
            if (!initialized) return;

            lock (mfile)
            {
                for (int i = 0; i < 1024; i++)
                {
                    if (mbufs[i] != null)
                    {
                        if (mbufs[i].is_dirty)
                        {
                            mfile.Seek((long)i * (OPS.SIZE_OF_MAPBUFS), SeekOrigin.Begin);
                            mfile.Write(mbufs[i].data, 0, (OPS.SIZE_OF_MAPBUFS));
                            mbufs[i].is_dirty = false;
                        }

                        //Garbage collect.
                        if (mbufs[i].get_buf_age() > 120000)
                        {
                            mbufs[i] = null;
                        }
                    }
                }

                xfile.SetLength(8);
                xfile.Seek(0, SeekOrigin.Begin);
                byte[] buf = new byte[8];
                OPS.set_dbn(buf, 0, USED_BLK_COUNT);
                xfile.Write(buf, 0, 8);
                xfile.Flush();
            } //lock 
        }


        public void shut_down()
        {
            mTerminateThread = true;
            while (!mThreadTerminated)
            {
                Thread.Sleep(100);
            }
            Sync();
            mfile.Flush();
            mfile.Close();
            xfile.Flush();
            xfile.Close();
            initialized = false;
            mfile = null;
            xfile = null;
        }
    }

}
