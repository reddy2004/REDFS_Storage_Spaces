using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using System.Collections.Concurrent;
using System.IO;
using System.Security.Cryptography;

namespace REDFS_ClusterMode
{

    public class wrcomparator : IComparer
    {
        int IComparer.Compare(object obj1, object obj2)
        {
            WRBuf w1 = (WRBuf)obj1;
            WRBuf w2 = (WRBuf)obj2;

            if (w1.get_buf_age() > w2.get_buf_age())
                return -1;
            else if (w1.get_buf_age() > w2.get_buf_age())
                return 1;
            return 0;
        }
    }

    public enum REFCNT_OP
    {
        UNDEFINED,
        INCREMENT_REFCOUNT_ALLOC,
        INCREMENT_REFCOUNT,
        DECREMENT_REFCOUNT_ONDEALLOC,
        DECREMENT_REFCOUNT,
        TOUCH_REFCOUNT,
        GET_REFANDCHD_INFO,
        DO_SYNC,
        TAKE_DISK_SNAPSHOT,
        UNDO_DISK_SNAPSHOT,
        BATCH_INCREMENT_REFCOUNT_ALLOC, /* Batch update of 1024 dbns at one shot, it is in 8KB block in tfile. 1024 dbns  => 8MB contiguious refcnt update */
        SHUT_DOWN
    }

    public class REFDEF
    {
        //8 byte entry for each 4k block. So a 4k block of refcounts have 512 refcount info for 512 data blocks.
        public static int dbn_to_rbn(long dbn) 
        { 
            return (int)(dbn / OPS.REF_INDEXES_IN_BLOCK); 
        }

        //Same as above, but we need offset in the rbn block for the particular dbn
        public static int dbn_to_ulist_idx(long dbn) 
        { 
            return (int)(dbn % OPS.REF_INDEXES_IN_BLOCK); 
        }

        public static string get_string_rep(UpdateReqI cu)
        {
            return "dbn, blk, op, value, tfbn = " + cu.dbn + "," + cu.blktype +
                "," + cu.optype + "," + cu.value + "," + cu.tfbn;
        }
    }

    public class OPS
    {
        /* Do not change these. They are not configurable values */
        public static int FS_BLOCK_SIZE = 8192;
        public static int REF_BLOCK_SIZE = 4096;

        public static int FS_BLOCK_SIZE_IN_KB = 8;
        public static int FS_PTR_SIZE = 8;
        public static int FS_SPAN_OUT = FS_BLOCK_SIZE / FS_PTR_SIZE;

        public static int NUM_PTRS_IN_WIP = 16;
        public static int WIP_SIZE = 256;
        public static int WIP_USED_SIZE = 160;
        public static int NUM_WIPS_IN_BLOCK = FS_BLOCK_SIZE / WIP_SIZE;

        public static int REF_INDEX_SIZE = 8;
        public static int REF_INDEXES_IN_BLOCK = REF_BLOCK_SIZE / REF_INDEX_SIZE;

        public static int NUM_DBNS_IN_1GB = 1024 * 1024 / FS_BLOCK_SIZE_IN_KB;

        public static int SPAN_ENTRY_SIZE_BYTES = 32;
        public static int NUM_SPAN_MAX_ALLOWED = 32 * 1024 * 4; /* One per GB, in 128TB filesystem, we can make this higher */

        public static int SIZE_OF_MAPBUFS = 256 * 1024;
        public static long NUM_MAPBUFS_FOR_CONTAINER = 8192;
        public static long MAPFILE_SIZE_IN_BYTES = SIZE_OF_MAPBUFS * NUM_MAPBUFS_FOR_CONTAINER;
        public static long MIN_ALLOCATABLE_DBN = 1024;

        public static int FSID_BLOCK_SIZE = 1024;

        //Lets define the sizes of the iMapWip and inodeWip which is part of every fsid. Now the max file size supported by redfs is
        //1024 8K L0 blocks per L1
        //1024 * 1024 L0 blocks per L2
        //1024 * 1024 * 16 L2 blocks allowed by design, hence 16777216 8K blocks. which implies 128GB
        public static int NUM_WIPS_PER_FSID = (int)(((long)1024 * 1024 * 1024 * 128) / WIP_SIZE);
        public static int NUM_BLOCKS_IN_IMAPWIP = NUM_WIPS_PER_FSID / (FS_BLOCK_SIZE * 8); //1bit per wip, total 64MB

        private static byte[] XORBUF = new byte[8192];
        private static byte[] buff_t8 = new byte[8];

        public static string HashToString(byte[] hash)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < hash.Length; i++)
            {
                sb.Append(hash[i].ToString("X2"));
            }
            return sb.ToString();
        }
        public static void set_int(byte[] data, int byteoffset, int value)
        {
            byte[] val = BitConverter.GetBytes(value);
            data[byteoffset] = val[0];
            data[byteoffset + 1] = val[1];
            data[byteoffset + 2] = val[2];
            data[byteoffset + 3] = val[3];
        }

        public static int get_int(byte[] data, int byteoffset)
        {
            return BitConverter.ToInt32(data, byteoffset);
        }

        public static long get_long(byte[] data, int offset)
        {
            long returnvalue = -1;

            lock (buff_t8)
            {
                Array.Copy(data, offset, buff_t8, 0, 6);
                returnvalue = BitConverter.ToInt64(buff_t8, 0);
            }
            return returnvalue;
        }

        public static void set_long(byte[] data, int offset, long dbn)
        {
            byte[] val = BitConverter.GetBytes(dbn);
            data[offset] = val[0];
            data[offset + 1] = val[1];
            data[offset + 2] = val[2];
            data[offset + 3] = val[3];
            data[offset + 4] = val[4];
            data[offset + 5] = val[5];
            data[offset + 6] = val[6];
            data[offset + 7] = val[7];
        }

        public static long get_dbn(byte[] data, int idx)
        {
            //Lets use only 6 bytes and reserve 2 bytes for later.
            int offset = idx * FS_PTR_SIZE;
            long returnvalue = -1;

            lock (buff_t8)
            {
                Array.Copy(data, offset, buff_t8, 0, 8);
                returnvalue = BitConverter.ToInt64(buff_t8, 0);
            }
            return returnvalue;
        }

        public static void set_dbn(byte[] data, int idx, long dbn)
        {
            //Lets use 6 bytes for storing the 64bit dbn. Since dbns are always positive
            //we can afford to ignore the last two bytes.

            byte[] val = BitConverter.GetBytes(dbn);
            int offset = idx * FS_PTR_SIZE;
            data[offset] = val[0];
            data[offset + 1] = val[1];
            data[offset + 2] = val[2];
            data[offset + 3] = val[3];
            data[offset + 4] = val[4];
            data[offset + 5] = val[5];
            data[offset + 6] = val[6];
            data[offset + 7] = val[7];
        }

        public static void GenerateXORBuf(string key)
        {
            MD5 md5 = System.Security.Cryptography.MD5.Create();
            byte[] inputBytes = System.Text.Encoding.ASCII.GetBytes(key);
            byte[] hash = md5.ComputeHash(inputBytes);


            byte smallbyte = 0;

            for (int i = 0; i < hash.Length; i++)
            {
                if (smallbyte > hash[i]) smallbyte = hash[i];
            }
            for (int i = 0; i < hash.Length; i++)
            {
                hash[i] -= smallbyte;
            }
            for (int i = 0; i < 512; i++)
            {
                for (int j = 0; j < hash.Length; j++)
                    XORBUF[i * 16 + j] = hash[j];
            }
        }

        public static void Decrypt_Read_WRBuf(byte[] readdata, byte[] incore)
        {
            for (int i = 0; i < FS_BLOCK_SIZE; i++)
            {
                incore[i] = (byte)(readdata[i] ^ XORBUF[i]);
                //incore[i] = readdata[i];
            }
        }

        public static void Encrypt_Data_ForWrite(byte[] writedata, byte[] incore)
        {
            for (int i = 0; i < FS_BLOCK_SIZE; i++)
            {
                writedata[i] = (byte)(incore[i] ^ XORBUF[i]);
                //writedata[i] = incore[i];
            }
        }

        public static bool snapshot_getbit(int offset, byte[] data)
        {

            return false;
        }
        public static void snapshot_setbit(int offset, byte[] data, bool value)
        {


        }

        public static int FSIZETOILEVEL(long size)
        {
            if (size <= NUM_PTRS_IN_WIP * FS_BLOCK_SIZE) return 0;
            else if (size <= (FS_SPAN_OUT * FS_BLOCK_SIZE) * NUM_PTRS_IN_WIP) return 1;
            else return 2;
        }

        public static long SomeFBNToStartFBN(int level, long somefbn)
        {
            if (level == 0)
            {
                return somefbn;
            }
            else if (level == 1)
            {
                return (long)((somefbn / FS_SPAN_OUT) * FS_SPAN_OUT);
            }
            else if (level == 2)
            {
                return (long)((somefbn / (FS_SPAN_OUT * FS_SPAN_OUT)) * (FS_SPAN_OUT * FS_SPAN_OUT));
            }
            else
            {
                DEFS.ASSERT(false, "Incorrect level to resolve in SomeFBNToStartFBN");
                return -1;
            }
        }

        public static long PIDXToStartFBN(int level, int idx)
        {
            if (level == 0)
            {
                return idx;
            }
            else if (level == 1)
            {
                return (long) idx * FS_SPAN_OUT;
            }
            else if (level == 2)
            {
                return (long) idx * (FS_SPAN_OUT * FS_SPAN_OUT);
            }
            else
            {
                DEFS.ASSERT(false, "Incorrect level passed to PIDXToStartFBN:" + level + " " + idx);
                return -1;
            }
        }

        public static long OffsetToFBN(long offset)
        {
            return (offset / FS_BLOCK_SIZE);
        }

        public static long OffsetToStartFBN(int level, long offset)
        {
            return SomeFBNToStartFBN(level, OffsetToFBN(offset));
        }

        public static string getDataInStringRep(long size)
        {
            double value = 0;
            string type = "";

            if (size < 1024) { value = (double)(size); type = " B"; }
            else if (size < 1024 * 1024) { value = (double)size / 1024; type = " KB"; }
            else if (size < (1024 * 1024 * 1024)) { value = (double)size / (1024 * 1024); type = " MB"; }
            else if (size < ((long)1024 * 1024 * 1024 * 1024)) { value = (double)size / (1024 * 1024 * 1024); type = " GB"; }
            else { value = (double)size / ((long)1024 * 1024 * 1024 * 1024); type = " TB"; }

            return String.Format("{0:0.00}", value) + type;
        }

        public static int get_first_free_bit(byte b)
        {
            int bint = b;
            for (int i = 0; i < 8; i++)
            {
                if (((bint >> i) & 0x0001) == 0) return i;
            }
            return -1;
        }
        public static byte set_free_bit(byte b, int k)
        {
            int bint = (1 << k);
            int bnew = (byte)(bint & 0x00FF);
            bnew |= b;
            DEFS.ASSERT(b != (byte)bnew, "Some bit must have been set");
            return (byte)bnew;
        }

        public static int NUML0(long size)
        {
            return (int)((size % FS_BLOCK_SIZE == 0) ? (size / FS_BLOCK_SIZE) : (size / FS_BLOCK_SIZE + 1));
        }

        public static int NUML1(long size)
        {
            if (size <= ((long)FS_BLOCK_SIZE * NUM_PTRS_IN_WIP)) { return 0; }
            int numl0 = NUML0(size);
            return (int)((numl0 % FS_SPAN_OUT == 0) ? (numl0 / FS_SPAN_OUT) : (numl0 / FS_SPAN_OUT + 1));
        }

        public static int NUML2(long size)
        {
            if (size <= ((long)FS_SPAN_OUT * FS_BLOCK_SIZE * NUM_PTRS_IN_WIP)) { return 0; }
            int numl1 = NUML1(size);
            return (int)((numl1 % FS_SPAN_OUT == 0) ? (numl1 / FS_SPAN_OUT) : (numl1 / FS_SPAN_OUT + 1));
        }

        public static long NEXT8KBOUNDARY(long currsize, long newsize)
        {
            return ((currsize % FS_BLOCK_SIZE) == 0) ? ((newsize < (currsize + FS_BLOCK_SIZE)) ?
                    newsize : (currsize + FS_BLOCK_SIZE)) : ((long)NUML0(currsize) * FS_BLOCK_SIZE);
        }

        /*
         * See if we can do the write directly without inserting L0's and get over with it
         * quickly. returns the number of dbns required if possible, or -1.
         */
        public static int myidx_in_myparent(int level, long somefbn)
        {
            long m_start_fbn = SomeFBNToStartFBN(level, somefbn);

            switch (level)
            {
                case 0:
                    return (int)(m_start_fbn % FS_SPAN_OUT);
                case 1:
                    return (int)((m_start_fbn % (FS_SPAN_OUT * FS_SPAN_OUT)) / FS_SPAN_OUT);
                case 2:
                    return (int)(m_start_fbn / (FS_SPAN_OUT * FS_SPAN_OUT));
            }
            return 0;
        }
    }

    /*
     * 8 byte format:-
     * 4 bytes -> refcount of the block.
     * 2 bytes -> refcoudn that should be propogated downward
     * 1 byte -> 01 bit -dedupe overwritten flag.
     *           1-
     * 1 bytes  -> Reserved for future use.
     * 
     * Each entry is OPS.REF_INDEX_SIZE bytes, There are OPS.REF_INDEXES_IN_BLOCK in each block
     * of size OPS.FS_BLOCK_SIZE. Always assert that REF_INDEX_SIZE==8
     */
    public class WRBuf
    {
        private long m_creation_time;
        public long start_dbn;

        public byte[] data = new byte[OPS.REF_BLOCK_SIZE];
        public bool is_dirty;
        public int m_rbn;

        public int m_unique_id;

        public WRBuf(int r)
        {
            DEFS.ASSERT(OPS.REF_INDEX_SIZE == 8, "Incorrect ref_cnt entry size defined, We support only 8 byte entry");
            m_rbn = r;
            start_dbn = (long)r * OPS.REF_INDEXES_IN_BLOCK;
            m_creation_time = DateTime.Now.ToUniversalTime().Ticks;
            Array.Clear(data, 0, data.Length);
        }

        public Boolean Equals(WRBuf other)
        {
            if (this.start_dbn != other.start_dbn ||
                this.m_rbn != other.m_rbn)
            {
                return false;
            }

            for (int i=0;i<data.Length;i++)
            {
                if (this.data[i] != other.data[i])
                {
                    return false;
                }
            }
            return true;
        }

        public void reinit(int r, int unique_id)
        {
            DEFS.ASSERT(OPS.REF_INDEX_SIZE == 8, "Incorrect ref_cnt entry size defined, We support only 8 byte entry");
            m_rbn = r;
            start_dbn = r * OPS.REF_INDEXES_IN_BLOCK;
            m_creation_time = DateTime.Now.ToUniversalTime().Ticks;
            Array.Clear(data, 0, data.Length);
            m_unique_id = unique_id;


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

        public void set_refcount(long dbn, int value)
        {
            lock (data)
            {
                touch_buf();
                int offset = (int) ((dbn % OPS.REF_INDEXES_IN_BLOCK) * OPS.REF_INDEX_SIZE + 0);
                DEFS.ASSERT(dbn >= start_dbn && dbn < start_dbn + OPS.REF_INDEXES_IN_BLOCK,
                    "Wrong dbn range in VBuf " + dbn + "," + start_dbn +
                    "," + m_rbn);

                byte[] val = BitConverter.GetBytes(value);
                data[offset] = val[0];
                data[offset + 1] = val[1];
                data[offset + 2] = val[2];
                data[offset + 3] = val[3];
                is_dirty = true;
            }
        }

        public void set_childcount(long dbn, int value)
        {
            lock (data)
            {
                touch_buf();
                int offset = (int)((dbn % OPS.REF_INDEXES_IN_BLOCK) * OPS.REF_INDEX_SIZE + 4);
                DEFS.ASSERT(dbn >= start_dbn && dbn < start_dbn + OPS.REF_INDEXES_IN_BLOCK,
                    "Wrong dbn range in VBuf 3" + dbn + "," +
                    start_dbn + "," + m_rbn);
                
                byte[] val = BitConverter.GetBytes((Int16)value);
                data[offset] = val[0];
                data[offset + 1] = val[1];
                is_dirty = true;
            }
        }

        public int get_refcount(long dbn)
        {
            lock (data)
            {
                touch_buf();

                DEFS.ASSERT(dbn >= start_dbn && dbn < start_dbn + OPS.REF_INDEXES_IN_BLOCK,
                    "Wrong dbn range in VBuf2 " + dbn + "," + start_dbn +
                    "," + m_rbn);

                int offset = (int)((int)(dbn % OPS.REF_INDEXES_IN_BLOCK) * OPS.REF_INDEX_SIZE + 0);
                return BitConverter.ToInt32(data, offset);
            }
        }
        public int get_childcount(long dbn)
        {
            lock (data)
            {
                touch_buf();
                DEFS.ASSERT(dbn >= start_dbn && dbn < (start_dbn + OPS.REF_INDEXES_IN_BLOCK),
                    "Wrong dbn range in VBuf3 " + dbn + "," + start_dbn +
                    "," + m_rbn);

                int offset = (int)((int)(dbn % OPS.REF_INDEXES_IN_BLOCK) * OPS.REF_INDEX_SIZE + 4);
                return BitConverter.ToInt16(data, offset);
            }
        }

        public bool get_dedupe_overwritten_flag(long dbn)
        {
            /* seems to be overwriting child refcnt
            lock (data)
            {
                int offset = (int)((dbn % OPS.REF_INDEXES_IN_BLOCK) * OPS.REF_INDEX_SIZE + 6);
                byte bvale = data[offset];
                if ((bvale & 0x01) != 0) return true;
                return false;
            }
            */
            return false;
        }

        public void set_dedupe_overwritten_flag(long dbn, bool flag)
        {
            /*seems to be overwriting child refcnt
            lock (data)
            {
                int offset = (int)((dbn % OPS.REF_INDEXES_IN_BLOCK) * OPS.REF_INDEX_SIZE + 6);
                data[offset] = (flag) ? ((byte)(data[offset] | 0x01)) : (byte)(data[offset] & 0xFE);
                is_dirty = true;
            }
            */
        }
    }

    public class UIUpdateMessage
    {
        public bool isDirectory;
        public String action;
        public String path1;
        public String path2;
    }

    public class UpdateReqI
    {
        public REFCNT_OP optype;
        public BLK_TYPE blktype;
        public int fsid;
        public long dbn;
        public Int16 value; /* -1/+1 always */
        public int tfbn; /* the fbn of the transaction file */
        public bool processed;
        public bool deleted_sucessfully;
        public int inodeNumber; //can be 0 or something else, if its -1, dont count it.
        public string who;
    }

    public class WRContainer
    {
        /* Allocated on demand */
        public WRBuf incoretbuf;

        public Boolean Equals(WRContainer other)
        {
            if (this.incoretbuf == null ||
                other.incoretbuf == null)
            {
                return false;
            }
            return this.incoretbuf.Equals(other.incoretbuf);
        }
    }

    public class GLOBALQ
    {
        public static BlockingCollection<UIUpdateMessage> m_ux_update = new BlockingCollection<UIUpdateMessage>(1024);
        public static BlockingCollection<UpdateReqI> m_reqi_queue = new BlockingCollection<UpdateReqI>(4194304);
        public static BlockingCollection<RedFS_Inode> m_wipdelete_queue = new BlockingCollection<RedFS_Inode>(524288);

        public static List<long> m_deletelog2 = new List<long>();
        public static List<long> m_deletelog_spanmap = new List<long>();

        public static WRContainer[] WRObj = new WRContainer[4194304 * 16]; //Each WRobj has 512 refs for 8k blocks, total per wrobj is 4MB. Hence 256TB

        /*
         * Below entries are for snapshot before starting dedupe so as to
         * protect dbns from being overwritten.
         */
        public static bool disk_snapshot_mode_enabled = false;
        public static bool[] disk_snapshot_map = new bool[4194304 * 16];
        public static REFCNT_OP disk_snapshot_optype;
        public static FileStream snapshotfile = null;
    }
}
