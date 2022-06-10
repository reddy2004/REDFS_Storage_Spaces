using System;
using System.Collections.Generic;
using System.Text;

namespace REDFS_ClusterMode
{
    public class DBN
    {
        public static long INVALID = -1;
    };

    public enum BLK_TYPE
    {
        FSID_BLOCK,
        PUBLIC_INODE_FILE_L0,
        REGULAR_FILE_L2,
        REGULAR_FILE_L1,
        REGULAR_FILE_L0,
        BATCH_DBNS,
        IGNORE
    };

    public interface Red_Buffer
    {
        int get_level();
        BLK_TYPE get_blk_type();
        void data_to_buf(byte[] data);
        byte[] buf_to_data(); /* for reading into, and out*/
        long get_ondisk_dbn();
        void set_ondisk_dbn(long dbn);

        long get_start_fbn();
        void set_start_fbn(long fbn);
        void set_dirty(bool flag);

        bool is_dirty();

        /*
         * This can be used in refcount adjustment logic.
         */
        bool does_exist_ondisk();
        void set_ondisk_exist_flag(bool value);

        bool get_dbn_reassignment_flag();
        void set_dbn_reassignment_flag(bool v);

        bool get_touchrefcnt_needed();
        void set_touchrefcnt_needed(bool v);

        //For comparision
        public Boolean Equals(Red_Buffer other);

        public void touch();
    }


    public class REDFS_BUFFER_ENCAPSULATED
    {
        Red_Buffer mwb;
        public REDFS_BUFFER_ENCAPSULATED(Red_Buffer wb)
        {
            mwb = wb;
        }

        public long get_child_dbn(int idx)
        {
            switch (mwb.get_blk_type())
            {
                case BLK_TYPE.REGULAR_FILE_L1:
                    return ((RedBufL1)mwb).get_child_dbn(idx);
                case BLK_TYPE.REGULAR_FILE_L2:
                    return ((RedBufL2)mwb).get_child_dbn(idx);
            }
            return DBN.INVALID;
        }

        public long get_dbn()
        {
            switch (mwb.get_blk_type())
            {
                case BLK_TYPE.REGULAR_FILE_L1:
                    return ((RedBufL1)mwb).m_dbn;
                case BLK_TYPE.REGULAR_FILE_L2:
                    return ((RedBufL2)mwb).m_dbn;
            }
            return DBN.INVALID;
        }

        public void set_dbn(long dbn)
        {
            switch (mwb.get_blk_type())
            {
                case BLK_TYPE.REGULAR_FILE_L0:
                    ((RedBufL0)mwb).m_dbn = dbn;
                    break;
                case BLK_TYPE.REGULAR_FILE_L1:
                    ((RedBufL1)mwb).m_dbn = dbn;
                    break;
                case BLK_TYPE.REGULAR_FILE_L2:
                    ((RedBufL2)mwb).m_dbn = dbn;
                    break;
            }
            mwb.set_dirty(true);
        }

        public static Red_Buffer DeepCopy(Red_Buffer rbuf)
        {
            Red_Buffer rbreturn = null;

            switch (rbuf.get_blk_type())
            {
                case BLK_TYPE.REGULAR_FILE_L0:
                    rbreturn = new RedBufL0(rbuf.get_start_fbn());
                    break;
                case BLK_TYPE.REGULAR_FILE_L1:
                    rbreturn = new RedBufL1(rbuf.get_start_fbn());
                    break;
                case BLK_TYPE.REGULAR_FILE_L2:
                    rbreturn = new RedBufL2(rbuf.get_start_fbn());
                    break;
            }
            if (rbreturn != null)
            {
                byte[] source = rbuf.buf_to_data();
                byte[] dest = rbreturn.buf_to_data();
                for (int i = 0; i < source.Length; i++)
                {
                    dest[i] = source[i];
                }
                return rbreturn;
            }
            else
            {
                throw new SystemException("Unknown Red_buffer type for deep copy");
            }
        }
    }

    public class RedBufL0 : Red_Buffer
    {
        public byte[] data = new byte[OPS.FS_BLOCK_SIZE];
        public bool is_dirty;
        public long m_dbn;
        public long m_start_fbn;
        private bool m_exists_ondisk;

        private long creation_time;
        public int mTimeToLive = 6;
        public bool needdbnreassignment;
        public bool needtouchbuf = true;

        public int tracker = 0;
        public string trackermsg;


        public RedBufL0(long sf)
        {
            m_start_fbn = sf;
            creation_time = DateTime.Now.ToUniversalTime().Ticks;
            needdbnreassignment = true;
            needtouchbuf = true;
        }

        int Red_Buffer.get_level() { return 0; }
        BLK_TYPE Red_Buffer.get_blk_type() { return BLK_TYPE.REGULAR_FILE_L0; }
        void Red_Buffer.data_to_buf(byte[] d) {
            for (int i = 0; i < OPS.FS_BLOCK_SIZE; i++)
            {
                data[i] = d[i];
            }
        }
        byte[] Red_Buffer.buf_to_data() { return data; }
        long Red_Buffer.get_ondisk_dbn() { return m_dbn; }
        void Red_Buffer.set_ondisk_dbn(long dbn) {m_dbn = dbn;}

        long Red_Buffer.get_start_fbn() { return m_start_fbn; }
        void Red_Buffer.set_start_fbn(long fbn) { m_start_fbn = fbn; }
        void Red_Buffer.set_dirty(bool flag) { is_dirty = flag; }

        bool Red_Buffer.is_dirty() { return is_dirty; }

        bool Red_Buffer.does_exist_ondisk() { return m_exists_ondisk; }
        void Red_Buffer.set_ondisk_exist_flag(bool value) { m_exists_ondisk = value; }
        bool Red_Buffer.get_dbn_reassignment_flag() { return needdbnreassignment; }
        void Red_Buffer.set_dbn_reassignment_flag(bool v) { needdbnreassignment = v; }
        bool Red_Buffer.get_touchrefcnt_needed() { return needtouchbuf; }
        void Red_Buffer.set_touchrefcnt_needed(bool v) { needtouchbuf = v; }
        void Red_Buffer.touch() { creation_time = DateTime.Now.ToUniversalTime().Ticks; }
        public bool isTimetoClear()
        {
            long curr = DateTime.Now.ToUniversalTime().Ticks;
            int seconds = (int)((curr - creation_time) / 10000000);

            if (seconds > mTimeToLive) return true;
            else return false;
        }
        public int myidx_in_myparent() { return (int)(m_start_fbn % OPS.FS_SPAN_OUT); }

        public void reinitbuf(long sf)
        {
            m_start_fbn = sf;
            creation_time = DateTime.Now.ToUniversalTime().Ticks;
            needdbnreassignment = true;
            needtouchbuf = true;
            is_dirty = false;
            m_dbn = 0;
            m_exists_ondisk = false;
            mTimeToLive = 1;
            Array.Clear(data, 0, OPS.FSID_BLOCK_SIZE);
        }

        public Boolean Equals(Red_Buffer other1)
        {
            if (other1.GetType() != this.GetType())
            {
                return false;
            }
            RedBufL0 other = (RedBufL0)other1;

            if (this.m_dbn != other.m_dbn || this.m_start_fbn != other.m_start_fbn)
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
    }

    public class RedBufL1 : Red_Buffer
    {
        public byte[] data = new byte[OPS.FS_BLOCK_SIZE];
        public bool is_dirty;
        public long m_dbn;
        public long m_start_fbn;
        private bool m_exists_ondisk;

        private long creation_time;
        private int mTimeToLive = 6;
        public bool needdbnreassignment;
        public bool needtouchbuf = true;

        public RedBufL1(long sf)
        {
            m_start_fbn = sf;
            for (int i = 0; i < OPS.FS_SPAN_OUT; i++)
            {
                set_child_dbn(i, DBN.INVALID);
            }
            creation_time = DateTime.Now.ToUniversalTime().Ticks;
            needdbnreassignment = true;
            needtouchbuf = true;
        }
        int Red_Buffer.get_level() { return 1; }
        BLK_TYPE Red_Buffer.get_blk_type() { return BLK_TYPE.REGULAR_FILE_L1; }
        void Red_Buffer.data_to_buf(byte[] d)
        {
            for (int i = 0; i < OPS.FS_BLOCK_SIZE; i++)
            {
                data[i] = d[i];
            }
        }
        byte[] Red_Buffer.buf_to_data() { return data; }
        long Red_Buffer.get_ondisk_dbn() { return m_dbn; }
        void Red_Buffer.set_ondisk_dbn(long dbn) { m_dbn = dbn; }
        long Red_Buffer.get_start_fbn() { return m_start_fbn; }
        void Red_Buffer.set_start_fbn(long fbn) { m_start_fbn = fbn; }
        void Red_Buffer.set_dirty(bool flag) { is_dirty = flag; }

        bool Red_Buffer.is_dirty() { return is_dirty; }
        bool Red_Buffer.does_exist_ondisk() { return m_exists_ondisk; }
        void Red_Buffer.set_ondisk_exist_flag(bool value) { m_exists_ondisk = value; }
        bool Red_Buffer.get_dbn_reassignment_flag() { return needdbnreassignment; }
        void Red_Buffer.set_dbn_reassignment_flag(bool v) { needdbnreassignment = v; }
        bool Red_Buffer.get_touchrefcnt_needed() { return needtouchbuf; }
        void Red_Buffer.set_touchrefcnt_needed(bool v) { needtouchbuf = v; }

        void Red_Buffer.touch() { creation_time = DateTime.Now.ToUniversalTime().Ticks; }
        public bool isTimetoClear()
        {
            long curr = DateTime.Now.ToUniversalTime().Ticks;
            int seconds = (int)((curr - creation_time) / 10000000);

            if (seconds > mTimeToLive) return true;
            else return false;
        }

        public int myidx_in_myparent() { return (int)((m_start_fbn % (OPS.FS_SPAN_OUT * OPS.FS_SPAN_OUT)) / OPS.FS_SPAN_OUT); }

        public long get_child_dbn(int idx)
        {
            lock (data)
            {
                return OPS.get_dbn(data, idx);
            }
        }

        public void set_child_dbn(int idx, long dbn)
        {
            lock (data)
            {
                is_dirty = true;
                OPS.set_dbn(data, idx, dbn);
            }
        }

        public Boolean Equals(Red_Buffer other1)
        {
            if (other1.GetType() != this.GetType())
            {
                return false;
            }
            RedBufL1 other = (RedBufL1)other1;

            if (this.m_dbn != other.m_dbn || this.m_start_fbn != other.m_start_fbn)
            {
                return false;
            }
            for (int i = 0; i < data.Length; i++)
            {
                if (this.data[i] != other.data[i])
                {
                    return false;
                }
            }
            return true;
        }
    }

    /*
     * Regular file L2 indirect buffer.
     */
    public class RedBufL2 : Red_Buffer
    {
        public byte[] data = new byte[OPS.FS_BLOCK_SIZE];
        public bool is_dirty;
        public long m_dbn;
        public long m_start_fbn;
        private bool m_exists_ondisk;

        private long creation_time;
        private int mTimeToLive = 6;
        public bool needdbnreassignment;
        public bool needtouchbuf = true;

        //public Red_Buffer[] m_chdptr = new Red_Buffer[OPS.FS_SPAN_OUT];

        public RedBufL2(long sf)
        {
            m_start_fbn = sf;
            for (int i = 0; i < OPS.FS_SPAN_OUT; i++)
            {
                set_child_dbn(i, DBN.INVALID);
            }
            creation_time = DateTime.Now.ToUniversalTime().Ticks;
            needdbnreassignment = true;
        }
        int Red_Buffer.get_level() { return 2; }
        BLK_TYPE Red_Buffer.get_blk_type() { return BLK_TYPE.REGULAR_FILE_L2; }
        void Red_Buffer.data_to_buf(byte[] d)
        {
            for (int i = 0; i < OPS.FS_BLOCK_SIZE; i++)
            {
                data[i] = d[i];
            }
        }
        byte[] Red_Buffer.buf_to_data() { return data; }
        long Red_Buffer.get_ondisk_dbn() { return m_dbn; }

        void Red_Buffer.set_ondisk_dbn(long dbn) { m_dbn = dbn; }
        long Red_Buffer.get_start_fbn() { return m_start_fbn; }
        void Red_Buffer.set_start_fbn(long fbn) { m_start_fbn = fbn; }
        void Red_Buffer.set_dirty(bool flag) { is_dirty = flag; }

        bool Red_Buffer.is_dirty() { return is_dirty; }
        bool Red_Buffer.does_exist_ondisk() { return m_exists_ondisk; }
        void Red_Buffer.set_ondisk_exist_flag(bool value) { m_exists_ondisk = value; }
        bool Red_Buffer.get_dbn_reassignment_flag() { return needdbnreassignment; }
        void Red_Buffer.set_dbn_reassignment_flag(bool v) { needdbnreassignment = v; }
        bool Red_Buffer.get_touchrefcnt_needed() { return needtouchbuf; }
        void Red_Buffer.set_touchrefcnt_needed(bool v) { needtouchbuf = v; }

        void Red_Buffer.touch() { creation_time = DateTime.Now.ToUniversalTime().Ticks; }
        public bool isTimetoClear()
        {
            long curr = DateTime.Now.ToUniversalTime().Ticks;
            int seconds = (int)((curr - creation_time) / 10000000);

            if (seconds > mTimeToLive) return true;
            else return false;
        }

        public int myidx_in_myparent() { return (int)(m_start_fbn / (OPS.FS_SPAN_OUT * OPS.FS_SPAN_OUT)); }

        public long get_child_dbn(int idx)
        {
            lock (data)
            {
                return OPS.get_dbn(data, idx);
            }
        }
        public void set_child_dbn(int idx, long dbn)
        {
            lock (data)
            {
                is_dirty = true;
                OPS.set_dbn(data, idx, dbn);
            }
        }

        public Boolean Equals(Red_Buffer other1)
        {
            if (other1.GetType() != this.GetType())
            {
                return false;
            }
            RedBufL2 other = (RedBufL2)other1;

            if (this.m_dbn != other.m_dbn || this.m_start_fbn != other.m_start_fbn)
            {
                return false;
            }
            for (int i = 0; i < data.Length; i++)
            {
                if (this.data[i] != other.data[i])
                {
                    return false;
                }
            }
            return true;
        }
    }
}
