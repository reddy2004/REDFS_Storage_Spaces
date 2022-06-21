using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace REDFS_ClusterMode
{
    public static class SEGMENT_TYPES
    {
        public static int Default = 0;
        public static int Mirrored = 1;
        public static int RAID5Data = 2;
        public static int RAID5Parity = 4;

        public static string[] TypeString = { "Default", "Mirrored", "Raid5Data", "<invalid>", "Raid5Parity" };
    }

    public enum SEGMENT_TYPESw : long
    {
        Default     = 0,
        Mirrored    = 1,
        RAID5Data   = 2,
        RAID5Parity = 4     /* This is technically not RAID 5 as we have a dedicated parity segment, to be fixed */
    }

    public enum SPAN_TYPE
    {
        DEFAULT,
        MIRRORED,   /* Only 2X mirroring for now*/
        RAID5 /* Only 4D +1P supported */
    }

    public class ReadPlanElement
    {
        public long dbn;
        public int  chunkId;
        public long readOffset;
    }

    /*
     * 1 entry for default, 2 for mirrored, and 4 + 1 for RAID5
     */ 
    public class WritePlanElement
    {
        public long[]   dbns;
        public int[]    dataChunkIds;
        public int[]    parityChunkIds;
        public long[]   writeOffsets;
    }

    public class RAWSegment : IEquatable<RAWSegment>
    {
        public int  chunkID;        /* Which file has data of this segment */
        public int  chunkOffset;    /* In terms of GB, where in the file? */
        public int segmentTypeInt = 0;

        public RAWSegment()
        {

        }

        public RAWSegment(int type, int cid, int coffset)
        {
            segmentTypeInt = type;
            chunkID = cid;
            chunkOffset = coffset;
        }


        //What the Fuck, this assignment is cpu intensive,wtf!
        //segmentType = type;
        //When reading from disk. Read only 4 bytes and nothing more.
        //For this constructor we are passing integer values instead of enum for segtype.
        public RAWSegment(int type, byte[] buffer, int offset)
        {
            segmentTypeInt = type;
            int coffset = 0;
            chunkID = (int)buffer[offset];

            coffset |= buffer[1 + offset];
            coffset |= (buffer[2 + offset] << 8);
            coffset |= (buffer[3 + offset] << 16);

            chunkOffset = coffset;
        }

        public void GetRAWBytes(byte[] buffer, int offset)
        {
            buffer[offset] = (byte)chunkID;

            buffer[offset + 1] = (byte)(chunkOffset & 0x000000FF);
            buffer[offset + 2] = (byte)((chunkOffset & 0x0000FF00) >> 8);
            buffer[offset + 3] = (byte)((chunkOffset & 0x00FF0000) >> 16);
        }

        public string GetStringRepresentation()
        {
            return "{" + chunkID + "," + chunkOffset + "," + SEGMENT_TYPES.TypeString[segmentTypeInt] + "}";
        }

        public bool Equals(RAWSegment other)
        {
            return (this.chunkID == other.chunkID && this.chunkOffset == other.chunkOffset && this.segmentTypeInt == other.segmentTypeInt);
        }
    }

    public class ChunkInfo
    {
        public int id;
        public string path;
        public string speedClass;
        public int size;
        public int freeSpace;
        public string status;

        public int allowedSegmentTypes;

        //For the flags in UX.
        public Boolean canDeleteChunk = false;
        public Boolean isBeingPreparedForRemoval = false;
        public Boolean isReadOnly = false;
        public Boolean chunkIsAccessible = false;    /*Maybe not accesible if its on a removal drive and that drive is not plugged in*/
        public Boolean creationInProgress = false; /* Used by create chunk and move chunk */
    }

    public class REDFSChunk
    {
        public string   chunkFilePath;
        public int      chunkID;
        public Boolean  isAvailable = false;
        public long     numSegmentsInChunk; /* File size in GB */

        /* 
         * This should be populated from 'spanFile.redfs' file to avoid storing duplicate information everywhere 
         * This piece of information is sparsely updated and shouldnt be an issue.
         */
        public Boolean[]    isSegmentInUse; /* One bool for each segment, where each segment is 1GB */

        /* 
         * Allow type of segments. Each Chunk can have segements which are in raid or mirrored mode as well,
         * This information is static and is marked at the time of creation of the chunk, it can be stored in the 
         * json file itself in the container folder. file "chunk.files"
         */
        public int allowed_types;

        public REDFSChunk(int id, int types, string path, long max_file_size)
        {
            chunkID = id;
            chunkFilePath = path;
            numSegmentsInChunk = max_file_size / (1024 * 1024 * 1024);
            allowed_types = types;
        }

        public Boolean LoadChunk()
        {
            isAvailable = false;
            return false;
        }
    }

    /*
     * Each segment is a multiple of 1GB,
     * SEGMENT_TYPE.DEFAULT is 1GB segments.
     * SEGMENT_TYPE.MIRRORED is 2x1GB segments, where each is mirror of the other.
     * SEGMENT_TYPE.RAID5 is 5 segments, 4 data and 1 parity, so reads are even multiples. ex. 16k read
     * 
     * Information required for each DBNSegmentSpan.
     * Ondisk, we need one entry for 1GB span (i.e default and mirrored), 1 entry for 4GB (which is 4D1P type).
     * 
     * 1 byte: Type of span.
     * 1 byte: Flags if RAID5, then what position is this span, 0,1,2 or 3?, adjust the start fbn accordingly.
     * 20 bytes
     *      4 byte - chunkid & offset for 1 D
     *      4 byte - chunkid & offset for 2 D
     *      4 byte - chunkid & offset for 3 D
     *      4 byte - chunkid & offset for 4 D
     *      4 byte - chunkid & offset for 1 P
     *      **1 byte for chunk id and 3 bytes for 1GB offset in that chunk.
     * 4 byte: free blocks (user blocks and not segement blocks, i.e in dbn space free blocks)
     * 4 byte: Unique hash of span.
     * 2 byte: reserved.
     * 
     * This entire 32byte struct could be duplicated. In case of raid5, we could have the same struct contiguiously for 4 spans in dbn space.
     */
    public class DBNSegmentSpan : IEquatable<DBNSegmentSpan>
    {
        public SPAN_TYPE    type;
        public int          position;
        public long         start_dbn;
        public int          num_segments;   /*Include both data and parity*/

        public RAWSegment[]    dataSegment; /* Each of RAWSegment & parity Segment must have different chunkIds */
        public RAWSegment[]    paritySegment;

        public Boolean  isSegmentValid = false;
        public string   invalidReason = "";
        public bool     isDirty = false;
        public int      totalFreeBlocks = 0;
        public UInt32   spanUniqueHash;

        public DBNSegmentSpan()
        { 
        
        }

        //Parse data directly from disk. Pos=>position of this span if its raid5. For default and mirror its zero
        public DBNSegmentSpan(long sdbn, byte[] buffer)
        {
            //For the time being, if its RAID5 then we have to adjust the start_dbn
            start_dbn = sdbn;

            DEFS.ASSERT(buffer.Length == 32, "Incorrect buffer length");
            
            type = (SPAN_TYPE)buffer[0];
            position = (int)buffer[1];

            spanUniqueHash |= buffer[26];
            spanUniqueHash |= (UInt32)(buffer[27] << 8);
            spanUniqueHash |= (UInt32)(buffer[28] << 16);
            spanUniqueHash |= (UInt32)(buffer[29] << 24);

            if (spanUniqueHash == 0)
            {
                isSegmentValid = false;
                return;
            }

            if (SPAN_TYPE.DEFAULT == type)
            {
                dataSegment = new RAWSegment[1];
                dataSegment[0] = new RAWSegment(SEGMENT_TYPES.Default, buffer, 2);
                num_segments = 1;
                isSegmentValid = true;
            }
            else if (SPAN_TYPE.MIRRORED == type)
            {
                dataSegment = new RAWSegment[2];
                dataSegment[0] = new RAWSegment(SEGMENT_TYPES.Mirrored, buffer, 2);
                dataSegment[1] = new RAWSegment(SEGMENT_TYPES.Mirrored, buffer, 6);
                num_segments = 2;
                isSegmentValid = true;
            }
            else if (SPAN_TYPE.RAID5 == type)
            {
                dataSegment = new RAWSegment[4];
                paritySegment = new RAWSegment[1];
                dataSegment[0] = new RAWSegment(SEGMENT_TYPES.RAID5Data, buffer, 2);
                dataSegment[1] = new RAWSegment(SEGMENT_TYPES.RAID5Data, buffer, 6);
                dataSegment[2] = new RAWSegment(SEGMENT_TYPES.RAID5Data, buffer, 10);
                dataSegment[3] = new RAWSegment(SEGMENT_TYPES.RAID5Data, buffer, 14);
                paritySegment[0] = new RAWSegment(SEGMENT_TYPES.RAID5Parity, buffer, 18);
                num_segments = 5;
                isSegmentValid = true;
            }
            else
            {
                num_segments = 0;
                isSegmentValid = false;
                DEFS.ASSERT(false, "Incorrect span type dettected!");
            }
            
            totalFreeBlocks |= buffer[22];
            totalFreeBlocks |= (buffer[23] << 8);
            totalFreeBlocks |= (buffer[24] << 16);
            totalFreeBlocks |= (buffer[25] << 24);          
        }

        public void GetRAWBytes(byte[] buffer)
        {
            DEFS.ASSERT(buffer.Length == 32, "Incorrect buffer length");

            buffer[0] = (byte) type;
            buffer[1] = (byte) (position &0xFF);
            if (SPAN_TYPE.DEFAULT == type)
            {
                dataSegment[0].GetRAWBytes(buffer, 2);
            }
            else if (SPAN_TYPE.MIRRORED == type)
            {
                dataSegment[0].GetRAWBytes(buffer, 2);
                dataSegment[1].GetRAWBytes(buffer, 6);
            }
            else if (SPAN_TYPE.RAID5 == type)
            {
                dataSegment[0].GetRAWBytes(buffer, 2);
                dataSegment[1].GetRAWBytes(buffer, 6);
                dataSegment[2].GetRAWBytes(buffer, 10);
                dataSegment[3].GetRAWBytes(buffer, 14);
                paritySegment[0].GetRAWBytes(buffer, 18);
            }

            buffer[22] = (byte)(totalFreeBlocks & 0x000000FF);
            buffer[23] = (byte)((totalFreeBlocks & 0x0000FF00) >> 8);
            buffer[24] = (byte)((totalFreeBlocks & 0x00FF0000) >> 16);
            buffer[25] = (byte)((totalFreeBlocks & 0xFF000000) >> 24);

            buffer[26] = (byte)(spanUniqueHash & 0x000000FF);
            buffer[27] = (byte)((spanUniqueHash & 0x0000FF00) >> 8);
            buffer[28] = (byte)((spanUniqueHash & 0x00FF0000) >> 16);
            buffer[29] = (byte)((spanUniqueHash & 0xFF000000) >> 24);
        }

        public static DBNSegmentSpan Duplicate(DBNSegmentSpan other)
        {
            String jsonStr = JsonConvert.SerializeObject(other, Formatting.Indented);
            return JsonConvert.DeserializeObject<DBNSegmentSpan>(jsonStr);
        }

        public bool Equals(DBNSegmentSpan other)
        {
            if (this.dataSegment == null || other.dataSegment == null)
            {
                return false;
            }
            for (int i=0; i < this.dataSegment.Length; i++)
            {
                if (!this.dataSegment[i].Equals(other.dataSegment[i]))
                {
                    Console.WriteLine(this.dataSegment[i].GetStringRepresentation() + " versus " + other.dataSegment[i].GetStringRepresentation());
                    return false;
                }
            }
            if (this.paritySegment != null && other.paritySegment != null)
            {
                if (!this.paritySegment[0].Equals(other.paritySegment[0]))
                {
                    return false;
                }
            }
            if (this.start_dbn != other.start_dbn || this.isSegmentValid != other.isSegmentValid ||
                this.num_segments != other.num_segments || this.spanUniqueHash != other.spanUniqueHash ||
                this.totalFreeBlocks != other.totalFreeBlocks || this.type != other.type || this.position != other.position)
            {
                return false;
            }
            return true;
        }

        /*
         * Assumes entire file is given for creating spans, so the numSegments must equal the size in GB of the chunks passed. 
         * Also we start at offset 0 in the chunk.
         */ 
        public static DBNSegmentSpan[] AutoGenerateContigiousSpans(SPAN_TYPE st, long sdbn, int numSegments, int[] dataChunksId, int[] parityChunkIds)
        {
            if (st == SPAN_TYPE.DEFAULT)
            {
                DBNSegmentSpan[] dssList = new DBNSegmentSpan[numSegments];

                for (int i=0;i<numSegments;i++)
                {
                    long start_dbn = sdbn + i * OPS.NUM_DBNS_IN_1GB;
                    RAWSegment[] dataDefault1 = new RAWSegment[1];
                    dataDefault1[0] = new RAWSegment(SEGMENT_TYPES.Default, dataChunksId[0], i);

                    dssList[i] = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, start_dbn, dataDefault1, null);
                }
                return dssList;
            }
            else if (st == SPAN_TYPE.MIRRORED)
            {
                DBNSegmentSpan[] dssList = new DBNSegmentSpan[numSegments];

                for (int i = 0; i < numSegments; i++)
                {
                    long start_dbn = sdbn + i * OPS.NUM_DBNS_IN_1GB;
                    RAWSegment[] dataMirror = new RAWSegment[2];
                    dataMirror[0] = new RAWSegment(SEGMENT_TYPES.Mirrored, dataChunksId[0], i);
                    dataMirror[1] = new RAWSegment(SEGMENT_TYPES.Mirrored, dataChunksId[1], i);

                    dssList[i] = new DBNSegmentSpan(SPAN_TYPE.MIRRORED, start_dbn, dataMirror, null);
                }
                return dssList;
            }
            else if (st == SPAN_TYPE.RAID5)
            {
                DBNSegmentSpan[] dssList = new DBNSegmentSpan[numSegments];

                for (int i = 0; i < numSegments; i++)
                {
                    long start_dbn = sdbn + i * OPS.NUM_DBNS_IN_1GB;
                    RAWSegment[] dataRaid = new RAWSegment[4];
                    RAWSegment[] parityRaid = new RAWSegment[1];

                    dataRaid[0] = new RAWSegment(SEGMENT_TYPES.RAID5Data, dataChunksId[0], i);
                    dataRaid[1] = new RAWSegment(SEGMENT_TYPES.RAID5Data, dataChunksId[1], i);
                    dataRaid[2] = new RAWSegment(SEGMENT_TYPES.RAID5Data, dataChunksId[2], i);
                    dataRaid[3] = new RAWSegment(SEGMENT_TYPES.RAID5Data, dataChunksId[3], i);

                    parityRaid[0] = new RAWSegment(SEGMENT_TYPES.RAID5Parity, parityChunkIds[0], i);

                    dssList[i] = new DBNSegmentSpan(SPAN_TYPE.RAID5, start_dbn, dataRaid, parityRaid);
                }
                return dssList;
            }
            else
            {
                throw new SystemException("Unknow span type passed for auto generate spans");
            }
        }

        public string GetStringRepresentation()
        {
            string retvalue = "[";
            if (dataSegment != null)
            {
                for (int i = 0; i < dataSegment.Length; i++)
                {
                    retvalue += dataSegment[i].GetStringRepresentation();
                    if (i < (dataSegment.Length - 1))
                    {
                        retvalue += ",";
                    }
                }
            }
            retvalue += "] [";
            if (paritySegment != null)
            {
                for (int i = 0; i < paritySegment.Length; i++)
                {
                    retvalue += paritySegment[i].GetStringRepresentation();
                }
            }
            retvalue += "]";
            return "dbn_range:( " + start_dbn + "," + (start_dbn + OPS.NUM_DBNS_IN_1GB) + ") from pos: " + position + ", Segments : " + retvalue + "\n";
        }

        public DBNSegmentSpan(SPAN_TYPE st, long sdbn, RAWSegment[] dataSegments, RAWSegment[] paritySegments)
        {
            start_dbn = sdbn;
           
            type = st;
            if (SPAN_TYPE.DEFAULT == st && dataSegments.Length == 1)
            {
                dataSegment = new RAWSegment[1];
                dataSegment[0] = dataSegments[0];
                num_segments = 1;
                isSegmentValid = true;
            }
            else if (SPAN_TYPE.MIRRORED == st && dataSegments.Length == 2 && paritySegments == null)
            {
                if (dataSegments[0].chunkID != dataSegments[1].chunkID && 
                    (dataSegments[0].segmentTypeInt == SEGMENT_TYPES.Mirrored && dataSegments[1].segmentTypeInt == SEGMENT_TYPES.Mirrored))
                {
                    dataSegment = new RAWSegment[2];
                    dataSegment[0] = dataSegments[0];
                    dataSegment[1] = dataSegments[1];
                    num_segments = 2;
                    isSegmentValid = true;
                }
                else
                {
                    invalidReason = "Overlapping ChunkIds";
                }
            }
            else if (SPAN_TYPE.RAID5 == st && dataSegments.Length == 4 && paritySegments.Length == 1 &&
                (dataSegments[0].segmentTypeInt == SEGMENT_TYPES.RAID5Data && dataSegments[0].segmentTypeInt == SEGMENT_TYPES.RAID5Data && dataSegments[0].segmentTypeInt == SEGMENT_TYPES.RAID5Data &&
                    dataSegments[0].segmentTypeInt == SEGMENT_TYPES.RAID5Data && paritySegments[0].segmentTypeInt == SEGMENT_TYPES.RAID5Parity))
            {
                if (dataSegments[0].chunkID != dataSegments[1].chunkID &&
                    dataSegments[0].chunkID != dataSegments[2].chunkID &&
                    dataSegments[0].chunkID != dataSegments[3].chunkID &&
                    dataSegments[1].chunkID != dataSegments[2].chunkID &&
                    dataSegments[1].chunkID != dataSegments[3].chunkID &&
                    dataSegments[2].chunkID != dataSegments[3].chunkID)
                {
                    dataSegment = new RAWSegment[4];
                    paritySegment = new RAWSegment[1];

                    dataSegment[0] = dataSegments[0];
                    dataSegment[1] = dataSegments[1];
                    dataSegment[2] = dataSegments[2];
                    dataSegment[3] = dataSegments[3];
                    paritySegment[0] = paritySegments[0];

                    num_segments = 5;
                    isSegmentValid = true;
                }
                else
                {
                    invalidReason = "Overlapping ChunkIds";
                }
            }
            else
            {
                invalidReason = "Incorrect Span type or incorrect segments";
            }

            spanUniqueHash = (UInt32) (start_dbn + position + 1);
        }

        /*
         * Each Segment span is multiple of 1GB. Each 1GB has 1G/BLK_SIZE dbns
         */ 
        public int GetDBNSpaceSegmentOffset()
        {
            return (int)(start_dbn / OPS.NUM_DBNS_IN_1GB);
        }

        public static int GetDBNSpaceSegmentOffset(long start_dbn)
        {
            return (int)(start_dbn / OPS.NUM_DBNS_IN_1GB);
        }

        public static long GetDBNSpaceSegmentOffsetToStartDbn(int offset)
        {
            return offset * OPS.NUM_DBNS_IN_1GB;
        }

        public int GetNumUserDataSegments()
        {
            return (SPAN_TYPE.RAID5 == type) ? 4 : 1;
        }

        public int GetNumParitySegments()
        {
            if (paritySegment != null)
            {
                return paritySegment.Length;
            }
            return 0;
        }

        public Boolean FindReadInformationForDBN(long dbn, out int chunk_id, out long actual_offset_to_read, bool mirrorCopy)
        {
            long sdbn, edbn;
            int num_blocks;
            GetStartAndEndDBNForSpan(out sdbn, out edbn, out num_blocks);

            if (dbn < sdbn || dbn > edbn)
            {
                throw new SystemException();
            }

            if (SPAN_TYPE.DEFAULT == type || SPAN_TYPE.MIRRORED == type)
            {
                /* start_Dbn is at dataSegment[0].chunkOffset
                 * dbn is at (dbn-start_dbn)*4096 + dataSegment[0].chunkOffset
                 */
                int idx = (mirrorCopy) ? 1 : 0;

                chunk_id = dataSegment[idx].chunkID;
                actual_offset_to_read = OPS.FS_BLOCK_SIZE * ((dataSegment[idx].chunkOffset * OPS.NUM_DBNS_IN_1GB) + (dbn - start_dbn));
            }
            else if (SPAN_TYPE.RAID5 == type)
            {
                int circular_write_offset = (int)((dbn - start_dbn) % 4);
                chunk_id = dataSegment[circular_write_offset].chunkID;
                actual_offset_to_read = OPS.FS_BLOCK_SIZE * ((dataSegment[circular_write_offset].chunkOffset * OPS.NUM_DBNS_IN_1GB) + ((dbn-start_dbn)/4));

                Console.WriteLine("Chunk id " + chunk_id);
                Console.WriteLine("4k offset of segemnt in chunk " + dataSegment[circular_write_offset].chunkOffset * OPS.NUM_DBNS_IN_1GB);
                Console.WriteLine("sdbn , edbn " + sdbn + "," + edbn + ", look for " + dbn);
                Console.WriteLine("4k offset in segment " + ((dbn - start_dbn) / 4));
            }
            else
            {
                throw new SystemException();
            }
            return true;
        }

        public void GetStartAndEndDBNForSpan(out long sdbn, out long edbn, out int num_blocks)
        {
            sdbn = start_dbn;
            if (SPAN_TYPE.MIRRORED == type || SPAN_TYPE.DEFAULT == type)
            {
                edbn = start_dbn + (1) * OPS.NUM_DBNS_IN_1GB;
            } 
            else if (SPAN_TYPE.RAID5 == type)
            {
                edbn = start_dbn + (4) * OPS.NUM_DBNS_IN_1GB;
            }
            else
            {
                throw new SystemException();
            }
            num_blocks = (int)(edbn - sdbn);
        }
    }

    /*
     * This is the DBN space map. Here thee DBN space is spread as portions of DBNSegmentSpan with each span
     * having single/multiple 1GB segments to deal with. Each segment span could be default, 2x mirror or 4D+1P RAID 5
     * 
     */ 
    public class DBNSegmentSpanMap : IEquatable<DBNSegmentSpanMap>
    {
        private FileStream spanFile;
        private byte[] spanFileSyncRAWData;
        private bool isDirty = false;
        private bool isSpanMapInited = false;
        /* 
         * 1024 entries for 1TB. We support 32TB
         * If we have say 5 segements in a segment span (raid 5), then the offsets corresponding to the 4 data segments
         * which is 4 1GB segments in DBN space, will have the same DBNSegmentSpan object for the 4 entries.
         * i.e Array startDBNToDBNSegmentSpan could have duplicate entries.
         */
        public DBNSegmentSpan[] startDBNToDBNSegmentSpan;
        private Boolean mTerminateThread = false;
        private Boolean mThreadTerminated = true;

        public long max_dbn; //overflows after this. so reset the search to 0 again.

        public DBNSegmentSpanMap(int numSpans)
        {
            startDBNToDBNSegmentSpan = new DBNSegmentSpan[numSpans];
            isSpanMapInited = true;
        }

        public DBNSegmentSpanMap()
        {
            string filepath = REDFS.getAbsoluteContainerPath() + "\\spanFile.redfs";

            //Lets read the file and load all the span information.
            spanFile = new FileStream(filepath, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            spanFile.SetLength(OPS.NUM_SPAN_MAX_ALLOWED * OPS.SPAN_ENTRY_SIZE_BYTES); /* 1MB */
            spanFile.Seek(0, SeekOrigin.Begin);

            spanFileSyncRAWData = new byte[OPS.NUM_SPAN_MAX_ALLOWED * OPS.SPAN_ENTRY_SIZE_BYTES];
            spanFile.Read(spanFileSyncRAWData, 0, spanFileSyncRAWData.Length);

            Console.WriteLine("Finished readinng spanfile into the local 1mb buffer");

            startDBNToDBNSegmentSpan = new DBNSegmentSpan[OPS.NUM_SPAN_MAX_ALLOWED];

            byte[] buffer = new byte[OPS.SPAN_ENTRY_SIZE_BYTES];

            Console.WriteLine(">>>>>>> DBNSegmentSpanMap()");
            
            //Lets start reading the span information;
            for (int i=0;i<OPS.NUM_SPAN_MAX_ALLOWED;i++)
            {
                
                Array.Clear(buffer, 0, buffer.Length);
                Array.Copy(spanFileSyncRAWData, i * OPS.SPAN_ENTRY_SIZE_BYTES, buffer, 0, OPS.SPAN_ENTRY_SIZE_BYTES);
                
                startDBNToDBNSegmentSpan[i] = new DBNSegmentSpan(DBNSegmentSpan.GetDBNSpaceSegmentOffsetToStartDbn(i), buffer);
                if (startDBNToDBNSegmentSpan[i].isSegmentValid)
                {
                    Console.WriteLine("loaded span : (" +  i + ") " + startDBNToDBNSegmentSpan[i].GetStringRepresentation());

                    long sdbn, edbn;
                    int numblks;
                    startDBNToDBNSegmentSpan[i].GetStartAndEndDBNForSpan(out sdbn, out edbn, out numblks);
                    max_dbn = edbn;
                }
            }
            
            isSpanMapInited = true;
        }

        public void init()
        {
            //Now lets start the delete log thread. this thread basically updates used_block count for spans after a free.
            Thread tc = new Thread(new ThreadStart(tServiceThread));
            tc.Start();
        }

        public void tServiceThread()
        {
            while (!mTerminateThread)
            {
                Thread.Sleep(10);
                lock (GLOBALQ.m_deletelog_spanmap)
                {
                    int count = GLOBALQ.m_deletelog_spanmap.Count;

                    try
                    {
                        for (int i = 0; i < count; i++)
                        {
                            long dbn = GLOBALQ.m_deletelog_spanmap.ElementAt(0);
                            REDFSCoreSideMetrics.m.InsertMetric(METRIC_NAME.BLOCK_DRAIN, count);

                            GLOBALQ.m_deletelog_spanmap.RemoveAt(0);

                            int start_at = DBNSegmentSpan.GetDBNSpaceSegmentOffset(dbn);
                            if (startDBNToDBNSegmentSpan[start_at] != null && startDBNToDBNSegmentSpan[start_at].isSegmentValid)
                            {
                                if (SPAN_TYPE.RAID5 == startDBNToDBNSegmentSpan[start_at].type)
                                {
                                    //As we have wafl style write, free should ideally give 4 dbns for raid5. we can free individually at each segemnt
                                    //as we know it will come in the deletelog queue.
                                    if (dbn % 4 == 0 && startDBNToDBNSegmentSpan[start_at].position == 0)
                                    {
                                        startDBNToDBNSegmentSpan[start_at].totalFreeBlocks += 1;
                                        startDBNToDBNSegmentSpan[start_at + 1].totalFreeBlocks += 1;
                                        startDBNToDBNSegmentSpan[start_at + 2].totalFreeBlocks += 1;
                                        startDBNToDBNSegmentSpan[start_at + 3].totalFreeBlocks += 1;
                                    }
                                    else
                                    {
                                        //Just ignore. very risky!
                                    }
                                }
                                else
                                {
                                    startDBNToDBNSegmentSpan[start_at].totalFreeBlocks += 1;
                                    //Console.WriteLine("Freeing " + dbn + " @ " + startDBNToDBNSegmentSpan[start_at].type + " FB: " + startDBNToDBNSegmentSpan[start_at].totalFreeBlocks);
                                }
                                isDirty = true;
                            }
                            else
                            {
                                throw new SystemException("We are freeing a dbn in a segment that does not exist!");
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        throw new SystemException("EXCEPTION : Caught in blk update for span : cnt = " + count + " and size = " +
                            GLOBALQ.m_deletelog2.Count + " e.msg = " + e.Message);
                    }
                }
            }
            mThreadTerminated = true;
        }

        public bool Equals(DBNSegmentSpanMap other)
        {
            Console.WriteLine("Entering equals >>>> ");
            for (int i = 0; i < OPS.NUM_SPAN_MAX_ALLOWED; i++)
            {
                if (this.startDBNToDBNSegmentSpan[i].isSegmentValid && other.startDBNToDBNSegmentSpan[i].isSegmentValid)
                {
                    if (!this.startDBNToDBNSegmentSpan[i].Equals(other.startDBNToDBNSegmentSpan[i]))
                    {
                        Console.WriteLine("Equals Fail @ " + i + " (" + this.startDBNToDBNSegmentSpan[i].GetStringRepresentation() + "  /vs/  " +
                            other.startDBNToDBNSegmentSpan[i].GetStringRepresentation() + ")");
                        return false;
                    }
                }
            }
            return true;
        }

        public long GetTotalAvailableFreeBlocks()
        {
            long freeBlocks = 0;
            for (int i = 0; i < OPS.NUM_SPAN_MAX_ALLOWED; i++)
            {
                if (startDBNToDBNSegmentSpan[i].isSegmentValid)
                {
                    freeBlocks += startDBNToDBNSegmentSpan[i].totalFreeBlocks;
                }
            }
            return freeBlocks;
        }

        public long GetAvailableBlocksWithType(SPAN_TYPE type)
        {
            long freeBlocks = 0;
            for (int i = 0; i < OPS.NUM_SPAN_MAX_ALLOWED; i++)
            {
                if (startDBNToDBNSegmentSpan[i].isSegmentValid && startDBNToDBNSegmentSpan[i].type == type)
                {
                    freeBlocks += startDBNToDBNSegmentSpan[i].totalFreeBlocks;
                }
            }
            return freeBlocks;
        }

        private bool isSpanMapOkay()
        {
            if (!isSpanMapInited)
            {
                throw new SystemException("Spanmap is closed and not inited");
            }
            return true;
        }
        public void MarkDiry()
        {
            if (isSpanMapOkay())
            {
                isDirty = true;
            }
        }

        //Copy data in the array to the binary data in the object, use locks
        public void Sync()
        {
            if (isDirty)
            {
                byte[] buffer = new byte[OPS.SPAN_ENTRY_SIZE_BYTES];

                //Lets start reading the span information;
                for (int i = 0; i < OPS.NUM_SPAN_MAX_ALLOWED; i++)
                {
                    if (startDBNToDBNSegmentSpan[i] != null && startDBNToDBNSegmentSpan[i].isSegmentValid == true)
                    {
                        Array.Clear(buffer, 0, buffer.Length);
                        startDBNToDBNSegmentSpan[i].GetRAWBytes(buffer);
                        Array.Copy(buffer, 0, spanFileSyncRAWData, i * OPS.SPAN_ENTRY_SIZE_BYTES, OPS.SPAN_ENTRY_SIZE_BYTES);
                        Console.WriteLine("copied buffer at offset :" + i * OPS.SPAN_ENTRY_SIZE_BYTES);

                        //are we wrting correctly/.?
                        DBNSegmentSpan tempo = new DBNSegmentSpan(startDBNToDBNSegmentSpan[i].start_dbn, buffer);
                        Console.WriteLine("wriing ... " + tempo.GetStringRepresentation());
                    }
                }

                if (REDFS.isTestMode)
                {
                    try
                    {
                        spanFile.Seek(0, SeekOrigin.Begin);
                        spanFile.Write(spanFileSyncRAWData);
                        spanFile.Flush();
                    }
                    catch (IOException e)
                    {
                        Console.WriteLine("Catch error");
                    }
                }
                else
                {
                    spanFile.Seek(0, SeekOrigin.Begin);
                    spanFile.Write(spanFileSyncRAWData);
                    spanFile.Flush();
                }
                isDirty = false;
            }
        }

        public void SyncAndTerminate()
        {
            if (isSpanMapOkay())
            {
                mTerminateThread = true;
                while (!mThreadTerminated)
                {
                    Thread.Sleep(100);
                }
                isDirty = true;
                Sync();
                spanFile.Close();
                spanFile.Dispose();
            }
        }

        /*
         * Returns the start dbn from where the allocator will look for free blocks
         * XCXX. .Update the totalFreeBlocks count as well
         */
        public long GetSpanWithSpecificTypesAndRequiredFreeBlocks(SPAN_TYPE type, long start_dbn, int numDbns)
        {
            //Lets start from the begining of the span
            int start_at = DBNSegmentSpan.GetDBNSpaceSegmentOffset(start_dbn);
            if (startDBNToDBNSegmentSpan[start_at].type == SPAN_TYPE.RAID5)
            {
                start_dbn -= (OPS.NUM_DBNS_IN_1GB * startDBNToDBNSegmentSpan[start_at].position);
            }

            //So start here now.
            start_at = DBNSegmentSpan.GetDBNSpaceSegmentOffset(start_dbn);

            for (int i = start_at; i < startDBNToDBNSegmentSpan.Length; i++)
            {
                if (startDBNToDBNSegmentSpan[i] != null && startDBNToDBNSegmentSpan[i].isSegmentValid)
                {
                    int totalFreeBlocks = 0;
                    if (startDBNToDBNSegmentSpan[i].type == SPAN_TYPE.RAID5 && startDBNToDBNSegmentSpan[i].position == 0)
                    {
                        DEFS.ASSERT(numDbns % 4 == 0, "For raid 5, num dbns must be multiple of 4");
                        totalFreeBlocks += startDBNToDBNSegmentSpan[i].totalFreeBlocks;
                        totalFreeBlocks += startDBNToDBNSegmentSpan[i+1].totalFreeBlocks;
                        totalFreeBlocks += startDBNToDBNSegmentSpan[i+2].totalFreeBlocks;
                        totalFreeBlocks += startDBNToDBNSegmentSpan[i+3].totalFreeBlocks;
                    } 
                    else
                    {
                        totalFreeBlocks += startDBNToDBNSegmentSpan[i].totalFreeBlocks;
                    }

                    if (startDBNToDBNSegmentSpan[i].type == type)
                    {
                        if (totalFreeBlocks >= numDbns)
                        {
                            if (SPAN_TYPE.RAID5 == type)
                            {
                                startDBNToDBNSegmentSpan[i].totalFreeBlocks -= numDbns / 4;
                                startDBNToDBNSegmentSpan[i+1].totalFreeBlocks -= numDbns / 4;
                                startDBNToDBNSegmentSpan[i+2].totalFreeBlocks -= numDbns / 4;
                                startDBNToDBNSegmentSpan[i+3].totalFreeBlocks -= numDbns / 4;
                            }
                            else
                            {
                                startDBNToDBNSegmentSpan[i].totalFreeBlocks -= numDbns;
                            }
                            return startDBNToDBNSegmentSpan[i].start_dbn;
                        }
                    }

                    if (SPAN_TYPE.RAID5 == startDBNToDBNSegmentSpan[i].type && startDBNToDBNSegmentSpan[i].position == 0)
                    {
                        i += 3; //Skip the next 3 positions as we know its raid 5 and we have already checked for space
                    }
                }
            }
            //File system if full?
            return -1;
        }

        public bool IncrementBlockUsageCounterInSegment(long start_dbn, int pos, int count)
        {
            int offset = DBNSegmentSpan.GetDBNSpaceSegmentOffset(start_dbn);
            startDBNToDBNSegmentSpan[offset + pos].totalFreeBlocks -= count;
            return false;
        }

        /*
         * We have a chunk, lets find out a bitmap of segments that are used/vs/unused.
         * We do this by iterating all the span elements, then we examine the span element to see if any of the span
         * element is stored in the chunk we are interested in. If yes, we get the offset of the chunk and update the
         * corresponding bit in the boolean array.
         */ 
        public bool[] getSegmentUsageBitmapForChunk(REDFSChunk chunk)
        {
            bool[] returnvalue = Array.Empty <bool>();
            if (isSpanMapOkay())
            {
               returnvalue = new bool[chunk.numSegmentsInChunk];
                
                for (int i = 0; i < startDBNToDBNSegmentSpan.Length; i++)
                {
                    if (startDBNToDBNSegmentSpan[i] != null && startDBNToDBNSegmentSpan[i].isSegmentValid)
                    {
                        if (startDBNToDBNSegmentSpan[i].dataSegment != null)
                        {
                            for (int j = 0; j < startDBNToDBNSegmentSpan[i].dataSegment.Length; j++)
                            {
                                
                                if (startDBNToDBNSegmentSpan[i].dataSegment[j].chunkID == chunk.chunkID)
                                {
                                    int chunktype = (int)chunk.allowed_types;
                                    int segtype = (int)startDBNToDBNSegmentSpan[i].dataSegment[j].segmentTypeInt;
                                    if ((chunktype & segtype) == 0)
                                    {
                                        Console.WriteLine(chunktype + " versus " + segtype);
                                        throw new SystemException("Segment types mentioned in chunk and map dont match! Incorrect usage of chunk vis-a-vis the segment! (1)");
                                    }
                                    else
                                    {
                                        //The offset in the chunk is being used.
                                        DEFS.ASSERT(startDBNToDBNSegmentSpan[i].dataSegment[j].chunkOffset < returnvalue.Length, "Mismatch in length of chunk and usage in segments! 1 (" +
                                                startDBNToDBNSegmentSpan[i].dataSegment[j].chunkOffset + ", " + returnvalue.Length + ") chunk id,size=(" + 
                                                chunk.chunkID + "," + chunk.numSegmentsInChunk);
                                        returnvalue[startDBNToDBNSegmentSpan[i].dataSegment[j].chunkOffset] = true;
                                    }
                                }
                            }
                        }
                        if (startDBNToDBNSegmentSpan[i].paritySegment != null) {
                            for (int k = 0; k < startDBNToDBNSegmentSpan[i].paritySegment.Length; k++)
                            {
                                if (startDBNToDBNSegmentSpan[i].paritySegment[k].chunkID == chunk.chunkID)
                                {
                                    int chunktype = (int)chunk.allowed_types;
                                    int segtype = (int)startDBNToDBNSegmentSpan[i].paritySegment[k].segmentTypeInt;
                                    
                                    if ((chunktype & segtype) == 0)
                                    {
                                        throw new SystemException("Segment types mentioned in chunk and map dont match! Incorrect usage of chunk vis-a-vis the segment! (2) ");
                                    }
                                    else
                                    {
                                        //The offset in the chunk is being used.
                                        DEFS.ASSERT(startDBNToDBNSegmentSpan[i].dataSegment[k].chunkOffset < returnvalue.Length, "Mismatch in length of chunk and usage in segments! 2 (" +
                                            startDBNToDBNSegmentSpan[i].dataSegment[k].chunkOffset + "," +  returnvalue.Length + ")");
                                        returnvalue[startDBNToDBNSegmentSpan[i].dataSegment[k].chunkOffset] = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return returnvalue;
        }

        public void InsertDBNSegmentSpan(DBNSegmentSpan span)
        {
            Console.WriteLine("Inserting segment : " + span.GetStringRepresentation());
            if (isSpanMapOkay())
            {
                int startSeg = span.GetDBNSpaceSegmentOffset();

                for (var i = 0; i < span.GetNumUserDataSegments(); i++)
                {
                    if (startDBNToDBNSegmentSpan[startSeg + i] == null || startDBNToDBNSegmentSpan[startSeg + i].isSegmentValid == false)
                    {
                            DBNSegmentSpan dupSpan = DBNSegmentSpan.Duplicate(span);
                            Console.WriteLine("inserting startSeg = " + (startSeg + i));
                            dupSpan.position = i;
                            dupSpan.totalFreeBlocks = OPS.NUM_DBNS_IN_1GB;
                            dupSpan.start_dbn = span.start_dbn + i * OPS.NUM_DBNS_IN_1GB;
                            startDBNToDBNSegmentSpan[startSeg + i] = dupSpan;

                            long sdbn, edbn;
                            int numblks;
                            dupSpan.GetStartAndEndDBNForSpan(out sdbn, out edbn, out numblks);
                            if (max_dbn < edbn)
                            {
                                max_dbn = edbn;
                            }
                    }
                    else
                    {
                        throw new SystemException("We are trying to insert into a span which already exists");
                    }
                }
            }
        }

        /*
         * Remember, each span consists of multiple of 1GB data
         */ 
        public DBNSegmentSpan GetDBNSegmentSpan(long start_dbn)
        {
            try
            {
                if (isSpanMapOkay())
                {
                    int segmentOffset = DBNSegmentSpan.GetDBNSpaceSegmentOffset(start_dbn);
                    if (startDBNToDBNSegmentSpan[segmentOffset] != null)
                    {
                        return startDBNToDBNSegmentSpan[segmentOffset];
                    }
                }
                throw new Exception("Span not okay! or we didnt find anything for " + start_dbn);
            }
            catch (Exception e)
            {
                Console.WriteLine("FATAL: "  + e.Message);
                throw new SystemException();
            }
        }

        //Lets return a representation object, say json, so that we can have compare tests
        public void PrintSpanToSegmentMapping()
        {
            Console.WriteLine("Entering PrintSpanToSegmentMapping() total : " + startDBNToDBNSegmentSpan.Length);
            for (int spanOffset = 0; spanOffset < startDBNToDBNSegmentSpan.Length; spanOffset++)
            {
                if (startDBNToDBNSegmentSpan[spanOffset] != null && startDBNToDBNSegmentSpan[spanOffset].isSegmentValid)
                {
                    Console.WriteLine(startDBNToDBNSegmentSpan[spanOffset].GetStringRepresentation());
                }
            }
        }

        public ReadPlanElement PrepareReadPlanSingle(long dbn)
        {
            if (isSpanMapOkay())
            {
                DBNSegmentSpan span = GetDBNSegmentSpan(dbn);
                int chunk_id;
                long actual_read_offset = 0;

                span.FindReadInformationForDBN(dbn, out chunk_id, out actual_read_offset, false);
                ReadPlanElement rpe = new ReadPlanElement();
                rpe.chunkId = chunk_id;
                rpe.readOffset = actual_read_offset;
                rpe.dbn = dbn;

                return rpe;
            }
            return null;
        }
        /*
         * To actually read from disk, we must prepare a read plan. This function accepts the start_dbn where to read.
         * Notice that if the file system wants to read byte 2 in dbn 2, we will tell the caller on how many blocks
         * to be read from which files (chunks)
         * 
         * Depending on the situation, if you want to read dbn 2 and that segmentspan is RAID 5, then this function
         * will ask to read dbn 0 to dbn 4, i.e 4 blocks.
         */
        public List<ReadPlanElement> PrepareReadPlan(long[] dbns)
        {
            List<ReadPlanElement> rpeList = new List<ReadPlanElement>();
            if (isSpanMapOkay())
            {
                long[] start_dbns = new long[dbns.Length];

                //get start_dbns for the series, must fall within the same span
                var start_dbn_t = dbns[0];

                DBNSegmentSpan span = GetDBNSegmentSpan(start_dbn_t);

                for (var i = 0; i < dbns.Length; i++)
                {
                    int chunk_id;
                    long actual_read_offset = 0;

                    span.FindReadInformationForDBN(dbns[i], out chunk_id, out actual_read_offset, false);

                    ReadPlanElement rpe = new ReadPlanElement();
                    rpe.chunkId = chunk_id;
                    rpe.readOffset = actual_read_offset;
                    rpe.dbn = dbns[i];

                    rpeList.Add(rpe);
                }
            }
            return rpeList;
        }

        public WritePlanElement PrepareWritePlanSingle(long dbn)
        {
            if (isSpanMapOkay())
            {
                DBNSegmentSpan span = GetDBNSegmentSpan(dbn);
                int chunk_id;
                long actual_write_offset = 0;

                DEFS.ASSERT(span.type == SPAN_TYPE.DEFAULT || span.type == SPAN_TYPE.MIRRORED, "Cant do single dbn read on RAID5, for now!");

                int sizes = (span.type == SPAN_TYPE.DEFAULT) ? 1 : 2;

                WritePlanElement wpe = new WritePlanElement();
                wpe.dataChunkIds = new int[sizes];
                wpe.dbns = new long[sizes];
                wpe.writeOffsets = new long[sizes];

                span.FindReadInformationForDBN(dbn, out chunk_id, out actual_write_offset, false);
                
                wpe.dataChunkIds[0] = chunk_id;
                wpe.dbns[0] = dbn;
                wpe.writeOffsets[0] = actual_write_offset;

                if (sizes == 2)
                {
                    span.FindReadInformationForDBN(dbn, out chunk_id, out actual_write_offset, true);
                    wpe.dataChunkIds[1] = chunk_id;
                    wpe.dbns[1] = dbn;
                    wpe.writeOffsets[1] = actual_write_offset;
                }
                return wpe;
            }
            return null;
        }

        /*
         * Prepare a write plan. Note the dbns itself indicates which segment they map to and the type
         * of segment span they will fall under. The dbns are always aligned to be withing a single span.
         * Dbns cannot cross span boundaries. This must be made sure by the caller. Else we throw system exception.
         * 
         * For SPAN_TYPE.DEFAULT, any count would do as long as dbns fall in the same segment.
         * For SPAN_TYPE.MIRRORED, any count would do, with two chunks and within a segment of each of the chunk
         * For SPAN_TYPE.RAID5, multiple of 4 blocks, again within segment boundaries and 4 chunks.
         * 
         * Given dbns are free dbns which we want to write to. We just have to figure out the file and offsets. If its RAID5,
         * automataically insert a WritePlanElement to figure out where to write the parity block. A writeplan is striped at
         * 32k or 4 bloks. So we return one element for each 32k stripe.
         */
        public List<WritePlanElement> PrepareWritePlan(long[] dbns, SPAN_TYPE type)
        {
            List<WritePlanElement> wpeList = new List<WritePlanElement>();
            if (isSpanMapOkay())
            {
                //Verify multiple of 4 and all contigious.
                if (type == SPAN_TYPE.RAID5 && dbns.Length % 4 != 0)
                {
                    throw new SystemException();
                }

                int num_stripes = dbns.Length / 4;
                if (type == SPAN_TYPE.RAID5)
                {
                    for (var j = 0; j < num_stripes; j++)
                    {
                        long sdbn = dbns[j * 4];
                        for (var i = 0; i < 4; i++)
                        {
                            if (dbns[i + j * 4] != sdbn + i)
                            {
                                throw new SystemException();
                            }
                        }
                    }
                }
                var start_dbn_t = dbns[0];

                DBNSegmentSpan span = GetDBNSegmentSpan(start_dbn_t);

                if (type == SPAN_TYPE.DEFAULT)
                {
                    for (var j = 0; j < dbns.Length; j++)
                    {
                        WritePlanElement wpe = new WritePlanElement();

                        int chunk_id;
                        long actual_write_offset = 0;
                        span.FindReadInformationForDBN(dbns[j], out chunk_id, out actual_write_offset, false);

                        wpe.dataChunkIds[0] = chunk_id;
                        wpe.parityChunkIds[0] = 0;
                        wpe.dbns[0] = dbns[j];
                        wpe.writeOffsets[0] = actual_write_offset;
                        wpeList.Add(wpe);
                    }
                    return wpeList;
                }
                else if (type == SPAN_TYPE.MIRRORED)
                {
                    for (var j = 0; j < dbns.Length; j++)
                    {
                        WritePlanElement wpe = new WritePlanElement();

                        int chunk_id;
                        long actual_write_offset = 0;
                        span.FindReadInformationForDBN(dbns[j], out chunk_id, out actual_write_offset, false);

                        wpe.dataChunkIds[0] = chunk_id;
                        wpe.parityChunkIds[0] = 0;
                        wpe.dbns[0] = dbns[j];
                        wpe.writeOffsets[0] = actual_write_offset;

                        span.FindReadInformationForDBN(dbns[j], out chunk_id, out actual_write_offset, true);

                        wpe.dataChunkIds[1] = chunk_id;
                        wpe.parityChunkIds[1] = 0;
                        wpe.dbns[1] = dbns[j]; /*dbn is same as its a mirror copy*/
                        wpe.writeOffsets[1] = actual_write_offset;

                        wpeList.Add(wpe);
                    }
                    return wpeList;
                }
                else if (type == SPAN_TYPE.RAID5)
                {
                    for (var j = 0; j < num_stripes; j++)
                    {
                        WritePlanElement wpe = new WritePlanElement();
                        for (var r = 0; r < 4; r++)
                        {
                            int chunk_id;
                            long actual_write_offset = 0;
                            span.FindReadInformationForDBN(dbns[r + j * 4], out chunk_id, out actual_write_offset, false);

                            wpe.dataChunkIds[r] = chunk_id;
                            wpe.parityChunkIds[r] = 0;
                            wpe.dbns[r] = dbns[r + j * 4];
                            wpe.writeOffsets[r] = actual_write_offset;
                        }
                        wpeList.Add(wpe);
                    }
                    return wpeList;
                }
                else
                {
                    throw new SystemException();
                }
            }
            return wpeList;
        }
    }
}
