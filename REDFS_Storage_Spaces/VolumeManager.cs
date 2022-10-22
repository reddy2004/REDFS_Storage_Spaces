using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using Newtonsoft.Json;

namespace REDFS_ClusterMode
{
    public class VolumeOperation
    {
        public string operation;
        public long volumeId;
        public string volname;
        public string hexcolor;
        public string volDesc;

        public VolumeOperation()
        {

        }
    }

    public class VirtualVolume
    {
        public int volumeId { get; set; }
        public long parentVolumeId { get; set; }
        public Boolean parentIsSnapshot { get; set; }

        public string volname { get; set; }

        public Boolean isDeleted { get; set; }


        public long logicalData { get; set; }

        public DateTime volumeCreateTime { get; set; }
        public String hexcolor { get; set; }

        public String status = "-";

        public Boolean isDirty = false;

        public string volDescription { get; set; }

        public bool isReadOnly { get; set; }
        public VirtualVolume()
        {
            volumeCreateTime = DateTime.Now;
        }
    }

    public class VolumeManager
    {
        public LinkedList<VirtualVolume> volumes = new LinkedList<VirtualVolume>();
        public int maxVolumeId = 0;
        public string containerFolderPath;
        public Boolean newRootVolumeCreated = false;

        public VolumeManager(string containerFolderPath1)
        {
            containerFolderPath = containerFolderPath1;
        }

        public Boolean CreateRootVolumeForNewContainer()
        {
            //First check if this container folder already has something valid?
            string volumesFile = containerFolderPath + "\\volumes.json";

            //if (File.Exists(volumesFile))
            //{
            //    throw new SystemException();
            //}

            VirtualVolume v = new VirtualVolume();
            v.volumeId = 0;
            v.parentVolumeId = -1;
            v.volname = "root";
            v.volumeCreateTime = DateTime.Now;

            volumes.AddFirst(v);

            SaveVolumeListToDisk();

            /*
             * The container must create a fsid[0] for this
             */ 
            newRootVolumeCreated = true;

            return true;
        }

        //Fill in other parameters to decorate the parameters
        private VirtualVolume DecorateVolume(VirtualVolume b2) {
            return b2;
        }

        //Wont be verified if it fits in graph since this is coming from disk.
        public void LoadVolumeListFromDisk() {
            int count = 0;
            volumes.Clear();
            try
            {
                string volumesFile = containerFolderPath + "\\volumes.json";
                Console.WriteLine("Reading volumes for container " + containerFolderPath + " @ location " + volumesFile);
                using (StreamReader sr = new StreamReader(volumesFile))
                {
                    string line;
                    while ((line = sr.ReadLine()) != null)
                    {
                        Console.WriteLine(line);
                        VirtualVolume b2 = JsonConvert.DeserializeObject<VirtualVolume>(line);
                        if (b2.volumeId > maxVolumeId) {
                            maxVolumeId = b2.volumeId;
                        }
                        volumes.AddLast(DecorateVolume(b2));
                        count++;
                    }
                }

                //Maybe its a new container, so create the default root volume.
                //Recursive call
                if (count == 0)
                {
                    volumes.Clear();
                    CreateRootVolumeForNewContainer();
                    LoadVolumeListFromDisk();
                    return;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("The file could not be read:");
                Console.WriteLine(e.Message);
            }
            Console.WriteLine("Finished reading volumes from disk");
        }

        /* Save only required information, not the whole array */
        public void SaveVolumeListToDisk() {
            try {
                string volumesFile = containerFolderPath + "\\volumes.json";
                Console.WriteLine("Saving volumes for container " + containerFolderPath + " @ location " + volumesFile);

                using (StreamWriter sw = new StreamWriter(volumesFile))
                {
                    foreach (var volume in volumes) {
                        String vstr = JsonConvert.SerializeObject(volume, Formatting.None);
                        Console.WriteLine(vstr);
                        sw.WriteLine(vstr);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("The file could not be read:");
                Console.WriteLine(e.Message);
            }
        }

        public void UpdateVolume(long volumeId, string volumename, string desc, string hc) {
            VirtualVolume v = FindVolume(volumeId);
            if (v != null)
            {
                v.volname = volumename;
                v.hexcolor = hc;
                v.volDescription = desc;
                SaveVolumeListToDisk();
            }
        }

        //Takes a snapshot
        public void VolumeSnapshot(long parentVolumeId, string newVolumeName) {
            Console.WriteLine("Creating snapshot " + parentVolumeId);
            VirtualVolume v = new VirtualVolume();
            if (HasSnapshotAlready(parentVolumeId))
            {
                Console.WriteLine("Cannot create snapshot as this volume has snapshots already, Try backed cloning instead");
                throw new SystemException();
            }

            Boolean found = false;
            foreach (var volume in volumes)
            {
                Console.WriteLine(volume.volumeId);
                if (volume.volumeId == parentVolumeId)
                {
                    found = true;
                    v.parentVolumeId = parentVolumeId;
                    v.parentIsSnapshot = true;
                    v.volname = newVolumeName;

                    int volid = -1;
                    if (!REDFS.isTestMode)
                    {
                        RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[parentVolumeId];
                        volid = REDFS.redfsContainer.ifsd_mux.CreateNewFSIDFromExistingFSID(rfsid);
                        DEFS.ASSERT(volid == REDFS.redfsContainer.ifsd_mux.numValidFsids - 1, "zero index volid should concur to fsid");
                        maxVolumeId = volid;
                    }
                    else
                    {
                        maxVolumeId++;
                    }

                    
                    v.volumeId = maxVolumeId;

                    //also make the parent read-only
                    volume.isReadOnly = true;

                     v.volDescription = "Snapshot of " + parentVolumeId;
                     v.hexcolor = volume.hexcolor;
                    break;
                }
            }
            if (found)
            {
                volumes.AddLast(v);
            }
            SaveVolumeListToDisk();
        }

        /*
         * Dummy test function
         */ 
        public void CreateZeroVolume(string newVolumeName)
        {
            maxVolumeId++;
            VirtualVolume v = new VirtualVolume();
            v.parentVolumeId = 0;
            v.volname = newVolumeName;
            v.volumeId = maxVolumeId;
            v.parentIsSnapshot = false;
            maxVolumeId++;

            volumes.AddLast(v);
            SaveVolumeListToDisk();
        }

        //takes a clone of a volume
        public void CloneVolume(long parentVolumeId, string newVolumeName) {
            
            VirtualVolume v = new VirtualVolume();
            VirtualVolume vcont = new VirtualVolume();

            if (HasSnapshotAlready(parentVolumeId))
            {
                Console.WriteLine("Cannot create snapshot as this volume has snapshots already, Try raw cloning instead");
                //throw new SystemException();
                return;
            }

            Boolean found = false;
            foreach (var volume in volumes)
            {
                int volid = -1;
                if (volume.volumeId == parentVolumeId) {
                    found = true;
                    volume.isReadOnly = true;

                    v.parentVolumeId = parentVolumeId;
                    v.parentIsSnapshot = false;
                    v.volumeId = maxVolumeId;
                    v.volname = newVolumeName;
                    v.volDescription = "clone of " + parentVolumeId;
                    v.hexcolor = volume.hexcolor;

                    if (!REDFS.isTestMode)
                    {
                        //one copy as a clone
                        RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[parentVolumeId];
                        volid = REDFS.redfsContainer.ifsd_mux.CreateNewFSIDFromExistingFSID(rfsid);
                        DEFS.ASSERT(volid == REDFS.redfsContainer.ifsd_mux.numValidFsids - 1, "zero index volid should concur to fsid");
                        v.volumeId = volid;
                    }
                    else
                    {
                        v.volumeId = maxVolumeId++;
                    }

                    vcont.volumeId = maxVolumeId;
                    vcont.parentVolumeId = parentVolumeId;
                    vcont.parentIsSnapshot = true;
                    vcont.volname = volume.volname; //use same name
                    vcont.volDescription = "snapshot of " + parentVolumeId;
                    vcont.hexcolor = volume.hexcolor;

                    if (!REDFS.isTestMode)
                    {
                        //one copy as a snapshot.
                        RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[parentVolumeId];
                        volid = REDFS.redfsContainer.ifsd_mux.CreateNewFSIDFromExistingFSID(rfsid);
                        DEFS.ASSERT(volid == REDFS.redfsContainer.ifsd_mux.numValidFsids - 1, "zero index volid should concur to fsid");
                        vcont.volumeId = volid;

                        maxVolumeId = volid;
                    }
                    else
                    {
                        vcont.volumeId = maxVolumeId++;
                    }
                    
                    break;
                }
            }
            if (found) {
                volumes.AddLast(v);
                volumes.AddLast(vcont);
            }
            SaveVolumeListToDisk();
        }

        public void CloneVolumeRaw(long parentVolumeId, string newVolumeName, string desc, string hc)
        {
            VirtualVolume v = new VirtualVolume();

            Boolean found = false;
            foreach (var volume in volumes)
            {
                if (volume.volumeId == parentVolumeId)
                {
                    found = true;

                    //for test case to pass
                    if (REDFS.redfsContainer != null)
                    {
                        int volid = -1;
                        if (parentVolumeId == 0)
                        {
                            volid = REDFS.redfsContainer.ifsd_mux.CreateAndInitNewFSIDFromRootVolume();
                            DEFS.ASSERT(volid == REDFS.redfsContainer.ifsd_mux.numValidFsids -1, "zero index volid should concur to fsid");
                        }
                        else
                        {
                            RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[parentVolumeId];
                            volid = REDFS.redfsContainer.ifsd_mux.CreateNewFSIDFromExistingFSID(rfsid);
                            DEFS.ASSERT(volid == REDFS.redfsContainer.ifsd_mux.numValidFsids - 1, "zero index volid should concur to fsid");
                        }
                        DEFS.ASSERT(volid > maxVolumeId, "We should've created a new volume by now!");
                        maxVolumeId = volid;
                    }
                    else
                    {
                        maxVolumeId++;
                    }

                    
                    v.parentVolumeId = parentVolumeId;
                    v.parentIsSnapshot = false;
                    v.volumeId = maxVolumeId;
                    v.volname = newVolumeName;

                    if (parentVolumeId == 0)
                    {
                        v.volDescription = desc;
                        v.hexcolor = hc;
                    } else
                    {
                        v.volDescription = "Clone of " + parentVolumeId;
                        v.hexcolor = volume.hexcolor;
                    }

                    break;
                }
            }
            if (found)
            {
                volumes.AddLast(v);
            }
            SaveVolumeListToDisk();
        }

        public String GetVolumeJSONList() {
            string json = JsonConvert.SerializeObject(volumes, Formatting.Indented);
            return json;
        }

        public string CalculateMD5Hash(string input)
        {
            // step 1, calculate MD5 hash from input
            MD5 md5 = System.Security.Cryptography.MD5.Create();
            byte[] inputBytes = System.Text.Encoding.ASCII.GetBytes(input);
            byte[] hash = md5.ComputeHash(inputBytes);

            // step 2, convert byte array to hex string
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < hash.Length; i++)
            {
                sb.Append(hash[i].ToString("X2"));
            }
            return sb.ToString();
        }

        public static String GetTimestamp(DateTime value)
        {
            return value.ToString("yyyyMMddHHmmssffff");
        }

        private VirtualVolume FindVolume(long volumeId) {
            foreach (var volume in volumes)
            {
                if (volume.volumeId == volumeId)
                {
                    return volume;
                }
            }
            return null;
        }

        private Boolean HasSnapshotAlready(long volumeId)
        {
            foreach (var volume in volumes)
            {
                if (volume.parentVolumeId == volumeId && volume.parentIsSnapshot == true)
                {
                    return true;
                }
            }
            return false;
        }
        private Boolean HasSiblingsOrChildren(long volumeId) {
            foreach (var volume in volumes)
            {
                if (volume.parentVolumeId == volumeId)
                {
                    return true;
                }
            }
            return false;
        }

        /*
         * does not actually remove volume struct, just marks it as deleted in case it has children.
         * If its a leaf volume, then delete it. Create a volume cleanup job to delete data and recover
         * space
         */ 
        public Boolean DeleteVolume(long volumeId) {
            VirtualVolume v = FindVolume(volumeId);
            if (v != null)
            {
                //if has children, or sibling then mark as deleted, otherwise
                //if its leaf node, then delete it directly.
                if (HasSiblingsOrChildren(volumeId)) {
                    v.isDeleted = true;
                } else {
                    volumes.Remove(v);
                }
                return true;
            }
            SaveVolumeListToDisk();
            return false;
        }
    }
}
