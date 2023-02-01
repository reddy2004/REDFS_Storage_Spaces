using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Collections;

namespace REDFS_ClusterMode
{
        //Incoming object from the client side
        public class HostSystemOperation
        {
            //If operation is for reading host filesystems or internal redfs filesystem
            public string operation;
            public string path;

            public LinkedList<string> fileBackupPaths;
            public LinkedList<string> directoryBackupPaths;

            //for backup tasks.
            public int backupTaskId;
            public string backupJobName;

            public HostSystemOperation()
            {

            }
        }

    public class FileDirItem
    {
        public string name;
        public string type;

        public FileDirItem(string n, string t)
        {
            name = n;
            type = t;
        }
    }

    public class webFileBrowserRequest
    {
        public string path;
    }

    public class REDFSSystemOperation
    {

        public string operation;
        public string path;
        public int fsid;

        public string sourceFileOrDirPath = "";
        public string destinationFileOrDirPath = "";

        public LinkedList<FileDirItem> list = new LinkedList<FileDirItem>();

        public REDFSSystemOperation()
        {


        }

        public void PopulateResult()
        {
            if (operation == "no-load")
            {
                //do nothing, send empty list.
                list.Clear();
            }
            else if (operation == "list")
            {
                IList<DokanNet.FileInformation> flist = REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[fsid].FindFilesWithPattern(path, "*");

                foreach (DokanNet.FileInformation f in flist)
                {
                    int index = f.FileName.LastIndexOf(".");
                    string extension = f.FileName.Substring(index + 1, f.FileName.Length - index - 1);
                    string type = f.Attributes.HasFlag(FileAttributes.Directory) ? "folder" : extension;
                    list.AddLast(new FileDirItem(f.FileName, type));
                }
            }
        }
    }

    //Outgoing object from the server (this program) side.
    public class ListOfFolderContents
        {
            public string parent;
            public LinkedList<string> files;
            public LinkedList<string> directories;
            public string result;
        }

        //static class for checking if file exist etc.
        public static class HostOSFileSystem
        {
            public static void IsFileOrDirectory(string checkpath, out Boolean pathExists, out Boolean pathIsFile)
            {
                if (File.Exists(checkpath))
                {
                    pathIsFile = true;
                    pathExists = true;
                }
                else if (Directory.Exists(checkpath))
                {
                    pathIsFile = false;
                    pathExists = true;
                }
                else
                {
                    pathExists = false;
                    pathIsFile = false;
                }
            }

            public static void ChunkExistsInPath(string checkpath, out Boolean pathExists, out int numSegments)
            {

                if (File.Exists(checkpath))
                {
                    long length = new System.IO.FileInfo(checkpath).Length;
                    numSegments = (int)(length / (1024 * 1024 * 1024));
                    pathExists = true;
                }
                else
                {
                    pathExists = false;
                    numSegments = 0;
                }
            }

            public static ListOfFolderContents getDirectoryContents(string path)
            {
            try
            {
                if (path == "")
                {
                    ListOfFolderContents reply = new ListOfFolderContents();
                    reply.directories = new LinkedList<string>();
                    reply.files = new LinkedList<string>();
                    reply.directories.AddLast("My Computer");
                    reply.result = "SUCCESS";
                    reply.parent = path;
                    return reply;
                }
                else if (path == "My Computer")
                {
                    DriveInfo[] drives = DriveInfo.GetDrives();
                    Console.WriteLine("Detected Drives: ");

                    ListOfFolderContents reply = new ListOfFolderContents();
                    reply.directories = new LinkedList<string>();
                    reply.files = new LinkedList<string>();

                    for (int i = 0; i < drives.Length; i++)
                    {
                        reply.directories.AddLast(drives[i].Name);
                    }
                    reply.result = "SUCCESS";
                    reply.parent = path;
                    return reply;
                }
                else
                {
                    string[] darray = Directory.GetDirectories(path);
                    string[] farray = Directory.GetFiles(path);

                    ListOfFolderContents reply = new ListOfFolderContents();
                    reply.result = "SUCCESS";

                    reply.directories = new LinkedList<string>();
                    reply.files = new LinkedList<string>();
                    foreach (string f in farray)
                    {
                        reply.files.AddLast(f);
                    }
                    foreach (string d in darray)
                    {
                        reply.directories.AddLast(d);
                    }
                    reply.parent = path;
                    return reply;
                }
            }
            catch (Exception e)
            {
                ListOfFolderContents reply = new ListOfFolderContents();
                reply.result = "FAILED";
                return reply;
            }
        }
    }
}
