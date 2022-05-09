using System;
using System.IO;
using System.Text;
using System.Net;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Threading;

namespace REDFS_ClusterMode
{
    public class GenericSuccessReply
    {
        public string result = "SUCCESS";
        public string message;
        public string cookie;
        public string connectedFrom;
    }

    public class GenericFailureReply
    {
        public string result = "FAILED";
        public string message;
    }

    public class HttpServer
    {
        
        public HttpListener listener;
        public string url = "http://localhost:8000/";
        public int pageViews = 0;
        public int requestCount = 0;

        public HttpServer(HttpListener la)
        {
            listener = la;
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

        public string ExtractREDFSCookie(HttpListenerRequest request)
        {
            foreach (Cookie cook in request.Cookies)
            {
                if (cook.Name == "redfs")
                {
                    return cook.Value;
                } 
            }
            return "";
        }

        public string CreateNewCookie()
        {
            String timeStamp = GetTimestamp(DateTime.Now);
            return CalculateMD5Hash(timeStamp);
        }

        public ListOfFolderContents getContentsOfFolder(string path)
        {
            return HostOSFileSystem.getDirectoryContents(path);
        }

        public async Task HandleIncomingConnections()
        {
            bool runServer = true;

            Console.WriteLine("... handling incoming connections....");
            string docFolder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);

            // While a user hasn't visited the `shutdown` url, keep on handling requests
            while (runServer)
            {
                // Will wait here until we hear from a connection
                HttpListenerContext ctx = await listener.GetContextAsync();

                if (ctx == null)
                {
                    continue;
                }
                // Peel out the requests and response objects
                HttpListenerRequest req = ctx.Request;
                HttpListenerResponse resp = ctx.Response;

                // Print out some info about the request
                /*
                Console.WriteLine("Request #: {0}", ++requestCount);
                Console.WriteLine(req.Url.ToString());
                Console.WriteLine(req.HttpMethod);
                Console.WriteLine(req.UserHostName);
                Console.WriteLine(req.UserAgent);
                Console.WriteLine("Query: {0}", req.QueryString);
                */
                Logger.LOG("HTTP-" + req.HttpMethod, req.Url.ToString());

                string cookie = ExtractREDFSCookie(req);
                bool isUserValid = REDFS.DoesCookieExist(cookie);
                cookie = CreateNewCookie();

                // If `shutdown` url requested w/ POST, then shutdown the server after serving the page
                if ((req.HttpMethod == "GET") && (req.Url.AbsolutePath == "/shutdown"))
                {
                    Console.WriteLine("Shutdown requested");
                    if (!isUserValid)
                    {
                        REDFS.UnmountContainer();
                        /*
                        if (REDFS.redfsContainer != null)
                        {
                            REDFS.redfsContainer.FlushAndWrapUp();
                            REDFS.redfsContainer = null;
                        }*/
                        resp.ContentType = "application/json";
                        resp.ContentEncoding = Encoding.UTF8;
                        runServer = false;
                        string json = JsonConvert.SerializeObject(new GenericSuccessReply(), Formatting.Indented);
                        byte[] data = Encoding.UTF8.GetBytes(json);
                        resp.ContentLength64 = data.LongLength;
                        resp.OutputStream.Write(data);
                        resp.Close();
                        Thread.Sleep(2000);
                        System.Environment.Exit(1);
                    }
                }

                // Make sure we don't increment the page views counter if `favicon.ico` is requested
                if (req.Url.AbsolutePath == "/favicon.ico")
                {
                    resp.Close();
                    continue;
                }
                pageViews += 1;

                if ((req.HttpMethod == "GET") && (req.Url.AbsolutePath == "/allvolumelist"))
                {
                    if (isUserValid)
                    {
                        if (REDFS.redfsContainer != null)
                        {
                            VolumeManager volumeManager = REDFS.redfsContainer.volumeManager;

                            String jsonData = volumeManager.GetVolumeJSONList();
                            byte[] data = Encoding.UTF8.GetBytes(jsonData);
                            await resp.OutputStream.WriteAsync(data, 0, data.Length);
                        }
                    }
                    resp.Close();
                }
                else if ((req.HttpMethod == "GET") && (req.Url.AbsolutePath == "/containerOpsStatus"))
                {
                    if (isUserValid)
                    {
                        string containerName;
                        if (REDFS.isContainerMounted(out containerName))
                        {
                            REDFS.redfsContainer.containerOperations.updateProgress();

                            String jsonData = JsonConvert.SerializeObject(REDFS.redfsContainer.containerOperations, Formatting.Indented);
                            byte[] data = Encoding.UTF8.GetBytes(jsonData);
                            await resp.OutputStream.WriteAsync(data, 0, data.Length);
                        }
                    }
                    resp.Close();
                }
                else if ((req.HttpMethod == "GET") && (req.Url.AbsolutePath == "/allmetrics"))
                {
                    if (isUserValid)
                    {
                        String jsonData = DokanSideMetrics.GetJSONDump();
                        //String jsonData = TestMetrics.GetJSONDump();
                        byte[] data = Encoding.UTF8.GetBytes(jsonData);
                        await resp.OutputStream.WriteAsync(data, 0, data.Length);
                    }
                    resp.Close();
                }
                else if ((req.HttpMethod == "GET") && (req.Url.AbsolutePath == "/getKnownContainers"))
                {
                    ContainerStatus list = REDFS.GetContainerNames();
                    String jsonData = JsonConvert.SerializeObject(list, Formatting.None);
                    byte[] data = Encoding.UTF8.GetBytes(jsonData);
                    await resp.OutputStream.WriteAsync(data, 0, data.Length);
                    resp.Close();
                }
                else if ((req.HttpMethod == "GET") && (req.Url.AbsolutePath == "/getKnownBackTasks"))
                {
                    if (isUserValid)
                    {
                        BackupTaskManager manager = REDFS.redfsContainer.GetBackupTaskManager();

                        String jsonData = JsonConvert.SerializeObject(manager, Formatting.None);
                        byte[] data = Encoding.UTF8.GetBytes(jsonData);
                        await resp.OutputStream.WriteAsync(data, 0, data.Length);
                    }
                    resp.Close();
                }
                else if ((req.HttpMethod == "GET") && req.Url.AbsolutePath.IndexOf("/validate") == 0)
                {
                    string userName = req.QueryString["uname"];
                    string passWord = req.QueryString["upassword"];
                    string container = req.QueryString["container"];

                    //Authenticate and respond, send cookie only in success case
                    if (REDFS.getMountedContainer() == "" || REDFS.getMountedContainer() == container)
                    {
                        if (REDFS.verifyAndLoadContainer(container, userName, passWord))
                        {
                            REDFS.AddNewCookie(cookie);
                            GenericSuccessReply rp = new GenericSuccessReply();
                            rp.cookie = cookie;
                            rp.connectedFrom = req.RemoteEndPoint.Address.ToString() + ":" + req.RemoteEndPoint.Port.ToString();
                            string json = JsonConvert.SerializeObject(rp, Formatting.Indented);
                            byte[] data = Encoding.UTF8.GetBytes(json);
                            resp.OutputStream.Write(data);
                        }
                        else
                        {
                            string json = JsonConvert.SerializeObject(new GenericFailureReply(), Formatting.Indented);
                            byte[] data = Encoding.UTF8.GetBytes(json);
                            resp.OutputStream.Write(data);
                        }
                    }
                    else
                    {
                        string json = JsonConvert.SerializeObject(new GenericFailureReply(), Formatting.Indented);
                        byte[] data = Encoding.UTF8.GetBytes(json);
                        resp.OutputStream.Write(data);
                    }
                    resp.Close();
                }
                else if ((req.HttpMethod == "GET") && req.Url.AbsolutePath.IndexOf("/newContainer") == 0)
                {
                    string userName = req.QueryString["uname"];
                    string passWord = req.QueryString["upassword"];
                    string containerName = req.QueryString["cname"];
                    string containerDescription = req.QueryString["cdescription"];

                    if (REDFS.CreateNewContainer(containerName, containerDescription, userName, passWord))
                    {
                        string json = JsonConvert.SerializeObject(new GenericSuccessReply(), Formatting.Indented);
                        byte[] data = Encoding.UTF8.GetBytes(json);

                        //REDFS.redfsContainer.containerOperations.CreateNewOperation(ALLOWED_OPERATIONS.CHUNK_CREATE, "Create Chunking", "Writing", b2.id + "," + b2.path + "," + b2.size);

                        resp.OutputStream.Write(data);
                    }
                    else
                    {
                        string json = JsonConvert.SerializeObject(new GenericFailureReply(), Formatting.Indented);
                        byte[] data = Encoding.UTF8.GetBytes(json);
                        resp.OutputStream.Write(data);
                    }
                    resp.Close();
                }
                else if ((req.HttpMethod == "GET") && req.Url.AbsolutePath.IndexOf("/getAllChunksInContainer") == 0)
                {
                    if (isUserValid)
                    {
                        List<ChunkInfo> chunklist = REDFS.redfsContainer.getChunksInContainer();
                        String jsonData = JsonConvert.SerializeObject(chunklist, Formatting.None);
                        byte[] data = Encoding.UTF8.GetBytes(jsonData);
                        await resp.OutputStream.WriteAsync(data, 0, data.Length);
                    }
                    resp.Close();
                }
                else if ((req.HttpMethod == "POST") && (req.Url.AbsolutePath == "/optimizeStorageOperations"))
                {
                    if (isUserValid)
                    {
                        string request = new StreamReader(ctx.Request.InputStream).ReadToEnd();
                        OperationData b2 = JsonConvert.DeserializeObject<OperationData>(request);

                        switch (b2.OpName)
                        {
                            case "compress":
                                string json = JsonConvert.SerializeObject(new GenericSuccessReply(), Formatting.Indented);
                                byte[] data = Encoding.UTF8.GetBytes(json);
                                await resp.OutputStream.WriteAsync(data, 0, data.Length);
                                REDFS.redfsContainer.containerOperations.CreateNewOperation(ALLOWED_OPERATIONS.COMPRESS_CONTAINER, "Compression", "Compressing","");
                                break;
                            case "dedupe":
                                string json2 = JsonConvert.SerializeObject(new GenericSuccessReply(), Formatting.Indented);
                                byte[] data2 = Encoding.UTF8.GetBytes(json2);
                                await resp.OutputStream.WriteAsync(data2, 0, data2.Length);
                                REDFS.redfsContainer.containerOperations.CreateNewOperation(ALLOWED_OPERATIONS.DEDUPE_CONTAINER, "Dedupe", "Deduping","");
                                break;
                            default:
                                break;
                        }
                    }
                    resp.Close();
                }
                else if ((req.HttpMethod == "POST") && (req.Url.AbsolutePath == "/hostFileSystem"))
                {
                    if (isUserValid)
                    {
                        string request = new StreamReader(ctx.Request.InputStream).ReadToEnd();
                        HostSystemOperation b2 = JsonConvert.DeserializeObject<HostSystemOperation>(request);
                        ListOfFolderContents items = new ListOfFolderContents();
                        items.result = "FAILED";
                        BackupTaskManager manager = REDFS.redfsContainer.GetBackupTaskManager();

                        switch (b2.operation)
                        {
                            case "listContents":
                                items = getContentsOfFolder(b2.path);
                                byte[] data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(items, Formatting.None));
                                await resp.OutputStream.WriteAsync(data, 0, data.Length);
                                break;
                            case "newBackupTask":
                                string backupName = b2.path;
                                manager.CreateNewBackupTask(backupName, b2.fileBackupPaths, b2.directoryBackupPaths);
                                break;
                            case "isFileOrFolder":

                                string checkpath = b2.path;
                                Boolean pathExists;
                                Boolean pathIsFile;
                                HostOSFileSystem.IsFileOrDirectory(checkpath, out pathExists, out pathIsFile);

                                if (pathExists && !pathIsFile)
                                {
                                    byte[] data2 = Encoding.UTF8.GetBytes("{\"pathExists\" : true, \"isFile\" : false}");
                                    await resp.OutputStream.WriteAsync(data2, 0, data2.Length);
                                }
                                else if (pathExists && pathIsFile)
                                {
                                    byte[] data2 = Encoding.UTF8.GetBytes("{\"pathExists\" : true, \"isFile\" : true}");
                                    await resp.OutputStream.WriteAsync(data2, 0, data2.Length);
                                }
                                else
                                {
                                    byte[] data2 = Encoding.UTF8.GetBytes("{\"pathExists\" : false, \"isFile\" : false}");
                                    await resp.OutputStream.WriteAsync(data2, 0, data2.Length);
                                }
                                break;
                            case "deleteBackup":
                                REDFS.redfsContainer.backupManager.DeleteBackupTask(b2.backupTaskId);
                                break;
                            case "runBackupJob":
                                REDFS.redfsContainer.containerOperations.CreateNewOperation(ALLOWED_OPERATIONS.BACKUP_OP, "Backup", "Backing up", b2.backupTaskId + "," + b2.backupJobName);
                                break;
                           default:
                                break;
                        }
                        
                        
                    }
                    resp.Close();
                }
                else if ((req.HttpMethod == "POST") && (req.Url.AbsolutePath == "/addNewChunkForCurrentContainer"))
                {

                    if (isUserValid)
                    {
                        string request = new StreamReader(ctx.Request.InputStream).ReadToEnd();
                        try
                        {
                            // we are overloading the same class
                            ChunkInfo b2 = JsonConvert.DeserializeObject<ChunkInfo>(request);

                            if (REDFS.redfsContainer.AddNewChunkToContainer(false, b2))
                            {
                                string json = JsonConvert.SerializeObject(new GenericSuccessReply(), Formatting.Indented);
                                byte[] data = Encoding.UTF8.GetBytes(json);
                                await resp.OutputStream.WriteAsync(data, 0, data.Length);
                                REDFS.redfsContainer.containerOperations.CreateNewOperation(ALLOWED_OPERATIONS.CHUNK_CREATE, "Create Chunking", "Writing", b2.id + "," + b2.path + "," + b2.size + "," + b2.allowedSegmentTypes);
                            }
                            else
                            {
                                string json = JsonConvert.SerializeObject(new GenericFailureReply(), Formatting.Indented);
                                byte[] data = Encoding.UTF8.GetBytes(json);
                                await resp.OutputStream.WriteAsync(data, 0, data.Length);
                            }
                        }
                        catch (Exception e)
                        {
                            string json = JsonConvert.SerializeObject(new GenericFailureReply(), Formatting.Indented);
                            byte[] data = Encoding.UTF8.GetBytes(json);
                            await resp.OutputStream.WriteAsync(data, 0, data.Length);
                        }
                    }
                    resp.Close();
                }
                else if ((req.HttpMethod == "POST") && (req.Url.AbsolutePath == "/operationsAPI"))
                {
                    if (isUserValid)
                    {
                        string request = new StreamReader(ctx.Request.InputStream).ReadToEnd();

                        //Using same class for simplicyt, id is chunk id and not operation id.
                        //XXX dont confuse with each other.
                        OperationData b2 = JsonConvert.DeserializeObject<OperationData>(request);

                        switch(b2.OpName)
                        {
                            case "delete_operation":
                                string json = "";
                                if (REDFS.redfsContainer.containerOperations.RemoveCompletedOperation(b2.id))
                                {
                                    json = JsonConvert.SerializeObject(new GenericSuccessReply(), Formatting.Indented);

                                } 
                                else
                                {
                                    json = JsonConvert.SerializeObject(new GenericFailureReply(), Formatting.Indented);
                                }
                                byte[] data = Encoding.UTF8.GetBytes(json);
                                await resp.OutputStream.WriteAsync(data, 0, data.Length);
                                break;
                            default:
                                break;
                        }
                    }
                    resp.Close();
                }
                else if ((req.HttpMethod == "POST") && (req.Url.AbsolutePath == "/chunkRemoveOrMoveOrDeleteOperation")) 
                {
                    if (isUserValid)
                    {
                        string request = new StreamReader(ctx.Request.InputStream).ReadToEnd();

                        //Using same class for simplicyt, id is chunk id and not operation id.
                        //XXX dont confuse with each other.
                        OperationData b2 = JsonConvert.DeserializeObject<OperationData>(request);

                        switch (b2.OpName)
                        {
                            case "removal":
                                string json = JsonConvert.SerializeObject(new GenericSuccessReply(), Formatting.Indented);
                                byte[] data = Encoding.UTF8.GetBytes(json);
                                await resp.OutputStream.WriteAsync(data, 0, data.Length);
                                REDFS.redfsContainer.containerOperations.CreateNewOperation(ALLOWED_OPERATIONS.CHUNK_PREPARE_REMOVAL, "Preparing for removal : (" + b2.id + ")", "Removing", "");
                                break;
                            case "move":
                                string json1 = JsonConvert.SerializeObject(new GenericSuccessReply(), Formatting.Indented);
                                byte[] data1 = Encoding.UTF8.GetBytes(json1);
                                await resp.OutputStream.WriteAsync(data1, 0, data1.Length);
                                REDFS.redfsContainer.containerOperations.CreateNewOperation(ALLOWED_OPERATIONS.CHUNK_MOVE, "Moving : (" + b2.id + ")", "Moving", "");
                                break;
                            case "delete":
                                if (REDFS.redfsContainer.RemoveChunkFromContainer(b2.id, false))
                                {
                                    string json2 = JsonConvert.SerializeObject(new GenericSuccessReply(), Formatting.Indented);
                                    byte[] data2 = Encoding.UTF8.GetBytes(json2);
                                    await resp.OutputStream.WriteAsync(data2, 0, data2.Length);
                                }
                                else
                                {
                                    string json2 = JsonConvert.SerializeObject(new GenericFailureReply(), Formatting.Indented);
                                    byte[] data2 = Encoding.UTF8.GetBytes(json2);
                                    await resp.OutputStream.WriteAsync(data2, 0, data2.Length);
                                }
                                break;
                            case "forcedelete":
                                if (REDFS.redfsContainer.RemoveChunkFromContainer(b2.id, true))
                                {
                                    string json2 = JsonConvert.SerializeObject(new GenericSuccessReply(), Formatting.Indented);
                                    byte[] data2 = Encoding.UTF8.GetBytes(json2);
                                    await resp.OutputStream.WriteAsync(data2, 0, data2.Length);
                                }
                                else
                                {
                                    string json2 = JsonConvert.SerializeObject(new GenericFailureReply(), Formatting.Indented);
                                    byte[] data2 = Encoding.UTF8.GetBytes(json2);
                                    await resp.OutputStream.WriteAsync(data2, 0, data2.Length);
                                }
                                break;
                            default:
                                break;

                        }
                    }
                    resp.Close();
                }
                else if ((req.HttpMethod == "POST") && (req.Url.AbsolutePath == "/volumeOperation"))
                {
                    if (isUserValid)
                    {
                        VolumeManager volumeManager = REDFS.redfsContainer.volumeManager;

                        string request = new StreamReader(ctx.Request.InputStream).ReadToEnd();
                        VolumeOperation b2 = JsonConvert.DeserializeObject<VolumeOperation>(request);

                        if (REDFS.redfsContainer.containerOperations.currentlyMountedVolume != 0 && (
                            b2.operation == "delete" || b2.operation == "clone" || b2.operation == "snapshot" || b2.operation == "backedclone"))
                        {
                            GenericFailureReply gfr = new GenericFailureReply();
                            gfr.message = "Cannot perform clones/snapshots or deletes when any volume is mounted!";
                            string json = JsonConvert.SerializeObject(gfr, Formatting.Indented);
                            byte[] data = Encoding.UTF8.GetBytes(json);
                            await resp.OutputStream.WriteAsync(data, 0, data.Length);
                        }
                        else
                        {
                            switch (b2.operation)
                            {
                                case "delete":
                                    volumeManager.DeleteVolume(b2.volumeId);
                                    REDFS.redfsContainer.ReloadAllFSIDs();
                                    break;
                                case "backedclone":
                                    volumeManager.CloneVolume(b2.volumeId, b2.volname);
                                    REDFS.redfsContainer.ReloadAllFSIDs();
                                    break;
                                case "clone":
                                    volumeManager.CloneVolumeRaw(b2.volumeId, b2.volname, b2.volDesc, b2.hexcolor);
                                    REDFS.redfsContainer.ReloadAllFSIDs();
                                    break;
                                case "snapshot":
                                    volumeManager.VolumeSnapshot(b2.volumeId, b2.volname);
                                    REDFS.redfsContainer.ReloadAllFSIDs();
                                    break;
                                case "save":
                                    volumeManager.UpdateVolume(b2.volumeId, b2.volname, b2.volDesc, b2.hexcolor);
                                    break;
                                case "mount":
                                    REDFS.redfsContainer.containerOperations.currentlyMountedVolume = b2.volumeId;
                                    REDFS.redfsContainer.MountVolume((int)b2.volumeId);
                                    //volumeManager.MountVolume(b2.volumeId);
                                    break;
                                case "unmount":
                                    if (REDFS.redfsContainer.containerOperations.currentlyMountedVolume == b2.volumeId)
                                    {
                                        while (REDFS.redfsContainer.containerOperations.StopAllRunningOperations() == false)
                                        {
                                            Thread.Sleep(500);
                                        }
                                        REDFS.redfsContainer.containerOperations.currentlyMountedVolume = 0;
                                        REDFS.redfsContainer.UnMountVolume();
                                    }
                                    break;
                                default:
                                    break;
                            }

                            string json = JsonConvert.SerializeObject(new GenericSuccessReply(), Formatting.Indented);
                            byte[] data = Encoding.UTF8.GetBytes(json);
                            await resp.OutputStream.WriteAsync(data, 0, data.Length);
                        }
                    }
                    resp.Close();
                }
                else if ((req.HttpMethod == "GET") && req.Url.AbsolutePath.IndexOf("/GetInternalDataOfFSID") == 0)
                {
                    int fsid = Int32.Parse(req.QueryString["fsid"]);

                    DBNSegmentSpanMap spanMap = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator.dbnSpanMap;

                    string json = JsonConvert.SerializeObject(spanMap, Formatting.Indented);
                    byte[] data = Encoding.UTF8.GetBytes(json);

                    resp.OutputStream.Write(data);
                    resp.Close();
                }
                else
                {
                    if (req.Url.AbsolutePath == "/config")
                    {
                        string page = isUserValid ? docFolder + "\\REDFS\\Global\\WebUIAssets\\jamaica.html" : docFolder + "\\REDFS\\Global\\WebUIAssets\\login.html";
                        
                        byte[] data = Encoding.UTF8.GetBytes(File.ReadAllText(page));
                        resp.ContentType = "text/html";
                        resp.ContentEncoding = Encoding.UTF8;
                        resp.ContentLength64 = data.LongLength;
                        // Write out to the response stream (asynchronously), then close it
                        await resp.OutputStream.WriteAsync(data, 0, data.Length);
                        resp.Close();
                    }
                    else if (req.Url.AbsolutePath == "/login")
                    {
                        string page = isUserValid ? docFolder + "\\REDFS\\Global\\WebUIAssets\\jamaica.html" : docFolder + "\\REDFS\\Global\\WebUIAssets\\login.html";

                        byte[] data = Encoding.UTF8.GetBytes(File.ReadAllText(page));
                        resp.ContentType = "text/html";
                        resp.ContentEncoding = Encoding.UTF8;
                        resp.ContentLength64 = data.LongLength;
                        // Write out to the response stream (asynchronously), then close it
                        await resp.OutputStream.WriteAsync(data, 0, data.Length);
                        resp.Close();
                    }
                    else if ((req.HttpMethod == "GET") && (req.Url.AbsolutePath == "/slideGraph.js"))
                    {
                        if (isUserValid)
                        {
                            byte[] data = Encoding.UTF8.GetBytes(File.ReadAllText(docFolder + "\\REDFS\\Global\\WebUIAssets\\slideGraph.js"));
                            resp.ContentType = "text/javascript";
                            resp.ContentEncoding = Encoding.UTF8;
                            resp.ContentLength64 = data.LongLength;
                            await resp.OutputStream.WriteAsync(data, 0, data.Length);
                        }
                        resp.Close();
                    }
                    else if (req.Url.AbsolutePath == "/logoutAndUnmount")
                    {
                        if (isUserValid)
                        {
                            //Stop all ongoing operations as well.
                            REDFS.UnmountContainer();
                        }
                        string json = JsonConvert.SerializeObject(new GenericSuccessReply(), Formatting.Indented);
                        byte[] data = Encoding.UTF8.GetBytes(json);
                        resp.OutputStream.Write(data);
                        resp.Close();
                    }
                    else if (req.Url.AbsolutePath == "/loginCtrl.js")
                    {
                        byte[] data = Encoding.UTF8.GetBytes(File.ReadAllText(docFolder + "\\REDFS\\Global\\WebUIAssets\\loginCtrl.js"));
                        resp.ContentType = "text/javascript";
                        resp.ContentEncoding = Encoding.UTF8;
                        resp.ContentLength64 = data.LongLength;
                        // Write out to the response stream (asynchronously), then close it
                        await resp.OutputStream.WriteAsync(data, 0, data.Length);
                        resp.Close();
                    }
                    else if (req.Url.AbsolutePath == "/loader.css")
                    {
                        byte[] data = Encoding.UTF8.GetBytes(File.ReadAllText(docFolder + "\\REDFS\\Global\\WebUIAssets\\loader.css"));
                        resp.ContentType = "text/css";
                        resp.ContentEncoding = Encoding.UTF8;
                        resp.ContentLength64 = data.LongLength;
                        // Write out to the response stream (asynchronously), then close it
                        await resp.OutputStream.WriteAsync(data, 0, data.Length);
                        resp.Close();
                    }
                    else if (req.Url.AbsolutePath == "/clone.png" || req.Url.AbsolutePath == "/justclone.png" || req.Url.AbsolutePath == "/snapshot.png" || 
                        req.Url.AbsolutePath == "/delete.png" || req.Url.AbsolutePath == "/mount.png")
                    {
                        string filename = req.Url.AbsolutePath.Substring(1, req.Url.AbsolutePath.Length-1);
                        try
                        {
                            byte[] data = File.ReadAllBytes(docFolder + "\\REDFS\\Global\\WebUIAssets\\" + filename);
                            if (data.Length > 0)
                            {
                                resp.ContentType = "image/png";
                                resp.ContentEncoding = Encoding.Default;
                                resp.ContentLength64 = data.LongLength;
                                // Write out to the response stream (asynchronously), then close it
                                await resp.OutputStream.WriteAsync(data, 0, data.Length);
                            }
                        } catch (Exception e)
                        {

                        }
                        resp.Close();
                    }
                    else
                    {
                        byte[] data = Encoding.UTF8.GetBytes("504 Error Idiot");
                        resp.ContentType = "text/html";
                        resp.ContentEncoding = Encoding.UTF8;
                        resp.ContentLength64 = data.LongLength;
                        // Write out to the response stream (asynchronously), then close it
                        await resp.OutputStream.WriteAsync(data, 0, data.Length);
                        resp.Close();
                    }
                }
            }
        }

    }
}
