using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace REDFS_ClusterMode
{
    /*
     * All operations must verify this cookie list. Cookie list will be cleared
     * if a container is unmounted, or if *any* user logs out. Since we mount only 
     * one container, if two browsers are seeing the same container, then one logout
     * will logout the other as well.
     */ 
    public class LoggedInCookies
    {
        public string cookie;
        public DateTime cookieTime;
    }

    public class ContainerStatus
    {
        public string mounted;
        public List<string> all;
    }

    public class Auth
    {
        public string userName;
        public string passWord;
    }

    public class ContainerObject
    {
        public string containerName;
        public string containerPath;
        public string containerDescription;
        public List<Auth> auth = new List<Auth>();
    }

    public static class REDFS
    {
        public static REDFSContainer redfsContainer = null;
        public static List<ContainerObject> availableContainers = new List<ContainerObject>();
        public static string defaultPath = @"C:\Users\vikra\Documents\REDFS\Global\containers.json";

        public static List<LoggedInCookies> loggedCookies = new List<LoggedInCookies>();

        static REDFS()
        {
            string docFolder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);
            defaultPath = docFolder + "\\REDFS\\";

            try
            {
                using (StreamReader sr = new StreamReader(defaultPath + "Global\\containers.json"))
                {
                    string line;
                    while ((line = sr.ReadLine()) != null)
                    {
                        //Console.WriteLine(line);
                        ContainerObject b2 = JsonConvert.DeserializeObject<ContainerObject>(line);
                        availableContainers.Add(b2);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("The containers file could not be read:");
                Console.WriteLine(e.Message);
            }
        }

        public static void AddNewCookie(string cookie)
        {
            LoggedInCookies lc = new LoggedInCookies();
            lc.cookie = cookie;
            lc.cookieTime = DateTime.UtcNow;
            loggedCookies.Add(lc);
        }

        public static Boolean DoesCookieExist(string cookie)
        {
            foreach (var ct in loggedCookies)
            {
                //long elapsed = (DateTime.UtcNow.Ticks - ct.cookieTime.Ticks);
                //long millisInDay = 1000 * 60 * 60 * 24;
                if (ct.cookie == cookie)
                {
                    return true;
                }
            }
            return false;
        }

        public static ContainerStatus GetContainerNames()
        {
            ContainerStatus cs = new ContainerStatus();
            cs.mounted = (redfsContainer == null) ? "" : redfsContainer.containerName;
            cs.all = new List<string>();

            foreach (var ct in availableContainers)
            {
                cs.all.Add(ct.containerName);
            }
            return cs;
        }

        public static void CreateEmptyChunksAndVolumesFile(string containerFolderPath)
        {
            string volumesFile = containerFolderPath + "\\volumes.json";

            Directory.CreateDirectory(containerFolderPath);
            File.Create(volumesFile).Dispose();
            REDFSContainer.CreateBaseChunkFileForNewContainer(containerFolderPath);
        }

        public static Boolean CreateNewContainer(string cname, string cdescription, string uname, string upassword) 
        {
            foreach (var ct in availableContainers)
            {
                if (ct.containerName == cname)
                {
                    return false;
                }
            }
            ContainerObject co = new ContainerObject();
            co.containerName = cname;
            co.containerDescription = cdescription;
            co.containerPath = defaultPath + cname;
            Auth au = new Auth();
            au.userName = uname;
            au.passWord = upassword;
            co.auth.Add(au);

            AddNewContainer(co);

            return true;
        }

        public static void AddNewContainer(ContainerObject cont)
        {
            try
            {
                availableContainers.Add(cont);

                using (StreamWriter sw = new StreamWriter(defaultPath + "Global\\containers.json"))
                {
                    foreach (var ct in availableContainers)
                    {
                        String vstr = JsonConvert.SerializeObject(ct, Formatting.None);
                        Console.WriteLine(vstr);
                        sw.WriteLine(vstr);

                        //Also create the folder.
                        Directory.CreateDirectory(defaultPath + "\\" + cont.containerName);
                        
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("The file could not be read: Data/containers.json" );
                Console.WriteLine(e.Message);
            }
            //Dir is created Now lets create an empty volumes.json and chunks.file
            CreateEmptyChunksAndVolumesFile(cont.containerPath);
        }

        public static Boolean MountContainer(bool isTest, string containerName)
        {
            if (isTest)
            {
                UnmountContainer();
            }

            if (redfsContainer == null)
            {
                try
                {
                    foreach (var ct in availableContainers)
                    {
                        //Console.WriteLine("compare  " + containerName + " vs " + ct.containerName);
                        if (ct.containerName == containerName)
                        {
                            Console.WriteLine("Mounting container " + containerName);
                            redfsContainer = new REDFSContainer(containerName, ct.containerPath);
                            redfsContainer.InitContainer();
                            return true;
                        }
                    }
                    return false;
                } 
                catch (Exception e)
                {
                    Console.WriteLine("Shitty exception e " + e.Message);
                    return false;
                }
            }
            else
            {
                throw new SystemException("There is a container already mounted! " + redfsContainer.containerName);
            }
        }

        public static Boolean UnmountContainer()
        {
            if (redfsContainer != null)
            {
                redfsContainer.FlushAndWrapUp();
                redfsContainer = null;
                loggedCookies = new List<LoggedInCookies>();
                return true;
            }
            return false;
            //throw new SystemException();
        }

        public static Boolean isContainerMounted(out string containerName)
        {
            if (redfsContainer == null)
            {
                containerName = "";
                return false;
            }
            else
            {
                containerName = redfsContainer.containerName;
                return true;
            }
        }

        public static string getInterpretedContainerPath(string containerName)
        {
            string docFolder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);
            return (docFolder + "\\REDFS\\" + containerName);
        }

        public static string getAbsoluteContainerPath()
        {
            if (redfsContainer == null)
            {
                return "";
            }
            else
            {
                string docFolder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);
                return (docFolder + "\\REDFS\\" + redfsContainer.containerName);
            }
        }

        public static Boolean CleanupTestContainer(string containerName)
        {
            if (redfsContainer == null)
            {
                foreach (var ct in availableContainers)
                {
                    //Console.WriteLine("compare  " + containerName + " vs " + ct.containerName);
                    if (ct.containerName == containerName)
                    {
                        Console.WriteLine("Cleaning up container " + containerName);
                        Directory.Delete(ct.containerPath, true);
                        availableContainers.Remove(ct);

                        using (StreamWriter sw = new StreamWriter(defaultPath + "Global\\containers.json"))
                        {
                            foreach (var ct1 in availableContainers)
                            {
                                String vstr = JsonConvert.SerializeObject(ct1, Formatting.None);
                                sw.WriteLine(vstr);
                            }
                        }
                        return true;
                    }
                }
                return false;
            }
            else
            {
                throw new SystemException("There is a container already mounted! " + redfsContainer.containerName + ", First unmount any container before cleaning up test containers");
            }
        }

        public static string getMountedContainer()
        {
            if (redfsContainer == null)
            {
                return "";
            }
            else
            {
                return redfsContainer.containerName;
            }
        }
        public static Boolean verifyAndLoadContainer(string containerName1, string username, string password)
        {
            if (getMountedContainer() != "")
            {
                return false;
            }
            else
            {
                foreach (var ct in availableContainers)
                {
                    if (ct.containerName == containerName1)
                    {
                        foreach (var auth in ct.auth)
                        {
                            if (auth.userName == username && auth.passWord == password)
                            {
                                MountContainer(false, containerName1);
                                return true;
                            }
                        }
                    }
                }
                return false;
            }
        }
    }
}
