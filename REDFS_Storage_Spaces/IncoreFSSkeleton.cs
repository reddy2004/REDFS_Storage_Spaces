using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.AccessControl;
using DokanNet;
using DokanNet.Logging;
using static DokanNet.FormatProviders;
using FileAccess = DokanNet.FileAccess;
using System.Text;
using System.Collections;
using System.Threading;

namespace REDFS_ClusterMode
{
    public class IncoreFSSkeleton : IDokanOperations
    {
        private const FileAccess DataAccess = FileAccess.ReadData | FileAccess.WriteData | FileAccess.AppendData |
                                              FileAccess.Execute |
                                              FileAccess.GenericExecute | FileAccess.GenericWrite |
                                              FileAccess.GenericRead;

        private const FileAccess DataWriteAccess = FileAccess.WriteData | FileAccess.AppendData |
                                                   FileAccess.Delete |
                                                   FileAccess.GenericWrite;

        public REDFSTree rootDirectory;// = new REDFSTree();

        long logicalData = 0;
        long physicalData = 0;

        //Just return some default security attributes.
        FileSystemSecurity fSecurity;
        FileSystemSecurity dSecurity;

        public IncoreFSSkeleton(REDFSTree r)
        {
            fSecurity = new FileInfo(@"Data/fSecurity.txt").GetAccessControl();
            dSecurity = new DirectoryInfo(@"Data/dSecurity").GetAccessControl();
            DokanSideMetrics.init();
            rootDirectory = r;
        }

        void IDokanOperations.Cleanup(string fileName, IDokanFileInfo info)
        {
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.DOKAN_CALLS, 1);
            if (info.Context != null)
            {
                //Cleanup any memory
                info.Context = null;
            }

            if (info.DeleteOnClose)
            {
                if (info.IsDirectory)
                {
                    rootDirectory.DeleteDirectoryInCleanup(fileName);
                }
                else
                {
                    rootDirectory.DeleteFile(fileName);
                }
            }
        }

        void IDokanOperations.CloseFile(string fileName, IDokanFileInfo info)
        {
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.DOKAN_CALLS, 1);
            if (info.Context != null)
            {
                //cleanup
            }
            info.Context = null;
        }

        NtStatus IDokanOperations.CreateFile(string fileName, FileAccess access, FileShare share, FileMode mode, FileOptions options, FileAttributes attributes, IDokanFileInfo info)
        {
            var filePath = fileName;
            var result = DokanResult.Success;
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.DOKAN_CALLS, 1);

            if (fileName == "")
            {
                return DokanNet.DokanResult.Success;
            }

            if (fileName == "\\")
            {
                //Always return success there, we just set the info object.

                //Just sync statements prolly, Just return sucess as the rootdir is always present.
                if ((access & FileAccess.Synchronize) == FileAccess.Synchronize)
                {
                    //DEFS.ASSERT(info.IsDirectory == true, "Windows must already know that this is a dir, so calling with synchronize");
                    if (info != null && info.Context == null)
                    {
                        info.Context = new object();
                    }
                    return DokanNet.DokanResult.Success;
                }

                /*
                 * This is one more call to \\ rootdir
                 *  DGB:    CreateFile      \
                 *  DGB:    CreateFile      FileAccess = ReadAttributes
                    DGB:    CreateFile      FileShare = ReadWrite, Delete
                    DGB:    CreateFile      FileMode = Open
                    DGB:    CreateFile      FileOptions = None
                    DGB:    CreateFile      FileAttributes = 0
                    DGB:    CreateFile      IDokanFileInfo = {<null>, False, False, False, False, #3500, False, False}
                */
                if ((access & FileAccess.ReadAttributes) == FileAccess.ReadAttributes)
                {
                    if (info != null && info.Context == null)
                    {
                        info.Context = new object();
                    }
                    return DokanNet.DokanResult.Success;
                }

                /*  Right click on drive from windows explorer.
                    DGB:    CreateFile      \
                    DGB:    CreateFile      FileAccess = ReadPermissions, ChangePermissions, SetOwnership, AccessSystemSecurity
                    DGB:    CreateFile      FileShare = ReadWrite
                    DGB:    CreateFile      FileMode = Open
                    DGB:    CreateFile      FileOptions = None
                    DGB:    CreateFile      FileAttributes = 0
                    DGB:    CreateFile      IDokanFileInfo = {<null>, False, False, False, False, #8904, False, False}
                    ASSERT :: Unknown CreateFile for \
                    DGB:    ASSERT  Unknown CreateFile for \
                    As there is nothing we can do, lets return sucess. This can be merged with above two ifs
                 */
                return DokanNet.DokanResult.Success;
            }
            else
            {
                if (info.IsDirectory)
                {
                    try
                    {
                        switch (mode)
                        {
                            case FileMode.Open:
                                if (!rootDirectory.DirectoryExists(filePath))
                                {
                                    if (rootDirectory.FileExists(filePath))
                                    {
                                        return DokanResult.NotADirectory;
                                    }
                                    return DokanResult.PathNotFound;
                                }

                                //Verify that this is needed? Since we have already validated directory exists
                                //rootDirectory.CreateDirectory(filePath);
                                break;

                            case FileMode.CreateNew:
                                if (rootDirectory.DirectoryExists(filePath))
                                {
                                    return DokanResult.AlreadyExists; //or DokanResult.FileExists;?
                                }

                                rootDirectory.CreateDirectory(filePath);
                                break;
                        }
                    }
                    catch (UnauthorizedAccessException)
                    {
                            return DokanResult.AccessDenied;
                    }
                }
                else
                {
                    var pathExists = true;
                    var pathIsDirectory = false;

                    var readWriteAttributes = (access & DataAccess) == 0;
                    var readAccess = (access & DataWriteAccess) == 0;

                    var dirExists = rootDirectory.DirectoryExists(filePath);
                    var fileExists = rootDirectory.FileExists(filePath);

                    pathExists = ( dirExists || fileExists );
                    pathIsDirectory = pathExists ? rootDirectory.DirectoryExists(filePath) : false;

                    switch (mode)
                    {
                        case FileMode.Open:

                            if (pathExists)
                            {
                                // check if driver only wants to read attributes, security info, or open directory
                                if (readWriteAttributes || pathIsDirectory)
                                {
                                    if (pathIsDirectory && (access & FileAccess.Delete) == FileAccess.Delete
                                        && (access & FileAccess.Synchronize) != FileAccess.Synchronize)
                                        //It is a DeleteFile request on a directory
                                        return DokanResult.AccessDenied;

                                    info.IsDirectory = pathIsDirectory;
                                    info.Context = new object();
                                    // must set it to something if you return DokanError.Success
                                    return DokanNet.DokanResult.Success;
                                }
                            }
                            else
                            {
                                return DokanNet.DokanResult.PathNotFound;
                            }
                            break;

                        case FileMode.CreateNew:
                            if (pathExists)
                            {
                                if (info.Context == null)
                                {
                                    info.Context = new object();
                                }
                                return DokanNet.DokanResult.FileExists;
                            }
                            break;

                        case FileMode.Truncate:
                            if (!pathExists)
                            {
                                if (info.Context == null)
                                {
                                    info.Context = new object();
                                }
                                return DokanNet.DokanResult.FileNotFound;
                            }
                            break;
                    }

                    //Now we have to create the file if the path does not exist
                    try
                    {
                        //From here all paths are sort of sucess paths that the path exists or we create it. So set the context
                        //info.IsDirectory = pathIsDirectory;
                        //info.Context = new object();

                        if (pathExists && (mode == FileMode.OpenOrCreate
                                           || mode == FileMode.Create))
                        {
                            result = DokanResult.AlreadyExists;
                        }
                        else if (pathExists && (mode == FileMode.Open))
                        {
                            result = DokanNet.NtStatus.Success;
                        }
                        else
                        {
                            //We could come here when new folder is created. Ex "New Folder (1)" and we end up creating a file instead.
                           // rootDirectory.CreateFile(filePath);
                        }

                        bool fileCreated = mode == FileMode.CreateNew || mode == FileMode.Create || (!pathExists && mode == FileMode.OpenOrCreate);
                        if (fileCreated)
                        {
                            rootDirectory.CreateFile(filePath);

                            FileAttributes new_attributes = attributes;
                            new_attributes |= FileAttributes.Normal; // Files are always created as Archive
                                                                      // FILE_ATTRIBUTE_NORMAL is override if any other attribute is set.
                            //new_attributes &= ~FileAttributes.Normal;
                            //File.SetAttributes(filePath, new_attributes);
                        }
                    }
                    catch (UnauthorizedAccessException) // don't have access rights
                    {
                        return DokanResult.AccessDenied;
                    }
                    catch (DirectoryNotFoundException)
                    {
                        return DokanResult.PathNotFound;
                    }
                    catch (Exception ex)
                    {
                        return DokanResult.SharingViolation;
                    }

                    return result;
                }
                return result;
            }
        }

        NtStatus IDokanOperations.DeleteDirectory(string fileName, IDokanFileInfo info)
        {
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.DOKAN_CALLS, 1);
            return rootDirectory.DeleteDirectory(fileName) ? DokanNet.NtStatus.Success : DokanNet.NtStatus.Error;
        }

        NtStatus IDokanOperations.DeleteFile(string fileName, IDokanFileInfo info)
        {
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.DOKAN_CALLS, 1);
            return rootDirectory.DeleteFile(fileName) ? DokanNet.NtStatus.Success : DokanNet.NtStatus.Error;
        }

        NtStatus IDokanOperations.FindFiles(string fileName, out IList<FileInformation> files, IDokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        NtStatus IDokanOperations.FindFilesWithPattern(string dirPath, string searchPattern, out IList<FileInformation> files, IDokanFileInfo info)
        {
            //Since this is called all the time!
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.LOGICAL_DATA, logicalData);
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.PHYSICAL_DATA, physicalData);
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.DOKAN_CALLS, 1);

            if (rootDirectory.DirectoryExists(dirPath))
            {
                files = rootDirectory.FindFilesWithPattern(dirPath, searchPattern);
                return DokanResult.Success;
            }
            else
            {
                throw new UnauthorizedAccessException();
            }
        }

        NtStatus IDokanOperations.FindStreams(string fileName, out IList<FileInformation> streams, IDokanFileInfo info)
        {
            streams = null;
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.DOKAN_CALLS, 1);
            return NtStatus.NotImplemented;
        }

        NtStatus IDokanOperations.FlushFileBuffers(string fileName, IDokanFileInfo info)
        {
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.DOKAN_CALLS, 1);
            return rootDirectory.FlushFileBuffers(fileName) ? DokanNet.NtStatus.Success : DokanNet.NtStatus.Error;
        }

        NtStatus IDokanOperations.GetDiskFreeSpace(out long freeBytesAvailable, out long totalNumberOfBytes, out long totalNumberOfFreeBytes, IDokanFileInfo info)
        {
            freeBytesAvailable = 1024*1024*1024;
            totalNumberOfBytes = 1024 * 1024 * 1024;
            totalNumberOfFreeBytes = 1024 * 1024 * 1024;
            return DokanResult.Success;
        }

        NtStatus IDokanOperations.GetFileInformation(string fileName, out FileInformation fileInfo, IDokanFileInfo info)
        {
            //FileSystemInfo finfo = new FileInfo();
            //FileSystemInfofinfo finfo  = new DirectoryInfo()
            if (fileName == "\\")
            {
                fileInfo = rootDirectory.getFileInformationOfRootDir();
            } 
            else
            {
                fileInfo = rootDirectory.GetFileInformationStruct(fileName);
            }
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.DOKAN_CALLS, 1);
            return DokanResult.Success;
        }

        NtStatus IDokanOperations.GetFileSecurity(string fileName, out FileSystemSecurity security, AccessControlSections sections, IDokanFileInfo info)
        {
            if (info.IsDirectory == true) { security = dSecurity; } else { security = fSecurity; }
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.DOKAN_CALLS, 1);
            return DokanNet.DokanResult.Success;
        }

        NtStatus IDokanOperations.GetVolumeInformation(out string volumeLabel, out FileSystemFeatures features, out string fileSystemName, out uint maximumComponentLength, IDokanFileInfo info)
        {
            volumeLabel = "REDFS_Incore";
            fileSystemName = "REDFS";
            maximumComponentLength = 256;

            features = FileSystemFeatures.CasePreservedNames | FileSystemFeatures.CaseSensitiveSearch |
                       FileSystemFeatures.SupportsRemoteStorage | FileSystemFeatures.UnicodeOnDisk;

            DokanSideMetrics.m.InsertMetric(METRIC_NAME.DOKAN_CALLS, 1);
            return DokanNet.NtStatus.Success;
        }

        NtStatus IDokanOperations.LockFile(string fileName, long offset, long length, IDokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        NtStatus IDokanOperations.Mounted(IDokanFileInfo info)
        {
            //Callback after the disk is mounted. Do any mount side actions here.
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.DOKAN_CALLS, 1);
            return DokanResult.Success;
        }

        NtStatus IDokanOperations.MoveFile(string oldName, string newName, bool replace, IDokanFileInfo info)
        {
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.DOKAN_CALLS, 1);
            if (rootDirectory.MoveInode(null, oldName, newName, replace, info.IsDirectory))
            {
                return DokanResult.Success;
            }
            else
            {
                return DokanNet.NtStatus.AccessDenied;
            }    
        }

        NtStatus IDokanOperations.ReadFile(string fileName, byte[] buffer, out int bytesRead, long offset, IDokanFileInfo info)
        {
            DokanNet.NtStatus ret;
            DokanSideMetrics.m.StartMetric(METRIC_NAME.READ_FROM_FILE, (int)offset);
            if (rootDirectory.ReadFile(fileName, buffer, out bytesRead, offset))
            {
                ret = DokanNet.NtStatus.Success;
            }
            else
            {
                ret = DokanNet.NtStatus.Error;
            }
            DokanSideMetrics.m.StopMetric(METRIC_NAME.READ_FROM_FILE, (int)offset);
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.READ_KILOBYTES, bytesRead);
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.DOKAN_CALLS, 1);
            return ret;
        }

        NtStatus IDokanOperations.SetAllocationSize(string fileName, long length, IDokanFileInfo info)
        {
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.DOKAN_CALLS, 1);
            if (rootDirectory.SetAllocationSize(fileName, length, info.IsDirectory))
            {
                return DokanResult.Success;
            }
            else
            {
                return DokanResult.DiskFull;
            }
        }

        NtStatus IDokanOperations.SetEndOfFile(string fileName, long length, IDokanFileInfo info)
        {
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.DOKAN_CALLS, 1);
            return rootDirectory.SetEndOfFile(fileName, length, false) ? DokanNet.NtStatus.Success : DokanNet.NtStatus.Error;
        }

        NtStatus IDokanOperations.SetFileAttributes(string fileName, FileAttributes attributes, IDokanFileInfo info)
        {
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.DOKAN_CALLS, 1);
            return rootDirectory.SetFileAttributes(fileName, attributes) ? DokanNet.NtStatus.Success : DokanNet.NtStatus.Error;
        }

        NtStatus IDokanOperations.SetFileSecurity(string fileName, FileSystemSecurity security, AccessControlSections sections, IDokanFileInfo info)
        {
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.DOKAN_CALLS, 1);
            return DokanResult.NotImplemented;
        }

        NtStatus IDokanOperations.SetFileTime(string fileName, DateTime? creationTime, DateTime? lastAccessTime, DateTime? lastWriteTime, IDokanFileInfo info)
        {
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.DOKAN_CALLS, 1);
            return rootDirectory.SetFileTime(fileName, creationTime, lastAccessTime, lastWriteTime) ? DokanNet.NtStatus.Success : DokanNet.NtStatus.Error;
        }

        NtStatus IDokanOperations.UnlockFile(string fileName, long offset, long length, IDokanFileInfo info)
        {
            return DokanNet.NtStatus.NotImplemented;
        }

        NtStatus IDokanOperations.Unmounted(IDokanFileInfo info)
        {
            /*
             * XXX Todo, flush out all data to disk immediately
             */
            return DokanNet.NtStatus.Success;
        }

        NtStatus IDokanOperations.WriteFile(string fileName, byte[] buffer, out int bytesWritten, long offset, IDokanFileInfo info)
        {
            DokanSideMetrics.m.InsertMetric(METRIC_NAME.DOKAN_CALLS, 1);

            logicalData += buffer.Length;
            physicalData += buffer.Length / 2;

            DokanSideMetrics.m.StartMetric(METRIC_NAME.WRITE_TO_FILE, (int)offset);
            NtStatus ret = rootDirectory.WriteFile(fileName, buffer, out bytesWritten, offset) ? DokanNet.NtStatus.Success : DokanNet.NtStatus.Error;
            DokanSideMetrics.m.StopMetric(METRIC_NAME.WRITE_TO_FILE, (int)offset);

            DokanSideMetrics.m.InsertMetric(METRIC_NAME.WRITE_KILOBYTES, bytesWritten);
            return ret;
        }
    }
}
