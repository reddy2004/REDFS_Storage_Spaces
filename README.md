# REDFS_Storage_Spaces
A RedFS version similar to Windows storage spaces, with a file system spread over multiple files with support for multiple volumes with cloning, snapshots, dedupe and RAID. I wanted to port my previous Filesystem to a multidrive Filesystem with protection such as mirroring and RAID. Unfortunately, writing a Filesystem is quite a challenge and is very difficult to impliment a lot of interesting features in a hobby level programme. I wanted to do a take on Windows Storage Spaces which allows an user to aggregate disks into a pool and then create regular windows drives in that pool. Depending on your configuration, you could have data protection such as Mirroring or RAID5 for your drives. In REDFS, its not straight forward to do something like this, but anyway I wanted to come up with a solution.

![alt text](https://github.com/reddy2004/REDFS_Storage_Spaces/blob/main/REDFS_Storage_Spaces/Data/Screenshots/wssVersusRedFSSS.png)

With REDFS, you start with a template volume called the Zero Volume. A zero volume is empty and is considered the root of the tree as shown below. When you want to create a new volume/drive, you can clone the Zero Volume and from then on you can clone/snapshot your derieved volume as per your usage. REDFS allows you to clone/snapshot any volume in any arbitrary order. But to enforce some sanity, REDFS allows you to snapshot only the 'LIVE' volumes. If you need to branch off from a snapshot - then you could clone any of the snapshots and modify them. These clones can be snapshotted. Every node in the tree represents a point in time when an operation was done on a volume. i.e clone or snapshot. You can view any snapshot and if you want to modify them, you can take a clone of the volume. All data is only copied on write. i.e COW'd data. Hence your space usage does not double when you snapshot or clone volumes.


![alt text](https://github.com/reddy2004/REDFS_Storage_Spaces/blob/main/REDFS_Storage_Spaces/Data/Screenshots/tree_view.png)

In the webview, clicking on any of the node in the tree shows you the operations that are allowed on the node. Please see the dialog below.


![alt text](https://github.com/reddy2004/REDFS_Storage_Spaces/blob/main/REDFS_Storage_Spaces/Data/Screenshots/vol_options.png)


REDFS Storage Space itself is spread over multiple regular NTFS files potentially located at different places. Since REDFS is a single node, i.e single computer filesystem, it means that all the files managed by REDFS must be accessible from the computer where REDFS is run. The files that are included as part of the REDFS managed storage must be accessible by a windows style path and should be a multiple of 1GB. REDFS spreads data over these files, also called CHUNKS, so that we can achieve data redundancy. For ex. You could include 5 chunk files managed by REDFS. These could be C:\1.dat, D:\1.dat, E:\1.dat, F:\abc.dat, G:\xyz.dat. Since these chunk files are present on 5 different physical drives, REDFS could stripe data on them to achive MIRRORing or say RAID5 to protect data in the volumes.

![alt text](https://github.com/reddy2004/REDFS_Storage_Spaces/blob/main/REDFS_Storage_Spaces/Data/Screenshots/config_main.png)

You can specify what is the speed class of the Chunk when you add it to REDFS. The summary is available in the config tab.

![alt text](https://github.com/reddy2004/REDFS_Storage_Spaces/blob/main/REDFS_Storage_Spaces/Data/Screenshots/config_chunks.png)
