var volumeListData = null;
var volumeTable = null;
var root_volume = null;
var max_vol_id = 0;
var configDataJson = "";

var redfsSettings = {"Volumes" : "", "Free Space" : "", "Used Space" : "", "DedupeSavings" : "", "CompressionSavings" : ""};
var volumeSettings = {"Volume Name" : "", "Comments" : "", "HEX Color" : "", "HEX Color Mounted" : ""};

//angular
var myApp = angular.module('myApp11',['ngRoute']);

myApp.controller('myCtrl', ['$scope', function($scope) {

    $scope.globalData = {};

    $scope.currentlyViewingVolumeId = 0;
    $scope.currentlyViewingVolumeDisableMountAndUnmountButton = false;
    $scope.currentlyViewingVolumeName = "";
    $scope.CurrentlyMountedContainer = "";
    $scope.currentlyViewingVolumeLogicalData = 0;
    $scope.currViewingChunkId = "";
    $scope.isCurrentlyViewingVolumeDeleted = false;

    $scope.currentlyViewingVolAllowAllOperations = true;

    $scope.currentlyMountedVolumeIdGlobal = 0;
    $scope.currentlyMountedVolumeIdGlobalText = "Mount";

    $scope.containerList = [];

    $scope.isDebugDataForChunkFiles = "- <> -";
    $scope.listOfKnownChunksInContainer = [];
    $scope.totalSizeInGB = "-";
    $scope.totalFreeChunkSpace = "-";

    $scope.itemsInFolderForBackupTaskBrowser = {};
    $scope.itemsInFolderForBackupTaskBrowser.files = [];
    $scope.itemsInFolderForBackupTaskBrowser.directories = [];

    //when collecting the list to be sent to server
    $scope.tobackup = {};
    $scope.tobackup.listOfFoldersToBackup = [];
    $scope.tobackup.listOfFilesToBackup = [];

    $scope.isDebugData = "Nothing for now";
    $scope.showdebuginfo = false;
    $scope.isDebugDataForGraphs = "isDebugDataForGraphs";
    $scope.isDebugDataForProgress = "isDebugDataForProgress";
    $scope.isDebugDataForPieGraph = "isDebugDataForPieGraph";

    $scope.isDebugDataForBackupTasks = "-";

    $scope.compressionAndDedupeData = {};

    $scope.toggleDebug = function() {
        $scope.showdebuginfo = ($scope.showdebuginfo == false)? true : false;
    }

    $scope.listOfKnownBackupsInContainer = {};

    $("head").append (
        '<link '
        + 'href="//cdnjs.cloudflare.com/ajax/libs/jqueryui/1.12.1/jquery-ui.css" '
        + 'rel="stylesheet" type="text/css">'
    );

    $("head").append (
        '<link '
        + 'href="//cdnjs.cloudflare.com/ajax/libs/jqueryui/1.12.1/jquery-ui.theme.css" '
        + 'rel="stylesheet" type="text/css">'
    );

    //For the dialog widget, it must be on top to be properly clickable.
    $("body").append('<style> .ui-dialog { position: fixed; padding: .2em; } .ui-widget-overlay { overflow: auto; position: fixed; top: 0;left: 0;width: 50%;height: 80%;} .ui-front {z-index: 100;} </style>');
    $("body").append('<style> label, input { display:block; } input.text { margin-bottom:8px; width:95%; padding: .4em; } textarea.text { margin-bottom:12px; width:95%; padding: .4em; } fieldset { padding:0; border:0; margin-top:25px; } h1 { font-size: 1.2em; margin: .6em 0; } .validateTips { border: 1px solid transparent; padding: 0.3em; } </style>');

    $scope.globalData.connectedFrom = Cookies.get("redfs");

    $scope.notYetImplimented = function()
    {
        alert("Not yet implimented");
    }

    $scope.showDedupePopup = function() {
        $scope.compressionAndDedupeData.dedupeIsRunning = false;
        for (var i = 0; i < $scope.inProgressOpsList.length; i++) {
            if ($scope.inProgressOpsList[i].OpName == "Dedupe") {
                 $scope.compressionAndDedupeData.dedupeIsRunning = true;
                 $scope.compressionAndDedupeData.dedupePercent = $scope.inProgressOpsList[i].width;             
            }
        }
        $("#dedupeJob").modal();
    }

    $scope.showCompressionPopup = function()
    {
        $scope.compressionAndDedupeData.compressionIsRunning = false;
        for (var i = 0; i < $scope.inProgressOpsList.length; i++) {
            if ($scope.inProgressOpsList[i].OpName == "Compression") {
                 $scope.compressionAndDedupeData.compressionIsRunning = true;
                 $scope.compressionAndDedupeData.compressionPercent = $scope.inProgressOpsList[i].width;             
            }
        }
        
        $("#compressJob").modal();
    }

    $scope.StartDedupeJob = function() {
        $("#dedupeJob").modal('hide');
        $scope.optimizeStorageOperations("dedupe");
    }

    $scope.StartCompressJob = function() {
        $("#compressJob").modal('hide');
        $scope.optimizeStorageOperations("compress");
    }

    $scope.addItemToBackupTask = function(path)
    {
        $scope.hostFileSystem_operation("isFileOrFolder", path, [], [], function(data) {
                //alert(JSON.stringify(data));
                if (data.isFile == true) {
                    $scope.tobackup.listOfFilesToBackup.push(path);
                } else {
                    $scope.tobackup.listOfFoldersToBackup.push(path);
                }
        });

        $("#browserFolderModalOverModal").modal('hide');
        $("#newBackupTaskModal").modal();
    }

    $scope.dontAddFolderToBackupTask = function()
    {
        $("#browserFolderModalOverModal").modal('hide');
        $("#newBackupTaskModal").modal();
    }

    $scope.listFoldersAndFiles = function(name) 
    {
        $scope.viewingParentFolder = name;
        $scope.hostFileSystem_operation("listContents", name, [], [],function(data) {

            $scope.$apply(function () {
                $scope.isDebugData = JSON.stringify(data);
                $scope.itemsInFolderForBackupTaskBrowser.directories = data.directories;
                $scope.itemsInFolderForBackupTaskBrowser.files = data.files;
            });
        });
    }

    $scope.runBackupTask = function(id) {
        var jobname = $("#jobnametext").val();
        if (jobname == "") {
            alert("You must enter a job name!");
            return;
        }
        $scope.hostFileSystem_operation_bkup("runBackupJob", id, jobname, function(data) {
            setTimeout (function(){
                window.location.href='/config';
            },1000);
        });
    }

    $scope.deleteBackup = function(id) {
        $scope.hostFileSystem_operation_bkup("deleteBackup", id, "", function(data) {
            setTimeout (function(){
                window.location.href='/config';
            },1000);
        });
    }

    $scope.folderBrowser = function()
    {
        $scope.viewingParentFolder = "";
        $scope.hostFileSystem_operation("listContents", "", [], [], function(data) {

            $scope.isDebugData = JSON.stringify(data);

            $scope.itemsInFolderForBackupTaskBrowser.directories = data.directories;
            $scope.itemsInFolderForBackupTaskBrowser.files = data.files;
              var $table = $('#table')

              $(function() {
                $('#browserFolderModalOverModal').on('shown.bs.modal', function () {
                     $table.bootstrapTable('resetView')
                })
              })

            $("#newBackupTaskModal").modal('hide');
            $("#browserFolderModalOverModal").modal();

        });

    }

    $scope.CreateNewBackupTask = function() {
        $("#newBackupTaskModal").modal();
    }


    $scope.removeEntryFromBackupList = function(item) {
        alert(item);
        //tobackup.listOfFoldersToBackup
    }

    $scope.createNewBackupTaskInContainer = function() {
        var backupName = $("#m2_backup_task").val();
        //alert(JSON.stringify($scope.tobackup));
        $scope.hostFileSystem_operation("newBackupTask", backupName, $scope.tobackup.listOfFilesToBackup, $scope.tobackup.listOfFoldersToBackup, function(data) {
            setTimeout (function(){
                window.location.href='/config';
            },1000);
        });
        $("#newBackupTaskModal").modal('hide');
    }

    $scope.AddNewChunk = function() {
        $("#newChunkModal").modal();
    }

    $scope.saveVolumeInformation = function() {
        var volName = $("#m2_volname").val();
        var volDesc = $("#m2_voldesc").val();
        var hexcolor = $("#m2_volcolor").val();

        $scope.volume_operation("save", $scope.currentlyViewingVolumeId, volName, volDesc, hexcolor, function() {
            window.location.href='/config';
        });
    }

    $scope.actionOnVolume = function() 
    {
        $("#volumeInformation").modal('hide');
        $scope.shouldDisableSnapshotAndBackedClone($scope.currentlyViewingVolumeId);
        setTimeout (function(){
          
          $("#volumeSnapshotOrCloneOrDelete").modal();
        },500);
    }

    $scope.GetInternalDataOfFSID = function(volid)
    {
        $.get("GetInternalDataOfFSID?fsid=" + volid, function (data) {
            alert(data);
        });
        $("#volumeInformation").modal('hide');
    }

    $scope.shouldDisableSnapshotAndBackedClone = function(volumeId) {
        //volumeListData
        $scope.currentlyViewingVolAllowAllOperations = true;
        for(var i = 0;i < volumeListData.length; i++) {

            if (volumeListData[i].parentVolumeId == volumeId) {
                $scope.currentlyViewingVolAllowAllOperations = false;
                break;
            } 
        }
    }

    $scope.volumeSnapshotOrCloneOrDelete = function(action) {
        //alert("Action " + action);

        $("#volumeSnapshotOrCloneOrDelete").modal('hide');
        switch (action) {
            case 'backedclone':
                $scope.volume_operation("backedclone", $scope.currentlyViewingVolumeId, "", "", "", function() {
                    //window.location.href='/config';
                });
                alert("Backed Clone op request has been sent. Refresh this page in a few seconds to see the lastest volume Tree!");
                break;
            case 'clone':
                $scope.volume_operation("clone", $scope.currentlyViewingVolumeId, "", "", "", function() {
                    //window.location.href='/config';
                });
                alert("Clone op request has been sent. Refresh this page in a few seconds to see the lastest volume Tree!");
                break;
            case 'snapshot':
                $scope.volume_operation("snapshot", $scope.currentlyViewingVolumeId, "", "", "", function() {
                    //window.location.href='/config';
                });
                alert("Snapshot op request has been sent. Refresh this page in a few seconds to see the lastest volume Tree!");
                break;
            case 'delete':
                $scope.volume_operation("delete", $scope.currentlyViewingVolumeId, "", "", "", function() {
                    //window.location.href='/config';
                });
                alert("Delete op request has been sent. Refresh this page in a few seconds to see the lastest volume Tree!");
                break;
            case 'mountOrUnmount':
                var cmd = ($scope.currentlyMountedVolumeIdGlobalText == "Mount")? 'mount' : 'unmount';
                $scope.volume_operation(cmd, $scope.currentlyViewingVolumeId, "", "", "", function() {
                    //window.location.href='/config';
                });
                break;
        }

        setTimeout (function(){
            window.location.href='/config';
        },2000);
    }

    $scope.singleChunkOperation = function(id) {
        
        for (var i=0;i < $scope.listOfKnownChunksInContainer.length; i++) {
            if ($scope.listOfKnownChunksInContainer[i].id == id) {
                $scope.currViewingChunkId = $scope.listOfKnownChunksInContainer[i];
                break;
            }

        }
        $("#singleChunkOperation").modal();
    }

    $scope.listOfJobsForTask = function(serial) {
        $("#listOfJobsForTask").modal();

        for (var i=0;i<$scope.listOfKnownBackupsInContainer.listOfTasks.length;i++) {
            if ($scope.listOfKnownBackupsInContainer.listOfTasks[i].serial == serial) {
                $scope.currentlyViewingBackupTask = $scope.listOfKnownBackupsInContainer.listOfTasks[i];
                break;
            }
        }
        //alert($scope.currentlyViewingBackupTask.backupJobs);
    }

    $scope.listOfFilesAndFoldersForTask = function(serial) {

        $scope.listOfFoldersToBackup = [];
        $scope.listOfFilesToBackup = [];

        for (var i=0;i<$scope.listOfKnownBackupsInContainer.listOfTasks.length;i++) {
            if ($scope.listOfKnownBackupsInContainer.listOfTasks[i].serial == serial) {
                $scope.listOfFoldersToBackup = $scope.listOfKnownBackupsInContainer.listOfTasks[i].directories;
                $scope.listOfFilesToBackup = $scope.listOfKnownBackupsInContainer.listOfTasks[i].files;
            }
        }
        $("#listOfFilesAndFoldersForTask").modal();
    }

    $scope.actionOnBackupTask = function(id) {
        
        for (var i=0;i<$scope.listOfKnownBackupsInContainer.listOfTasks.length;i++) {
            if ($scope.listOfKnownBackupsInContainer.listOfTasks[i].serial == id) {
                $scope.currentlyViewingBackupTask = $scope.listOfKnownBackupsInContainer.listOfTasks[i];
            }
        }

        $("#actionOnBackupTask").modal();
    }

    $scope.unmount = function() {
        $.get("logoutAndUnmount", function (data) {
            window.location.href='/login';
        });
        //$scope.displayflags.isLoggedIn = false;  
    }

    $scope.volume_operation = function (op, volumeId, volName, volDesc, hexcolor, callback) {
         var datax = {"operation": op, "volumeId": volumeId, "volname": volName, "volDesc" : volDesc, "hexcolor" : hexcolor};
          $.ajax({
              type: 'POST',
              url: '/volumeOperation',
              data: JSON.stringify (datax),
              success: function(data) {
                 //alert('data: ' + data);
                 callback();
              },
              contentType: "application/json",
              dataType: 'json'
          });
    }

    $scope.hostFileSystem_operation_bkup = function (op, taskid, jobname, callback) {
         var datax = {"operation" : op ,"backupTaskId": taskid, "backupJobName" : jobname,
                "path": "", "fileBackupPaths" : [], "directoryBackupPaths" : []};
          $.ajax({
              type: 'POST',
              url: '/hostFileSystem',
              data: JSON.stringify (datax),
              success: function(data) {
                 callback(data);
              },
              contentType: "application/json",
              dataType: 'json'
          });
    }

    $scope.hostFileSystem_operation = function (op, currentfolderPath, filepaths, dirpaths, callback) {
         var datax = {"operation" : op ,"path": currentfolderPath, "fileBackupPaths" : filepaths, "directoryBackupPaths" : dirpaths};
          $.ajax({
              type: 'POST',
              url: '/hostFileSystem',
              data: JSON.stringify (datax),
              success: function(data) {
                 callback(data);
              },
              contentType: "application/json",
              dataType: 'json'
          });
    }


    $scope.CreateNewZeroVolume = function() {
        var volName = $("#m1_volname").val();
        var volDesc = $("#m1_voldesc").val();
        var volColor = $("#m1_volcolor").val();

        alert("Trying to clone root volume!");
        $scope.volume_operation("clone", 0, volName, volDesc, volColor, function() {
            window.location.href='/config';
        });
    }

    $scope.createDataUsagePie = function() {
        var options = {
          title: {
            //text: "CONTAINER USAGE"
          },
          data: [{
              type: "pie",
              startAngle: 45,
              showInLegend: "true",
              legendText: "{label}",
              indexLabel: "{label} ({size})",
              //yValueFormatString:"#,##0.#"%"",
              dataPoints: [
                { label: "User Backups data", y:6, size: "6 GB", color: "green" },
                { label: "Volume unique data", y: 31, size: "31 GB", color: "lightblue" },
                { label: "Free space", y: 7 , size: "7 GB", color: "lightyellow"},
                { label: "Volume Deduped data", y: 7, size: "7 GB", color: "blue" }
              ]
          }]
        };
        $("#konvaPie").CanvasJSChart(options);
    }

    $scope.addNewChunkForCurrentContainer = function()
    {
        var speedclass = $("#speedclass").val();
        var path = $("#filepath11").val();
        var size = $("#sizeingb11").val();
        var allowedTypes = $("#allowedTypes").val();

         var datax = {"speedClass": speedclass, "path": path, "size": size, "allowedTypes": allowedTypes};
          $.ajax({
              type: 'POST',
              url: '/addNewChunkForCurrentContainer',
              data: JSON.stringify (datax),
              success: function(data) {
                 //alert('data: ' + data);
                 //callback();
                window.location.href='/config';
              },
              contentType: "application/json",
              dataType: 'json'
          });
          $("#newChunkModal").modal('hide');
    }

    $scope.optimizeStorageOperations = function(op) 
    {
        //alert(op + " for chunkid " + $scope.currViewingChunkId);
        var datax = {"OpName": op};
        $.ajax({
              type: 'POST',
              url: '/optimizeStorageOperations',
              data: JSON.stringify (datax),
              success: function(data) {
                 //callback();
                window.location.href='/config';
              },
              contentType: "application/json",
              dataType: 'json'
          });
    }

    $scope.removeCompletedOperation = function(id)
    {
        $scope.operationsAPI("delete_operation", id);
    }

    $scope.operationsAPI = function(op, op_id) 
    {
        var datax = {"id": op_id, "OpName": op};
        $.ajax({
              type: 'POST',
              url: '/operationsAPI',
              data: JSON.stringify (datax),
              success: function(data) {
                window.location.href='/config';
              },
              contentType: "application/json",
              dataType: 'json'
          });
    }

    $scope.ChunkRemoveOrMoveOrDeleteOperation = function(op) 
    {
        //alert(op + " for chunkid " + $scope.currViewingChunkId);
        var datax = {"id": $scope.currViewingChunkId.id, "OpName": op};
        $.ajax({
              type: 'POST',
              url: '/chunkRemoveOrMoveOrDeleteOperation',
              data: JSON.stringify (datax),
              success: function(data) {
                 //callback();
                window.location.href='/config';
              },
              contentType: "application/json",
              dataType: 'json'
          });
        $("#singleChunkOperation").modal('hide');
    }

    $(document).ready(function() {
          
          $(".loader").show();

          //Set compression and dedupe flags
            $scope.compressionAndDedupeData.compressionIsRunning = false;
            $scope.compressionAndDedupeData.compressionPercent = 0;
            $scope.compressionAndDedupeData.compressionStartTime = "-";

            $scope.compressionAndDedupeData.dedupeIsRunning = false;
            $scope.compressionAndDedupeData.dedupePercent = 0;
            $scope.compressionAndDedupeData.dedupeStartTime = "-";

          $.get("getKnownContainers", function( data ) {
                $scope = angular.element('[ng-controller=myCtrl]').scope();
                $scope.$apply(function () {
                    $scope.containerList = JSON.parse(data).all;
                    $scope.CurrentlyMountedContainer = JSON.parse(data).mounted;
                    if (JSON.parse(data).mounted != "") {
                        $scope.existingMountedMessage = "The container ' " + JSON.parse(data).mounted + " ' is already mounted, Try logging in to that and unmount it before trying anything else";
                    }
                    //alert(data);
                });
                
          });

          //Call once during page load
          $.get("allvolumelist", function( data ) {
                //serverJsonData = data;
                volumeListData = JSON.parse(data);
                console.log(data); 
                updateVolumeStatusWithEmpty();
          });  

          //Call once during page load
          $.get("getKnownBackTasks", function( data ) {
                //serverJsonData = data;
                $scope = angular.element('[ng-controller=myCtrl]').scope();
                $scope.$apply(function () {
                    $scope.isDebugDataForBackupTasks = data;
                    $scope.listOfKnownBackupsInContainer = JSON.parse(data);
                });
                
                console.log(data); 
          });

          //Call once during page load
          $.get("getAllChunksInContainer", function( data ) {
                //serverJsonData = data;
                $scope = angular.element('[ng-controller=myCtrl]').scope();
                $scope.$apply(function () {
                    $scope.isDebugDataForChunkFiles = data;
                    
                    $scope.listOfKnownChunksInContainer = JSON.parse(data);
                    
                    $scope.totalSizeInGB = 0;
                    $scope.totalFreeChunkSpace = 0;
                    //alert($scope.listOfKnownChunksInContainer.length);
                    for (var i=0;i<$scope.listOfKnownChunksInContainer.length;i++) {
                        $scope.totalSizeInGB += $scope.listOfKnownChunksInContainer[i].size;
                        $scope.totalFreeChunkSpace += $scope.listOfKnownChunksInContainer[i].freeSpace;
                    }
                });
                
                console.log(data); 
          });

          //Traverse the list from root_volume
          function findVolume(current, volumeId) {

              if (current.volumeId == volumeId) {
                  return current;
              }

              //current height is already done.
              var nextSibling = current.sibling;

              if (nextSibling != null) {
                  var v = findVolume(nextSibling, volumeId);
                  if (v != null) {
                      return v;
                  }
              }

              for (var i = 0; i< current.children.length; i++) {
                  var v = findVolume(current.children[i], volumeId);
                  if (v != null) {
                      return v;
                  }
              }
              return null;
          }

          function updateVolumeStatusWithEmpty() {
                var array = volumeListData;
                for(i in array) {
                  array[i].status = '-';
                }

                //Lets also update if the volume can be snapshoted or backedClone is possible.

          }

          function download_volume_data(callback) {
            $.get("allvolumelist", function( data ) {
                  volumeListData = JSON.parse(data);
                  console.log(data);
                  updateVolumeStatusWithEmpty();
                  callback(data);
            });  
          }


          var volTreeBar;
          volTreeBar = $("<div id='volTreeBar' class='bg-secondary' windowsize='minimized'></div>");

          $('body').prepend(volTreeBar);   

          $('#volTreeBar').click(function (e) {
              e.stopPropagation();
              
              if (volumeListData == null) {
                  alert("Data not yet loaded, please try in a few seconds..");
                  return;
              }

              if ('minimized' == $(this).attr('windowsize')) {

                  var mysidebar = $("<div id='mysidebar' ></div>");
                  var konvadiv = $("<div id='konvadiv'> </div>"); 
                  konvadiv.css({'background-color': 'white'});

                  $('#konvaPad').innerHTML = '';
                  $('#konvaPad').prepend(konvadiv);

                  $(this).attr('windowsize','maximized');
                  var height2 = window.innerHeight - 70;
                  var width2 = window.innerWidth - 70;

                  var stage = new Konva.Stage({
                        container: 'konvadiv',
                        width: width2,
                        height: height2,
                        draggable: true,
                        x: 0,
                        y: 0,
                        color: 'yellow',
                        fill : 'yellow'           
                  });

                  var layer2 = new Konva.Layer({draggable: true,fill: 'green', x:0, y:0});
                  stage.add(layer2);

                  var infoRect = new Konva.Rect({
                      x: 0,
                      y: 0,
                      width: 800,
                      height: 280,
                      fill: 'red',
                      opacity: 0,
                      stroke: 'black',
                      cornerRadius: 5,
                      shadowBlur: 10,
                      shadowOffsetX: 10,
                      shadowOffsetY: 3,
                      name: ''
                  });
                  layer2.add(infoRect);

                  layer2.draw();

                  stage.on('click', function (e) {
                      if (!(typeof e.target.name() == "undefined")) {
                          if (e.target.name() == "DIV") {
                            console.log("cannot edit this..");
                            alert("Currently you cannot edit the DIV section from within the UI, Edit JSON directly and upload it to the right place and refresh this page");
                          } else if (e.target.name() == "V0") {
                              $scope = angular.element('[ng-controller=myCtrl]').scope();
                              $scope.$apply(function () {
                                  $("#createNewVolumeFromRootVolume").modal();
                              });
                          } else {
                              var volumeId = e.target.name().substring(1, e.target.name().length);
                              $scope = angular.element('[ng-controller=myCtrl]').scope();
                              $scope.$apply(function () {
                                  for (var i=0; i < current_volume_list.length;i++) {
                                        if (current_volume_list[i].volumeId == volumeId) {
                                            //alert(JSON.stringify(current_volume_list[i]));
                                            $scope.currentlyViewingVolumeId = volumeId;
                                            $scope.currentlyViewingVolumeLogicalData = current_volume_list[i].logicalData;
                                            $scope.currentlyViewingVolumeCreationTime = current_volume_list[i].volumeCreateTime;
                                            $scope.currentlyViewingVolumeName = current_volume_list[i].volname;
                                            $scope.isCurrentlyViewingVolumeDeleted = current_volume_list[i].isDeleted;

                                            $("#m2_volcolor").val(current_volume_list[i].hexcolor);
                                            $("#m2_volname").val(current_volume_list[i].volname);
                                            $("#m2_voldesc").val(current_volume_list[i].volDescription);

                                            if (volumeId ==  $scope.currentlyMountedVolumeIdGlobal) {
                                                 $scope.currentlyMountedVolumeIdGlobalText = "Unmount";
                                                 $scope.currentlyViewingVolumeDisableMountAndUnmountButton = false;
                                             } else {
                                                 if ($scope.currentlyMountedVolumeIdGlobal == 0) {
                                                        $scope.currentlyViewingVolumeDisableMountAndUnmountButton = false;
                                                 } else {
                                                        $scope.currentlyViewingVolumeDisableMountAndUnmountButton = true;
                                                 }
                                                 $scope.currentlyMountedVolumeIdGlobalText = "Mount";
                                             }
                                        }
                                  }
                                  $("#volumeInformation").modal();

                              });
                          }
                      }
                  });

                  var current_volume_list = [];

                  var load_volume_list = function() {
                        var array = volumeListData;
                        for(i in array) {
                          current_volume_list.push(array[i]);
                          console.log("volume " + JSON.stringify(array[i]));

                          if (array[i].volumeId > max_vol_id)
                            max_vol_id = array[i].volumeId;
                        }                 
                  }

                  var create_root_volume = function () {
                        root_volume = new Object();
                        root_volume.volumeId = 0;
                        root_volume.sibling = null;
                        root_volume.parentVolumeId = -1;
                        root_volume.parentIsSnapshot = false;
                        root_volume.volname = "rootVolume";
                        root_volume.children = [];
                        root_volume.width = 0;
                        root_volume.height = 0;
                        root_volume.color = 'yellow';
                  }

                  var create_volume = function(volumeId, parentVolumeId, volname, parentIsSnapshot, hexcolor) {
                        var obj = new Object();
                        obj.volumeId = volumeId;
                        obj.parentVolumeId = parentVolumeId;
                        obj.parentIsSnapshot = parentIsSnapshot;

                        //XXX todo fix
                        root_volume.sibling = null;

                        obj.volname = volname;
                        obj.children = [];
                        obj.width = 0;
                        obj.height = 0;
                        obj.hexcolor = hexcolor;
                        return obj;
                  }

                  //return net height added in this tree.
                  var adjust_heights = function(current) {
                        //current height is already done.
                        var nextSibling = current.sibling;
                        var tree_start_height = current.height;

                        if (nextSibling != null) {
                            nextSibling.width = current.width + 1;
                            nextSibling.height = current.height;
                            tree_start_height = adjust_heights(nextSibling);
                        }

                        console.log(current.volname + "," + tree_start_height);

                        for (var i = 0; i< current.children.length; i++) {
                            current.children[i].width = current.width + 1;
                            current.children[i].height = tree_start_height + 2; 
                            tree_start_height = adjust_heights(current.children[i]);
                        }

                        if (tree_start_height > current.height)
                            return tree_start_height;
                        else
                            return current.height;
                  }
                  
                  var find_and_insert = function(current, volume) {
                    
                        if (volume.parentVolumeId == current.volumeId) {
                            if (volume.parentIsSnapshot) {
                              current.sibling = volume;
                            } else {
                              current.children.push(volume);
                              if (current.deleted == true)
                                current.color = 'gray'; 
                              return true;
                            }
                        } else {
                            if (current.sibling != null && find_and_insert(current.sibling, volume))
                                return true;
                            for (var i=0;i<current.children.length; i++) {
                                if (find_and_insert(current.children[i], volume))
                                  return true;
                            }
                        }
                        return false;
                  }

                  //Adjust the list so that we can insert correctly. i.e parents come first before children
                  var order_volume_list = function() {


                  }

                  //Insert this into the tree, do traversal
                  var create_tree = function() {
                        load_volume_list();
                        create_root_volume();

                        //FIX this .. todo
                        order_volume_list();

                        for(var i = 0;i < current_volume_list.length; i++) {
                            var vol = create_volume(current_volume_list[i].volumeId, current_volume_list[i].parentVolumeId, 
                                        current_volume_list[i].volname, current_volume_list[i].parentIsSnapshot, 
                                        (current_volume_list[i].isDeleted)? "#000000" : current_volume_list[i].hexcolor);
                            find_and_insert(root_volume, vol);
                            console.log(JSON.stringify(vol));
                        }

                        var workingtreeheight = adjust_heights(root_volume);
                  }

                  function volumebox2(volname, volumeId, x1, y1, color2) {
                        $scope = angular.element('[ng-controller=myCtrl]').scope();

                        var volCode = "V" + volumeId;
                        var color = "#AABBCC";
                        
                        if (color2 != null) {
                            color = color2
                        }
                        console.log(color);
                        var group = new Konva.Group({
                            x: x1,
                            y: y1,
                            width: 300,
                            height: 80,
                            rotation: 0,
                            //draggable: true,
                            opacity: 1,
                            fill: 'white',
                            stroke: 'black',
                            strokeWidth: 1

                        });

                        var swidth = 1;
                        if (volumeId == $scope.currentlyMountedVolumeIdGlobal) {
                            swidth = 20;
                        }

                        var rect = new Konva.Rect({
                            x: 0,
                            y: 0,
                            width: 300,
                            height: 80,
                            fill: color,
                            stroke: 'black',
                            cornerRadius: 5,
                            shadowBlur: 20,
                            shadowOffsetX: 10,
                            shadowOffsetY: 10,
                            name: volCode,
                            opacity: 1,
                            strokeWidth : swidth
                        });

                        rect.fill(color);
                        var text3 = new Konva.Text({
                            x: 10,
                            y: 10,
                            fontFamily: 'Verdana',
                            fontSize: 40,
                            text: volname,
                            fill: 'black'
                        }); 

                              
                        group.add(rect);
                        group.add(text3);

                        return group;             
                  }

                  //200x70 is box size
                  function draw_connector(mylayer, w1, h1, w2, h2) {
                        let quad1 = new Konva.Shape({
                            stroke: 'black',
                            strokeWidth: 10,
                            lineCap: 'round',
                            sceneFunc: function(context) {
                              context.beginPath();
                              context.moveTo(w1*350 + 100, h1*50 + 80 + 4);
                              context.quadraticCurveTo(w1*350 + 100, h2*50 + 40,w2*350 - 4, h2*50 + 40);
                              context.strokeShape(this);
                            }
                        });         
                        mylayer.add(quad1);
                  }

                  function draw_line(mylayer, w1, h1, w2, h2) {
                        var redLine = new Konva.Line({
                              points: [w1*350 + 300 + 4, h1*50 + 40, w2*350 - 4, h2*50 + 40],
                              stroke: 'black',
                              strokeWidth: 10,
                              lineCap: 'round',
                              lineJoin: 'round'
                        });       
                        mylayer.add(redLine);
                  }

                  var draw_vol_tree = function(mylayer, current) {
                        var volbox1 = volumebox2(current.volname, current.volumeId, current.width*350, current.height*50, current.hexcolor);
                        mylayer.add(volbox1);
                    
                        if (current.sibling != null) {
                            draw_vol_tree(mylayer, current.sibling);
                            draw_line(mylayer, current.width, current.height, current.sibling.width, current.sibling.height);
                        }

                        if (current.children.length > 0) {
                            for (var i=0;i<current.children.length;i++) {
                                draw_vol_tree(mylayer, current.children[i]);
                                draw_connector(mylayer, current.width, current.height, current.children[i].width, current.children[i].height);
                            }
                        }
                  }

                  //Lets create the tree here.
                  create_tree();
                  draw_vol_tree(layer2, root_volume);    

                  layer2.draw();
                  stage.scale({ x: 0.4, y: 0.4 });
                  stage.batchDraw();            
              }  
          });
          
          volumeTable = null;

          function GetOptionsForMetricGraphs(title1, g1, g2, suffix1) {
                    var options = {
                        //animationEnabled: true,
                        theme: "light",
                        title:{
                            text: "-"
                        },
                        axisX:{
                            //valueFormatString: "S"
                        },
                        axisY: {
                            title: title1,
                            suffix: suffix1,
                            minimum: 0
                        },
                        toolTip:{
                            shared:true
                        },  
                        legend:{
                            cursor:"pointer",
                            verticalAlign: "bottom",
                            horizontalAlign: "left",
                            dockInsidePlotArea: true
                            //itemclick: toogleDataSeries
                        },
                        data: [{
                            type: "line",
                            showInLegend: true,
                            name: g1,
                            markerType: "square",
                            //xValueFormatString: "DD MMM, YYYY",
                            color: "#FF0000",
                            //yValueFormatString: "#,##0K",
                            dataPoints: [
                            ]
                        },{
                            type: "line",
                            showInLegend: true,
                            name: g2,
                            markerType: "square",
                            //xValueFormatString: "DD MMM, YYYY",
                            color: "#00FFFF",
                            //yValueFormatString: "#,##0K",
                            dataPoints: [
                            ]
                        }]
                    };
                    return options;
          }

          function drawLinearGraphs(data) {

              var options = GetOptionsForMetricGraphs("Latency", "Read Latency (ms)", " Write latency (ms)", "(ms)");
              options.data[0].dataPoints = [];
              options.data[1].dataPoints = [];
              options.title.text = "Read and Write latency";

              for (var i=0;i<120;i++) {
                  options.data[0].dataPoints.push({x: i, y: data[1][i]});
                  options.data[1].dataPoints.push({x: i, y: data[2][i]});
              }
              //console.log(options);
              //$(".loader").hide();
              
              $("#chartContainerRW").CanvasJSChart(options);



              options = GetOptionsForMetricGraphs("Bytes R/W", "Read KB", "Write KB", " (KB)");
              options.title.text = "Data Moved I/O";
              options.data[0].dataPoints = [];
              options.data[1].dataPoints = [];
              for (var i=0;i<120;i++) {
                  options.data[0].dataPoints.push({x: i, y: data[3][i]});
                  options.data[1].dataPoints.push({x: i, y: data[4][i]});
              }
              $("#chartContainerRefs").CanvasJSChart(options);


              options = GetOptionsForMetricGraphs("Dokan Ops", "Calls", "-", "");
              options.title.text = "Dokan Calls";
              options.data[0].dataPoints = [];
              options.data[1].dataPoints = [];
              for (var i=0;i<120;i++) {
                  options.data[0].dataPoints.push({x: i, y: data[5][i]});
                  options.data[1].dataPoints.push({x: i, y: data[5][i]});
              }

              $("#chartContainerDirty").CanvasJSChart(options);


              options = GetOptionsForMetricGraphs("Container Space", "Logical Data", "Physical Data", "(MB)");
              options.title.text = "Container Space Usage";
              options.data[0].dataPoints = [];
              options.data[1].dataPoints = [];
              for (var i=0;i<120;i++) {
                  options.data[0].dataPoints.push({x: i, y: data[6][i]});
                  options.data[1].dataPoints.push({x: i, y: data[7][i]});
              }
              $("#chartContainerSpace").CanvasJSChart(options);
          }

          //Refresh thread, refreshes every 1 second
          setInterval(function(){
              if (volumeTable != null) {
                  
                    getContainerStatus();

                   $.get("allmetrics", function( datastr ) {
                      var data = JSON.parse(datastr);
                      $scope.isDebugDataForGraphs = JSON.stringify(data);
                      drawLinearGraphs(data);
                   });

              }
          }, 5000);

          function update_table(showdata) {
              $(".loader").show();
              $('#table_id').hide();
              
              download_volume_data(function(data){
                    if (volumeTable != null)
                      volumeTable.destroy();

                    var dData = showdata ? volumeListData : {};
                    volumeTable = $('#table_id').DataTable( {
                        data: dData,
                        columns: [
                        {
                            data: "volumeId",
                            title: "Vol ID"
                        },
                        {
                            data: "volname",
                            title: "Vol Name"
                        },
                        {
                            data: "parentVolumeId",
                            title: "Parent Vol ID"
                        },
                        {
                            data: "volumeCreateTime",
                            title: "Created"
                        },
                        {
                            data: "logicalData",
                            title: "Logical Data"
                        },
                        {
                            data: "status",
                            title: "Status"
                        },
                        {
                            data: "volDescription",
                            title : "Description"
                        }]
                    } );
                    $(".loader").hide();
                    $('#table_id').show(); 
              });
          }

          //start with metrics page
          $("#aggrconfig").attr('class', 'col-3 bg-light');
          $scope.currentTab = 'config';

          update_table(true);

        //Let make sure that the graphs come up quicky, even if its empty
        $scope.isDebugDataForGraphs = [[],[],[],[],[],[],[],[]];
        drawLinearGraphs($scope.isDebugDataForGraphs);

    });

    $scope.operations = {};

    $scope.opsdata = {};
    $scope.currentTab = 'liveMetrics';

    $scope.mystyle = {
        "display" : 'none',
        "font-size" : "10px",
    }
    $scope.myrightstyle = {
        //"font-size" : "15px",
        "width" : "60%",
        "margin-left" : "auto",
        "margin-right" : "auto"
    }
    $scope.anchorstyle = {
        "background-color" : "#FF00FF"
    }

    $scope.shout = function(){
      $scope.anchorstyle = {
          "background-color" : "#FFFF00"
      }
    }

    $scope.showOperationDialog = function(type) {
        $scope.aaopbutton = {
            "display" : "none"
        }
    }

    $scope.gotoTab = function(tabname) {
      $scope.currentTab = tabname;

       if (tabname == 'mountedVolume') {
            $("#sharedvols").attr('class', 'col-3 bg-light');
            $("#snapshotvols").attr('class', 'col-3 bg-link');
            $("#aggrops").attr('class', 'col-3 bg-link');
            $("#aggrconfig").attr('class', 'col-3 bg-link');
            $("#xtable").show();
            $("#xconfig").hide();
            //update_table(true);
            $('#volTreeBar').trigger('click');
        }

        if (tabname == 'allSnapshots') {
            $("#sharedvols").attr('class', 'col-3 bg-link');
            $("#snapshotvols").attr('class', 'col-3 bg-light');
            $("#aggrops").attr('class', 'col-3 bg-link');
            $("#aggrconfig").attr('class', 'col-3 bg-link');
            $("#xtable").show();
            $("#xconfig").hide();
            //update_table(true);
        }

        if (tabname == 'liveMetrics') {
            $("#sharedvols").attr('class', 'col-3 bg-link');
            $("#snapshotvols").attr('class', 'col-3 bg-link');
            $("#aggrops").attr('class', 'col-3 bg-light');
            $("#aggrconfig").attr('class', 'col-3 bg-link');
            $("#xtable").hide();
            $("#xconfig").show();
            $scope.createDataUsagePie();

        }

        if (tabname == 'config') {
            $("#sharedvols").attr('class', 'col-3 bg-link');
            $("#snapshotvols").attr('class', 'col-3 bg-link');
            $("#aggrops").attr('class', 'col-3 bg-link');
            $("#aggrconfig").attr('class', 'col-3 bg-light');
            $("#xtable").hide();
            $("#xconfig").show();
        }
    }

    //$scope.parent = "rootVolume [0 -> 10]";
    //$scope.volsize = "165 GB [Snapshot]";
    //$scope.volname = "First Volume X";
    //$scope.ctime = "March 20 2020 8:30AM GMT+5:30"
    var numFailures = 0;
    var getContainerStatus = function()
    {
          $.get("containerOpsStatus").done(function( data ) {
                $scope = angular.element('[ng-controller=myCtrl]').scope();
                $scope.$apply(function () {
                    $scope.isDebugDataForProgress = data;
                    var odata = JSON.parse(data);
                    $scope.inProgressOpsList = odata.inProgressOpsList;
                    $scope.currentlyMountedVolumeIdGlobal = odata.currentlyMountedVolume;
                });
          }).fail(function(err){
                //alert("erorr " + JSON.stringify(err));
                numFailures++;
                if (numFailures >= 10) {
                    window.location.href='https://github.com/reddy2004/RedFS-Windows-Filesystem';
                }
          });
    }

    $scope.gotoTab('config');
    getContainerStatus();

}]);


