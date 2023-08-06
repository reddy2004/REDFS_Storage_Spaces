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

    $scope.zNodes = [
      {id: 1, pId: 0, name: "000", open: true},
    ];

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

    $scope.listOfKnownValidSegmentsInContainer = [];

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

    $scope.visualizerTextKonvaItem1 = "";
    $scope.visualizerTextKonvaItem2 = "";

    $scope.visualizeBlockInfoDebugData = {};

    $scope.toggleDebug = function() {
        $scope.showdebuginfo = ($scope.showdebuginfo == false)? true : false;
    }

    $scope.listOfKnownBackupsInContainer = {};

    $scope.ClonerWindowData = {};


    $scope.ClonerWindowData.leftFSID = -1;
    $scope.ClonerWindowData.rightFSID = -1;
    $scope.ClonerWindowData.leftSelectedPath = "";
    $scope.ClonerWindowData.rightSelectedPath = "";

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
        alert("Not yet implimented 2");
        $("#tree_17_span2").css("background-color", "red");
    }

    $scope.mountLeftFSID = function() {
        var fsid = $("#fsid_1").val();
        //alert(fsid);
        $scope.ClonerWindowData.leftFSID = fsid;

        /*
        var redFSSystemOperation = {};
        redFSSystemOperation.operation = "list";
        redFSSystemOperation.path = "/";
        redFSSystemOperation.fsid = fsid;
        redFSSystemOperation.list = [];
        redFSSystemOperation.sourceFileOrDirPath = "";
        redFSSystemOperation.destinationFileOrDirPath = "";
        */

        var path_t = "/" + fsid + "/list" + "/"; 
        $("#example1").simpleFileBrowser("chgOption", {
            path: path_t
        });

        //$( "#example1" ).simpleFileBrowser("redraw");
    }

    $scope.mountRightFSID = function() {
         var fsid = $("#fsid_2").val();
         //alert(fsid);
         $scope.ClonerWindowData.rightFSID = fsid;

        var path_t = "/" + fsid + "/list" + "/"; 
        $("#files1").simpleFileBrowser("chgOption", {
            path: path_t
        });
    }

    $scope.unmountLeftFSID = function() {
        var fsid = $("#fsid_1").val();
         $scope.ClonerWindowData.leftFSID = -1;

        var path_t = "/0/no-load/"; 
        $("#example1").simpleFileBrowser("chgOption", {
            path: path_t
        });
    }

    $scope.unmountRightFSID = function() {
        var fsid = $("#fsid_2").val();
        $scope.ClonerWindowData.rightFSID = -1;

        var path_t = "/0/no-load/"; 
        $("#files1").simpleFileBrowser("chgOption", {
            path: path_t
        });
    }

    $scope.cloneLeftToRight = function()
    {
        $("#clonerWindow").modal();
    }

    $scope.cloneRightToLeft = function()
    {
        $("#clonerWindow").modal();
    }

    $scope.moveLeftToRight = function()
    {
        $("#moverWindow").modal();
    }

    $scope.moveRightToLeft = function()
    {
        $("#moverWindow").modal();
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

    $scope.AddNewSegment = function()
    {
        $("#newSegmentModal").modal();
    }

    $scope.showFullSegmentSpanmap = function()
    {
              $(".loader").show();
              $('#table_spanmap').hide();
        
             $("#allSegmentMapDisplayPopUp").modal();

            if (volumeTable != null)
              volumeTable.destroy();

                var dataTable = [];
                for (var i=0;i<$scope.listOfKnownSegmentsInContainer.length;i++) {
                    if ($scope.listOfKnownSegmentsInContainer[i].isSegmentValid) {
                            dataTable.push({
                                'id': i,
                                'start_dbn' : $scope.listOfKnownSegmentsInContainer[i].start_dbn,
                                'num_segments' : $scope.listOfKnownSegmentsInContainer[i].num_segments,
                                'totalFreeBlocks' : $scope.listOfKnownSegmentsInContainer[i].totalFreeBlocks,
                                'type' : $scope.listOfKnownSegmentsInContainer[i].type,
                                'isBeingPreparedForRemoval' : false,
                                'dataSegments' : JSON.stringify($scope.listOfKnownSegmentsInContainer[i].dataSegment),
                                'paritySegment' : $scope.listOfKnownSegmentsInContainer[i].paritySegment

                            });
                    }
                }

            var dData = dataTable;
            volumeTable = $('#table_spanmap').DataTable( {
                data: dData,
                columns: [
                {
                    data: "id",
                    title: "Segment ID"
                },
                {
                    data: "start_dbn",
                    title: "Start DBN"
                },
                {
                    data: "totalFreeBlocks",
                    title: "Free Blocks"
                },
                {
                    data: "type",
                    title: "Type"
                },
                {
                    data: "dataSegments",
                    title: "Data Segments"
                },
                {
                    data: "paritySegment",
                    title: "Parity Segments"
                },
                ]
            } );
            $(".loader").hide();
            $('#table_spanmap').show(); 
    }

    var checkRedFSSpaceSegmentHaveChunk = function(segmentInfo, chunkid, chunkoffset)
    {
        if (segmentInfo.type == 0) {
            if (segmentInfo.dataSegments[0].chunkID &&
                    segmentInfo.dataSegments[0].chunkOffset == chunkoffset)
            {
                return true;
            }
            return false;
        } else if (segmentInfo.type == 1) {
            if (segmentInfo.dataSegments[0].chunkID &&
                    segmentInfo.dataSegments[0].chunkOffset == chunkoffset)
            {
                return true;
            }
            if (segmentInfo.dataSegments[1].chunkID &&
                    segmentInfo.dataSegments[1].chunkOffset == chunkoffset)
            {
                return true;
            }
            return false;
        } else if (segmentInfo.type == 2) {
            for (var i=0;i<4;i++) {
                if (segmentInfo.dataSegments[i].chunkID &&
                        segmentInfo.dataSegments[i].chunkOffset == chunkoffset)
                {
                    return true;
                }                
            }
            if (segmentInfo.paritySegment[0].chunkID &&
                    segmentInfo.paritySegment[0].chunkOffset == chunkoffset)
            {
                return true;
            }
            return false;
        } else {
            return false;
        }
    }

    //given any chunkid + offset, see which dbns space segment this maps to
    var chunkSegmentToRedfsSegmentId = function(chunkid, chunkoffset)
    {
            for (var i=0;i<$scope.listOfKnownSegmentsInContainer.length;i++) 
            {
                    if ($scope.listOfKnownSegmentsInContainer[i].isSegmentValid && 
                        checkRedFSSpaceSegmentHaveChunk($scope.listOfKnownSegmentsInContainer[i], chunkid, chunkoffset) == true) {
                        return ($scope.listOfKnownSegmentsInContainer[i].start_dbn/131072);
                    }
            }
            return -1;
    }

    //given a mouse positiion, ie. x position, return the segment offset in that chunk
    var getSegmentOffsetForChunkFromMousePosition = function(width_in_pixel, chunkid, mouse_x)
    {
        var chunksize = 1;
        for (var i=0;i<$scope.listOfKnownChunksInContainer.length;i++) {
            if ($scope.listOfKnownChunksInContainer[i].id == chunkid) {
                chunksize = $scope.listOfKnownChunksInContainer[i].size;
            }
        }
        return ((chunksize * mouse_x)/width_in_pixel);
    }

    var getSegmentOffsetForDBNSpaceFromMousePosition = function(width_in_pixel, mouse_x)
    {
        return (($scope.totalSizeInGB * mouse_x)/width_in_pixel);
    }

    //given a dbn space segment id, return all the chunks and offsets mapped to it.
    var getAllChunksAndOffsetsFromDBNSegmentOffset = function(dbnSegmentId)
    {
            for (var i=0;i<$scope.listOfKnownSegmentsInContainer.length;i++) 
            {
                    if ($scope.listOfKnownSegmentsInContainer[i].isSegmentValid) {
                        //alert("for " + i + " " + JSON.stringify($scope.listOfKnownSegmentsInContainer[i]));
                        var segid = ($scope.listOfKnownSegmentsInContainer[i].start_dbn/131072);
                        if (segid == dbnSegmentId) {
                            return $scope.listOfKnownSegmentsInContainer[i];
                        }
                    }
            }
            return null;
    }

    //do reverse of getSegmentOffsetForChunkFromMousePosition()
    var getMouseX1andX2RangeToHighlightFromChunkSegmentOffsets = function(chunksize, chunkoffset, width_in_pixel)
    {
        var start_x = (chunkoffset * width_in_pixel)/chunksize;
        var end_x = start_x + (width_in_pixel/chunksize);
        return {"start" : start_x, "end" : end_x};
    }

    //same as above but for dbn space, actually the above could be used as well.
    var getMouseX1andX2RangetoHighlightForDBNSpaceSegmentFromMousePosition = function(redfsSizeInGB, segmentoffset, width_in_pixel)
    {
        var start_x = (segmentoffset * width_in_pixel)/redfsSizeInGB;
        var end_x = start_x + (width_in_pixel/redfsSizeInGB);
        return {"start" : Math.ceil(start_x), "end" : Math.floor(end_x)};
    }

    var resetAllChunkHighlighters = function()
    {
        for (var cid= 0; cid < $scope.listOfKnownChunksInContainer.length; cid++) {
                $scope.listOfKnownChunksInContainer[cid].chunkHighlightRect.width(1);
                $scope.listOfKnownChunksInContainer[cid].chunkHighlightRect.x(0);
                $scope.listOfKnownChunksInContainer[cid].chunkHighlightRect.draw();
        }
        $scope.visualizerLayerKonvaItem.draw();

    }


    var getStorageItemFromMouseXY = function(num_chunks, x , y, maxx, maxy)
    {
        if (y > 0 && y < 50)// && y > 0 && y < maxy)
        {
            var dbnoffset = Math.floor( getSegmentOffsetForDBNSpaceFromMousePosition(maxx, x));

            var xandy = getMouseX1andX2RangetoHighlightForDBNSpaceSegmentFromMousePosition($scope.totalSizeInGB, dbnoffset, maxx);
            $scope.dbnSpaceHighLightRect.width(xandy.end - xandy.start);
            $scope.dbnSpaceHighLightRect.x(xandy.start);
            $scope.dbnSpaceHighLightRect.draw();
             $scope.visualizerLayerKonvaItem.draw();

             resetAllChunkHighlighters();

            $scope.visualizerTextKonvaItem1.text("mouse x: " + x + ", y:" + y + " choosen:dbnspace  & range:" + maxx + "," + maxy);
            $scope.visualizerTextKonvaItem2.text("num_chunks: " + num_chunks + ", dbnspaceoffet : " + dbnoffset + " xandy :" + JSON.stringify(xandy));
            //console.log("mouse x: " + x + ", y:" + y + " choosen:dbnspace  & range:" + maxx + "," + maxy);

            //We also need to update the corresponding chunks where this dbnspace segment is mapped to.
            var segmentInfo = getAllChunksAndOffsetsFromDBNSegmentOffset(dbnoffset);
            //alert(JSON.stringify(segmentInfo) + " maps to dbnspace offset " + dbnoffset);
            if (segmentInfo.type == 0) {
                var cid = segmentInfo.dataSegment[0].chunkID;
                var xandy = getMouseX1andX2RangeToHighlightFromChunkSegmentOffsets($scope.listOfKnownChunksInContainer[cid].size,segmentInfo.dataSegment[0].chunkOffset, $scope.listOfKnownChunksInContainer[cid].width_in_pixel);
                $scope.listOfKnownChunksInContainer[cid].chunkHighlightRect.width(xandy.end - xandy.start);
                $scope.listOfKnownChunksInContainer[cid].chunkHighlightRect.x(xandy.start);
                $scope.listOfKnownChunksInContainer[cid].chunkHighlightRect.draw();
                //alert("cid = " + JSON.stringify($scope.listOfKnownChunksInContainer[cid]) + "   xandy :" + JSON.stringify(xandy));
            } else if (segmentInfo.type == 1) {
                //segmentInfo.dataSegment[0].chunkOffset
                //segmentInfo.dataSegment[1].chunkOffset
            } else if (segmentInfo.type == 2) {
                //segmentInfo.dataSegment[0].chunkOffset
                //segmentInfo.dataSegment[1].chunkOffset
                //segmentInfo.dataSegment[2].chunkOffset
                //segmentInfo.dataSegment[3].chunkOffset
                //segmentInfo.paritySegment[0].chunkOffset
            }
            $scope.visualizerLayerKonvaItem.draw();
            return "dbnspace";
        }

        for (var i = 0;i< num_chunks;i++) {
            if (y > (160 + i * 60) && y < (210 + i * 60) ){//&& y > 0 && y < maxy) {
                var chunkoffset = Math.floor( getSegmentOffsetForChunkFromMousePosition(maxx, i, x));
                $scope.visualizerTextKonvaItem1.text("mouse x: " + x + ", y:" + y + " choosen:chunk_" + i  +  " & range:" + maxx + "," + maxy);
                $scope.visualizerTextKonvaItem2.text("num_chunks: " + num_chunks + " chunkid : " + i + ", offset : " + chunkoffset);
                console.log("mouse x: " + x + ", y:" + y + " choosen:chunk_" + i  +  " & range:" + maxx + "," + maxy);
                $scope.visualizerLayerKonvaItem.draw();
                return "chunk_" + i;
            }
        }
        return "";
    }

    $scope.visualizeBlockInfoDebug = function(type) {
        var value = "";
        $scope.visualizeBlockInfoDebugData.TitleBlockVisualizer = "Wait.. updating..";
        $scope.visualizeBlockInfoDebugData.hexDisplay = [];
        $scope.visualizeBlockInfoDebugData.wipList = [];
        switch (type)
        { 
            case 'dbn':
                value = $("#v1111").val();
                break;
            case 'inodenumber':
                value = $("#v1112").val();
                break;
            case 'inodefbn':
                value = $("#v1113").val();
                break;
            case 'fsid':
                value = $("#v1114").val();
                break;
            default:
                value = "";
        }
        
           $.get("getblockrawdata?type=" + type + "&value=" + value, function( datastr ) {
                  var data = JSON.parse(datastr);
                  $scope.visualizeBlockInfoDebugData = data;
                  $("#block_visualizer").modal();
                  $scope.visualizeBlockInfoDebugData.TitleBlockVisualizer = "Viewing " + type + " of " + value;
           });
    }

    $scope.visualizeSegment = function(visualizeid) {

        $("#visualizer").modal();

        $("#konvaPadVisualizer").html("");

        setTimeout(function() {
            var konvadiv = $("<div id='konvadiv2'> </div>"); 
            $("#konvaPadVisualizer").html("");
            $('#konvaPadVisualizer').prepend(konvadiv);

            var num_chunks = $scope.listOfKnownChunksInContainer.length;
            var height_in_pixel = 50 * (num_chunks + 1) + num_chunks * 10 + 120;
            var width_in_pixel = $("#konvaPadVisualizer").width();

            width_in_pixel = width_in_pixel;

            var max_chunk_size = $scope.totalSizeInGB;
            var chunk_usage_from_segment_map = $scope.listOfKnownSegmentsInContainer_length;

            /*
            for (var i=0;i<$scope.listOfKnownChunksInContainer.length;i++)
            {
                if ($scope.listOfKnownChunksInContainer[i].size > max_chunk_size) {
                    max_chunk_size = $scope.listOfKnownChunksInContainer[i].size;
                }
            }
            */
            for (var i=0;i<$scope.listOfKnownChunksInContainer.length;i++)
            {
                $scope.listOfKnownChunksInContainer[i].width_in_pixel = 
                        Math.ceil(($scope.listOfKnownChunksInContainer[i].size * width_in_pixel) / max_chunk_size);
            }

            $('#konvaPad').innerHTML = '';
             
                  var stage = new Konva.Stage({
                        container: 'konvadiv2',
                        width: width_in_pixel,
                        height: height_in_pixel,
                        draggable: false,
                        x: 0,
                        y: 0,
                        color: 'yellow',
                        fill : 'yellow'           
                  });
                  
                  var layer2 = new Konva.Layer();
                  stage.add(layer2);
                  
                  var infoRect = new Konva.Rect({
                      x: 0,
                      y: 0,
                      width: width_in_pixel,
                      height: 50,
                      fill: 'darkgray',
                      opacity: 50,
                      stroke: 'black',
                      cornerRadius: 0,
                      shadowBlur: 0,
                      shadowOffsetX: 0,
                      shadowOffsetY: 0,
                      name: 'dbnspace'
                  });

                  var widthForFreeAddressSpace = width_in_pixel - Math.ceil((width_in_pixel * chunk_usage_from_segment_map)/max_chunk_size);
                  //alert("max_chunk_size = " + max_chunk_size +  ",  widthForFreeAddressSpace = " + widthForFreeAddressSpace + " and chunk_usage_from_segment_map = " + chunk_usage_from_segment_map + " $scope.listOfKnownValidSegmentsInContainer.length = "  + $scope.listOfKnownValidSegmentsInContainer.length);
                  var infoRect22 = new Konva.Rect({
                      x: (width_in_pixel - widthForFreeAddressSpace),
                      y: 1,
                      width: widthForFreeAddressSpace,
                      height: 48,
                      fill: 'white',
                      opacity: 50,
                      stroke: 'black',
                      cornerRadius: 0,
                      shadowBlur: 0,
                      shadowOffsetX: 0,
                      shadowOffsetY: 0,
                      name: 'dbnspace22'
                  });

                  var dbnSpaceHighLightRect = new Konva.Rect({
                      x: 0,
                      y: 0,
                      width: 1,
                      height: 49,
                      fill: 'red',
                      opacity: 100,
                      stroke: 'red',
                      cornerRadius: 0,
                      shadowBlur: 0,
                      shadowOffsetX: 0,
                      shadowOffsetY: 0,
                      name: 'dbnspace'
                  });

                    var text3 = new Konva.Text({
                        x: 10,
                        y: 5,
                        fontFamily: 'Verdana',
                        fontSize: 14,
                        text: "RedFS address space (sum of all chunks, both used and unused)",
                        fill: 'black'
                    }); 

                    var text4 = new Konva.Text({
                        x: 10,
                        y: 65,
                        fontFamily: 'Verdana',
                        fontSize: 15,
                        text: "data",
                        fill: 'black'
                    }); 
                    var text5 = new Konva.Text({
                        x: 10,
                        y: 85,
                        fontFamily: 'Verdana',
                        fontSize: 15,
                        text: "data",
                        fill: 'black'
                    }); 

                   $scope.visualizerTextKonvaItem1 = text4;
                   $scope.visualizerTextKonvaItem2 = text5;
                   $scope.dbnSpaceHighLightRect = dbnSpaceHighLightRect;
                  layer2.add(infoRect);
                  layer2.add(infoRect22);
                  layer2.add(dbnSpaceHighLightRect);
                  layer2.add(text3);
                  layer2.add(text4);
                  layer2.add(text5);
                  layer2.draw();
                  
                  for (var i =0;i<num_chunks; i++) {
                          var infoRect = new Konva.Rect({
                              x: 0,
                              y: 160 + i * 60 ,
                              width: $scope.listOfKnownChunksInContainer[i].width_in_pixel,
                              height: 50,
                              fill: 'lightblue',
                              opacity: 100,
                              stroke: 'black',
                              cornerRadius: 5,
                              shadowBlur: 10,
                              shadowOffsetX: 10,
                              shadowOffsetY: 3,
                              name: 'chunk_' + i,

                          });
                          var chunkHighlightRect = new Konva.Rect({
                              x: 0,
                              y: 160 + i * 60 ,
                              width: 1,
                              height: 49,
                              fill: 'red',
                              opacity: 10,
                              stroke: 'red',
                              cornerRadius: 5,
                              shadowBlur: 0,
                              shadowOffsetX: 0,
                              shadowOffsetY: 0,
                              name: 'chunkx_' + i,

                          });
                        var text3 = new Konva.Text({
                            x: 10,
                            y: 175 + i * 60,
                            fontFamily: 'Verdana',
                            fontSize: 14,
                            opacity: 50,
                            text: "chunk " + i,
                            fill: 'black'
                        }); 
                          
                          $scope.listOfKnownChunksInContainer[i].chunkHighlightRect = chunkHighlightRect;
                          layer2.add(infoRect);  
                          layer2.add(text3);
                          layer2.add(chunkHighlightRect);                 
                  }

                  layer2.add(infoRect);

                  stage.batchDraw();

                  stage.on('click', function (e) {
                     //alert(e.target.name());
                      if (!(typeof e.target.name() == "undefined")) {
                          if (e.target.name() == "DIV") {
                            console.log("cannot edit this..");
                            alert("Currently you cannot edit the DIV section from within the UI, Edit JSON directly and upload it to the right place and refresh this page");
                          } else {
                            if (e.target.name() == "dbnspace") {
                              //$scope = angular.element('[ng-controller=myCtrl]').scope();
                              //$scope.$apply(function () {
                               //   $("#createNewVolumeFromRootVolume").modal();
                              //});
                            } else {
                                //var chunkid = parseInt(e.target.name().substring(6, e.target.name().length));
                            }
                        }
                    }
                });

                $scope.visualizerLayerKonvaItem = layer2;

                layer2.on('mousemove', function (e) {
                    var pos = stage.getPointerPosition();

                    var choosenitem = getStorageItemFromMouseXY(num_chunks, pos.x, pos.y, width_in_pixel, height_in_pixel);
                    
                    if (choosenitem == "dbnspace") {

                    } else {
                        var chunkid = parseInt(choosenitem.substring(6, choosenitem.length));
                    }

                });


        }, 1000);
    }

    $scope.DropDownChnaged = function()
    {
        var num_chunks = $scope.listOfKnownChunksInContainer.length;
        var all_chunk_paths = [];

        for (var i=0;i<num_chunks;i++) {
            all_chunk_paths.push($scope.listOfKnownChunksInContainer[i].path);
        }
        var speedclass = $("#segmenttype66").val();

        //alert(speedclass);
        if (speedclass == "segmentDefault") {
            $scope.showchunkoption1forsegment = true;
            $scope.showchunkoption2forsegment = false;
            $scope.showchunkoption3forsegment = false;
            $scope.showchunkoption4forsegment = false;
            $scope.showchunkoption5forsegment = false;
            $scope.segmentHelpHint = "['Default' segment is a single copy and is located in only one chunk]";
            $scope.showchunkoption1forsegmentLabel = true;

            $scope.listOfChunks = all_chunk_paths;
        } else if (speedclass == "segmentMirrored") {

            if (num_chunks < 2) {
                alert("You need atleast two chunks to created a mirrored segment in RedFS address space");
                $("#segmenttype").val("segmentDefault");
                //$scope.DropDownChnaged();
                return;
            }
            $scope.showchunkoption1forsegment = true;
            $scope.showchunkoption2forsegment = true;
            $scope.showchunkoption3forsegment = false;
            $scope.showchunkoption4forsegment = false;
            $scope.showchunkoption5forsegment = false;
            $scope.showchunkoption1forsegmentLabel = true;
            $scope.segmentHelpHint = "['Mirrored' segment is 2x mirrored and hence needs two chunks]";

            $scope.listOfChunks = all_chunk_paths;
        } else if (speedclass == "segmentRaid5") {
            if (num_chunks < 5) {
                alert("You need atleast five chunks to created a 4D1P segment in RedFS address space");
                $("#segmenttype").val("segmentDefault");
                //$scope.DropDownChnaged();
                return;
            }
            $scope.showchunkoption1forsegment = true;
            $scope.showchunkoption2forsegment = true;
            $scope.showchunkoption3forsegment = true;
            $scope.showchunkoption4forsegment = true;
            $scope.showchunkoption5forsegment = true;
            $scope.showchunkoption1forsegmentLabel = true;
            $scope.segmentHelpHint = "['RAID5' segment is a 4D+1P fixed. Hence requires data to be striped across 5 chunks]";

            $scope.listOfChunks = all_chunk_paths;
        } else {
            $scope.showchunkoption1forsegment = false;
            $scope.showchunkoption2forsegment = false;
            $scope.showchunkoption3forsegment = false;
            $scope.showchunkoption4forsegment = false;
            $scope.showchunkoption5forsegment = false;
            $scope.showchunkoption1forsegmentLabel = false;
            $scope.segmentHelpHint = "";
        }
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
                if (op == "mount") {
                    alert('reply: ' + JSON.stringify(data));
                }
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

    $scope.addNewSegmentForCurrentContainer = function()
    {
        var sizeInGB = $scope.sizeingb11;

        var segmenttype1 = $("#segmenttype1").val();
        var segmenttype2 = $("#segmenttype2").val();
        var segmenttype3 = $("#segmenttype3").val();
        var segmenttype4 = $("#segmenttype4").val();
        var segmenttype5 = $("#segmenttype5").val();

        var segmenttype = $scope.segmenttype66;

        var chunkid = -1;

        if (segmenttype == "segmentDefault") {
            //first check that the size is available in the chunk.
            //get the chunk id
            for (var i=0;i<$scope.listOfKnownChunksInContainer.length;i++) {
                if ($scope.listOfKnownChunksInContainer[i].path == segmenttype1) {
                    chunkid = $scope.listOfKnownChunksInContainer[i].id;
                    var freeSpaceInGB = $scope.listOfKnownChunksInContainer[i].freeSpace/1024;
                    if (freeSpaceInGB >= sizeInGB) {
                        
                         var datax = {"sizeInGB": sizeInGB, "segmentTypeString": segmenttype, "chunkIDs": [chunkid]};
                          $.ajax({
                              type: 'POST',
                              url: '/createNewSegment',
                              data: JSON.stringify (datax),
                              success: function(data) {
                                 //alert('data: ' + data);
                                 //callback();
                                window.location.href='/config';
                              },
                              contentType: "application/json",
                              dataType: 'json'
                          });
                          $("#newSegmentModal").modal('hide');

                    }
                    else
                    {
                        alert("selected chunk does not have enough free space");
                    }
                }
            }
        } else {
            alert("not yet supported for mirror and raid 5 type");
        }
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
          
            var setting = {
              view: {
                dblClickExpand: false,
                showLine: true,
                selectedMulti: false
              },
              data: {
                simpleData: {
                  enable: true,
                  idKey: "id",
                  pIdKey: "pId",
                  rootPId: ""
                }
              },
              callback: {
                beforeClick: function (treeId, treeNode) {
                  var zTree = $.fn.zTree.getZTreeObj("tree");
                  if (treeNode.isParent) {
                    zTree.expandNode(treeNode);
                    alert(JSON.stringify(treeNode));
                    return false;
                  } else {
                    alert(JSON.stringify(treeNode));
                    return true;
                  }
                }
              }
            };

          $(".loader").show();

          var t = $("#tree");
          t = $.fn.zTree.init(t, setting, $scope.zNodes);

          var zTree = $.fn.zTree.getZTreeObj("tree");
          zTree.selectNode(zTree.getNodeByParam("id", 1));

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
                updateZTreeWithVolumeData();
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
                    //alert(data);
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

          //Call once during page load, not autoupdated
          $.get("getAllSegmentsInContainer", function( data ) {
                //serverJsonData = data;
                $scope = angular.element('[ng-controller=myCtrl]').scope();
                $scope.$apply(function () {
                    $scope.isDebugDataForSegments = data;
                    
                    $scope.listOfKnownSegmentsInContainer = JSON.parse(data).startDBNToDBNSegmentSpan;
                    
                    $scope.totalSegmentSpace = 222;
                });
                
                //now create an array to copy necessary information to display

                var max_valid_segment = 0;
                var add_empty_entries = 0;
                $scope.listOfKnownSegmentsInContainer_length = 0;

                for (var i=0;i<$scope.listOfKnownSegmentsInContainer.length;i++) {
                    if ($scope.listOfKnownSegmentsInContainer[i].isSegmentValid) {
                        max_valid_segment++;
                        $scope.listOfKnownSegmentsInContainer_length++;
                    }
                }
                //alert(max_valid_segment);
                for (var i=0;i<max_valid_segment;i++) {
                    if ($scope.listOfKnownSegmentsInContainer[i].isSegmentValid) {

                        

                        if (i < 2 || i >  max_valid_segment - 2) {
                            $scope.listOfKnownValidSegmentsInContainer.push({
                                'id': i,
                                'start_dbn' : $scope.listOfKnownSegmentsInContainer[i].start_dbn,
                                'num_segments' : $scope.listOfKnownSegmentsInContainer[i].num_segments,
                                'totalFreeBlocks' : $scope.listOfKnownSegmentsInContainer[i].totalFreeBlocks,
                                'type' : $scope.listOfKnownSegmentsInContainer[i].type,
                                'isBeingPreparedForRemoval' : false,
                                'dataSegments' : $scope.listOfKnownSegmentsInContainer[i].dataSegment

                            });
                        } else {
                            if (add_empty_entries++ < 2) {
                                    $scope.listOfKnownValidSegmentsInContainer.push({
                                        'id': '...',
                                        'start_dbn' : '...',
                                        'num_segments' : '...',
                                        'totalFreeBlocks' : '...',
                                        'type' : '...',
                                        'isBeingPreparedForRemoval' :'...',
                                        'dataSegments' : '...'

                                    });                                
                            }
                        }
                    }
                }
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

          function updateZTreeWithVolumeData() {
                //update $scope.zNodes with the correct list of volumes with all expanded tree
                /*
                        [
                          {
                            "status": "-",
                            "isDirty": false,
                            "volumeId": 0,
                            "parentVolumeId": -1,
                            "parentIsSnapshot": false,
                            "volname": "root",
                            "isDeleted": false,
                            "logicalData": 0,
                            "logicalDataStr": "0.00 B",
                            "volumeCreateTime": "2023-08-04T19:53:35.4630132+05:30",
                            "hexcolor": null,
                            "volDescription": null,
                            "isReadOnly": false
                          }
                        ]
                        to 
                        [
                            {id: 1, pId: 0, name: "Root volume", open: true},
                        ]
                */
                var array = volumeListData;
                $scope.zNodes = [];
                for(i in array) {
                    $scope.zNodes.push({id : array[i].volumeId, pId: array[i].parentVolumeId, name : array[i].volname, open :true});
                }

                //update and render
                  var t = $("#tree");
                  t = $.fn.zTree.init(t, setting, $scope.zNodes);

                  var zTree = $.fn.zTree.getZTreeObj("tree");
                  zTree.selectNode(zTree.getNodeByParam("id", 0));

          }

          function download_volume_data(callback) {
            $.get("allvolumelist", function( data ) {
                  volumeListData = JSON.parse(data);
                  console.log(data);
                  updateVolumeStatusWithEmpty();
                  updateZTreeWithVolumeData();
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

          function drawLinearGraphs(data, coredata) {

              var options = GetOptionsForMetricGraphs("Latency", "Read Latency (ms)", " Write latency (ms)", "(ms)");
              options.data[0].dataPoints = [];
              options.data[1].dataPoints = [];
              options.title.text = "Read and Write latency";

              //alert(data[1]);
              //alert(JSON.stringify(data));
              for (var i=0;i<120;i++) {
                  options.data[0].dataPoints.push({x: i, y: data[1][i]});
                  options.data[1].dataPoints.push({x: i, y: data[2][i]});
                  //console.log("Push " + data[1] + " and " + data[2]);
              }
              
              //$(".loader").hide();
              
              $("#chartContainerRW").CanvasJSChart(options);

              

              options = GetOptionsForMetricGraphs("Bytes R/W", "Read KB", "Write KB", " (KB)");
              options.title.text = "Data Moved I/O";
              options.data[0].dataPoints = [];
              options.data[1].dataPoints = [];
              for (var i=0;i<data[3].length;i++) {
                  options.data[0].dataPoints.push({x: i, y: data[3][i]});
                  options.data[1].dataPoints.push({x: i, y: data[4][i]});
              }
              $("#chartContainerRefs").CanvasJSChart(options);


              options = GetOptionsForMetricGraphs("Dokan Ops", "Calls", "-", "");
              options.title.text = "Dokan Calls";
              options.data[0].dataPoints = [];
              options.data[1].dataPoints = [];
              for (var i=0;i<data[5].length;i++) {
                  options.data[0].dataPoints.push({x: i, y: data[5][i]});
                  options.data[1].dataPoints.push({x: i, y: data[5][i]});
              }

              $("#chartContainerDirty").CanvasJSChart(options);


              options = GetOptionsForMetricGraphs("Container Space", "Logical Data", "Physical Data", "(MB)");
              options.title.text = "Container Space Usage";
              options.data[0].dataPoints = [];
              options.data[1].dataPoints = [];
              for (var i=0;i<data[6].length;i++) {
                  options.data[0].dataPoints.push({x: i, y: data[6][i]});
                  options.data[1].dataPoints.push({x: i, y: data[7][i]});
              }
              $("#chartContainerSpace").CanvasJSChart(options);
           
                   
              options = GetOptionsForMetricGraphs("USED BLKS", "Used Blocks", "Free Queue", "(Blocks)");
              options.title.text = "Block Usage (8KB)";
              options.data[0].dataPoints = [];
              options.data[1].dataPoints = [];
              for (var i=0;i<data[6].length;i++) {
                  options.data[0].dataPoints.push({x: i, y: coredata[14][i]});
                  options.data[1].dataPoints.push({x: i, y: coredata[15][i]});
              }
              $("#chartUsedBlks").CanvasJSChart(options);
          }

          //Refresh thread, refreshes every 1 second
          setInterval(function(){
              if (volumeTable != null) {
                  
                    getContainerStatus();

                   $.get("allmetrics", function( datastr ) {
                      var data = JSON.parse(datastr);
                      $scope.isDebugDataForGraphs = JSON.stringify(data);
                      drawLinearGraphs(JSON.parse(data.dokan), JSON.parse(data.core));
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
                            title: "Logical Data (bytes)"
                        },

                        {
                            data: "logicalDataStr",
                            title: "Logical Data (approx)"
                        },                        {
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
          $("#aggrconfig").attr('class', 'col-2 bg-light');
          $scope.currentTab = 'config';

          update_table(true);

        //Let make sure that the graphs come up quicky, even if its empty
        $scope.isDebugDataForGraphs = [[],[],[],[],[],[],[],[]];
        drawLinearGraphs($scope.isDebugDataForGraphs, $scope.isDebugDataForGraphs);

        $('#table_id tbody').on('click', 'tr', function () {
            var data = volumeTable.row(this).data();
            $("#m2_volcolor").val(data.hexcolor);
            $("#m2_volname").val(data.volname);
            $("#m2_voldesc").val(data.volDescription);

            $scope.currentlyViewingVolumeId = data.volumeId;
            $scope.currentlyViewingVolumeLogicalData = data.logicalData;
            $scope.currentlyViewingVolumeCreationTime = data.volumeCreateTime;
            $scope.currentlyViewingVolumeName =data.volname;
            $scope.isCurrentlyViewingVolumeDeleted = data.isDeleted;

            $("#volumeInformation").modal();
            //alert('You clicked on ' + data[0] + "'s row");
        });

        /*
         * Load elements of the web based file system browser.
         * Hows list of files and we can use this to clone or snapshot
         * files/dirs in a volume or across volumes
         */
         $("#example1").simpleFileBrowser({
            url: 'folder.php',
            //json: musica_mini,
            path: '/0/list/',
            view: 'icon',
            select: true,
            breadcrumbs: true,
            onSelect: function (obj, file, folder, type) {
                //alert("You select a "+type+" "+folder+'/'+file);
                $scope.ClonerWindowData.leftSelectedPath = folder+'/'+file;
            },
            onOpen: function (obj,file, folder, type) {
                if (type=='file') {
                    //alert("Open file: "+folder+'/'+file);
                }
            }
         });


        $("#files1").simpleFileBrowser({
            url: 'folder.php',
            path: '/0/list/',
            view: 'icon',
            select: true,
            breadcrumbs: true,
            onSelect: function (obj, file, folder, type) {
                //alert("You select a "+type+" "+folder+'/'+file);
                $scope.ClonerWindowData.rightSelectedPath = folder+'/'+file;
            },
            onOpen: function (obj,file, folder, type) {
                if (type=='file') {
                    //alert("Open file: "+folder+'/'+file);
                }
            }
        });
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
            $("#sharedvols").attr('class', 'col-2 bg-light');
            $("#snapshotvols").attr('class', 'col-2 bg-link');
            $("#aggrops").attr('class', 'col-2 bg-link');
            $("#aggrconfig").attr('class', 'col-2 bg-link');
            $("#webfsbrowser").attr('class', 'col-2 bg-link');
            $("#webfiledircloner").attr('class', 'col-2 bg-link');
            $("#xtable").show();
            $("#xconfig").hide();
            //update_table(true);
            $('#volTreeBar').trigger('click');
        }

        if (tabname == 'allSnapshots') {
            $("#sharedvols").attr('class', 'col-2 bg-link');
            $("#snapshotvols").attr('class', 'col-2 bg-light');
            $("#aggrops").attr('class', 'col-2 bg-link');
            $("#aggrconfig").attr('class', 'col-2 bg-link');
            $("#webfsbrowser").attr('class', 'col-2 bg-link');
            $("#webfiledircloner").attr('class', 'col-2 bg-link');
            $("#xtable").show();
            $("#xconfig").hide();
            //update_table(true);
        }

        if (tabname == 'liveMetrics') {
            $("#sharedvols").attr('class', 'col-2 bg-link');
            $("#snapshotvols").attr('class', 'col-2 bg-link');
            $("#aggrops").attr('class', 'col-2 bg-light');
            $("#aggrconfig").attr('class', 'col-2 bg-link');
            $("#webfsbrowser").attr('class', 'col-2 bg-link');
            $("#webfiledircloner").attr('class', 'col-2 bg-link');
            $("#xtable").hide();
            $("#xconfig").show();
            $scope.createDataUsagePie();

        }

        if (tabname == 'config') {
            $("#sharedvols").attr('class', 'col-2 bg-link');
            $("#snapshotvols").attr('class', 'col-2 bg-link');
            $("#aggrops").attr('class', 'col-2 bg-link');
            $("#aggrconfig").attr('class', 'col-2 bg-light');
            $("#webfsbrowser").attr('class', 'col-2 bg-link');
            $("#webfiledircloner").attr('class', 'col-2 bg-link');
            $("#xtable").hide();
            $("#xconfig").show();
        }

        if (tabname == 'webFSbrowser') {
            $("#sharedvols").attr('class', 'col-2 bg-link');
            $("#snapshotvols").attr('class', 'col-2 bg-link');
            $("#aggrops").attr('class', 'col-2 bg-link');
            $("#aggrconfig").attr('class', 'col-2 bg-link');
            $("#webfsbrowser").attr('class', 'col-2 bg-light');
            $("#webfiledircloner").attr('class', 'col-2 bg-link');
            $("#xtable").hide();
            $("#xconfig").show();
        }

        if (tabname == "webFileDirCloner") {
            $("#sharedvols").attr('class', 'col-2 bg-link');
            $("#snapshotvols").attr('class', 'col-2 bg-link');
            $("#aggrops").attr('class', 'col-2 bg-link');
            $("#aggrconfig").attr('class', 'col-2 bg-link');
            $("#webfsbrowser").attr('class', 'col-2 bg-link');
            $("#webfiledircloner").attr('class', 'col-2 bg-light');
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


