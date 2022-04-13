//angular
var myApp = angular.module('myLoginApp', []);

myApp.controller('myLogin', ['$scope', function ($scope) {

    $scope.displayflags = {};
    $scope.displayflags.isLoggedIn = false;
    $scope.displayflags.newContainerForm = false;
    $scope.myip = "<My ip address>";
    $scope.errMsg = "<My ip address>";

    $scope.containerList = [];
    $scope.selectedContainer = '';

    $scope.existingMountedMessage = "";

    $scope.onChange = function(val) {
        alert(val);
    }

    $scope.newContainer = function() {
        $scope.displayflags.newContainerForm = true;
    }

    $scope.CreateContainer = function() {
        $scope.displayflags.isLoggedIn = false;
        $scope.displayflags.newContainerForm = false;

        var cname = $("#cname").val();
        var cdescription = $("#cdescription").val();
        
        //var size = $("#size").val();
        //var csize = $("#csize").val();

        var uname = $("#uname111").val();
        var upassword = $("#upassword111").val();

        //alert(cname + "," + cdescription + "," + size + "," + csize + "," + uname + "," + upassword);
        $.get("newContainer?cname=" + cname + "&cdescription=" + cdescription + "&uname=" + uname + "&upassword=" + upassword, function (data) {
            var res = JSON.parse(data);
            if(res.result == 'SUCCESS') {
                $scope.$apply(function () {
                        alert("Container created successfully!");
                        $scope.displayflags.isLoggedIn = true;
                        window.location.href='/login';
                });
            } else {
                alert("Incorrect options given ! or container already exists");
            }
        });
    }

    $scope.shutdown = function() {

        $.get("shutdown", function (data) {
            alert("Shutdown Done");
            window.location.href='https://github.com/reddy2004/RedFS-Windows-Filesystem';
        });

    }

    $scope.login = function() {
        var container = $('#container :selected').text();
        var uname = $("#uname").val();
        var upassword = $("#upassword").val();

        //alert(container);
        //window.location.href='/config';

        $.get("validate?uname=" + uname + "&upassword=" + upassword + "&container=" + container, function (data) {
            var res = JSON.parse(data);
            console.log(data + " <<<< " + res.result);
            if(res.result == 'SUCCESS') {
                $scope.$apply(function () {
                        document.cookie = "redfs=" + res.cookie + "," + "connectedFrom=" + res.connectedFrom;
                        $scope.displayflags.isLoggedIn = true;
                        window.location.href='/config';
                });
            } else {
                document.cookie = "";
                alert("Incorrect credentials! or some container is already mounted");
            }
        });
    }

    $scope.unmount = function() {
        $scope.displayflags.isLoggedIn = false;  
    }

    $(document).ready(function () {
          $.get("getKnownContainers", function( data ) {
                $scope = angular.element('[ng-controller=myLogin]').scope();
                $scope.$apply(function () {
                    $scope.containerList = JSON.parse(data).all;
                    if (JSON.parse(data).mounted != "") {
                        $scope.existingMountedMessage = "The container ' " + JSON.parse(data).mounted + " ' is already mounted, Try logging in to that and unmount it before trying anything else";
                    }
                    //alert(data);
                });
                
          });
    });

}]);

