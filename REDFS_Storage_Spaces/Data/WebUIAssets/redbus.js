const puppeteer = require('puppeteer');
var fs = require('fs');

var data_blrhyd;
var data_hydblr;

function loadData() {
  var cfg2 = fs.readFileSync('data_blrhyd.json').toString();
  data_blrhyd = JSON.parse(cfg2);

  var cfg3 = fs.readFileSync('data_hydblr.json').toString();
  data_hydblr = JSON.parse(cfg3);
}

function LOG(logger, text) {
    var writeLine = (line) => logger.write(`\n${line}`);
    writeLine(text);
}


function extractNumberOfSeatsFromHTMLElement(htmlstr) {
    var idx = htmlstr.indexOf("<");
    if (idx == 0 || idx == -1) {
        return 0;
    }
    //console.log(htmlstr + " -> " + htmlstr.substring(0, idx));
    return parseInt(htmlstr.substring(0, idx));
}

//Overwrite
function updateData(filename, timeStamp, allBusData)
{
    var data = (filename == "data_blrhyd.json")? data_blrhyd : data_hydblr;

    data.timeslots.push(timeStamp);
    var totalAvailableSeats = 0;

    for (var i=0;i< allBusData.length; i++) {
        var busid = allBusData[i].id;
        var operator = allBusData[i].operator;
        var currprice = allBusData[i].price;
        var currseats = extractNumberOfSeatsFromHTMLElement(allBusData[i].seats);

        totalAvailableSeats += currseats;

        var found = false;
        for (var j = 0;j < data.graph.length; j++) {
            if (data.graph[j].id == busid) {
                data.graph[j].prices.push(currprice);
                data.graph[j].seats.push(currseats);
                found = true;
                break;
            }
        }

        if (found == false) {
            data.graph.push({id : busid, operator : operator, prices : [], seats : []});
            data.graph[data.graph.length - 1].prices.push(currprice);
            data.graph[data.graph.length - 1].seats.push(currseats);
        }
    }

    data.totalSeats.push(totalAvailableSeats);

    fs.writeFile(filename, JSON.stringify(data, null, 4), function (err) {

    });

      //Now convert this to CSV file.
      var logger = fs.createWriteStream(filename + ".csv" , {
        flags: 'w' // 'a' means appending, w is truncate and write
      });

      var hstring = "ID, Operator,";
      for (var t = 0; t < data.timeslots.length; t++) {
         hstring += data.timeslots[t] + ",";
      }

      LOG(logger, hstring);

      for (var jx = 0;jx < data.graph.length; jx++) {
          var jstring = data.graph[jx].id + "," + data.graph[jx].operator + ",";
          for (var cx=0;cx < data.graph[jx].prices.length; cx++) {
             jstring += data.graph[jx].prices[cx] + ",";
          }
          LOG(logger, jstring);

      }
      logger.end();
}



function Wrapper(filename, url)
{
    (async () => {

      console.log("running for " + filename);
      loadData();

      var allBusData = [];
      const browser = await puppeteer.launch();

      const page = await browser.newPage();
      await page.setViewport({width: 2000, height: 1000, deviceScaleFactor : 1, isMobile : false, hasTouch: false, isLandscape: false});
      await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36');
      
      await page.goto(url);
      await page.waitForTimeout(5000);

      await page.screenshot({path: 'example74.png'});


      await page.evaluate(() => {
          var lastScrollHeight = 0;
          function autoScroll() {
            var sh = document.documentElement.scrollHeight;
            if (sh != lastScrollHeight) {
              lastScrollHeight = sh;
              document.documentElement.scrollTop = sh;
                console.log("Top = " + lastScrollHeight);
            }
          }
          let intervalId = window.setInterval(autoScroll, 100);

          function myGreeting()
          {
             console.log("stopping auto scroll after 5 seconds");
               window.clearInterval(intervalId);
          }

          const myTimeout = setTimeout(myGreeting, 7000);
      });

      await page.waitForTimeout(8000);
      await page.screenshot({path: 'example75.png'});


      await page.evaluate(() => {
          var allBusData = [];

          var busids = [];
          $(".bus-items").find('li').each(function(){
              var product = $(this);
              var name = product.attr('id');
              if (!(typeof (name) === "undefined")) {
                  console.log(name);
                  busids.push(name);
              }

          });
          var currbus = 0;
          console.log("num busids found = " + busids.length);

          var spans2a = $(".row-sec .clearfix").find(".travels ");
          var spans2b = $(".row-sec .clearfix").find(".fare").find(".f-bold");
          var spans3b = $(".row-sec .clearfix").find(".seat-left");

          console.log("Num rows found = " + spans2a.length + "," + spans2b.length);
          var ctr2 = Math.min(spans2a.length, spans2b.length);
          for(var i=0;i<ctr2;i++)
          {
              var busid = busids[currbus++];
              console.log("[" + busid + "]" + spans2a[i].innerHTML + "  , " + spans2b[i].innerHTML);
              
              allBusData.push({id : busid, "operator" : spans2a[i].innerHTML, "price" :  spans2b[i].innerHTML, "seats" : spans3b[i].innerHTML});

          }

          return allBusData
        }).then(function(result) {

            var date = new Date;
            var hour = date.getHours();
            var minutes = date.getMinutes();

            allBusData =  result;
            updateData(filename, hour + ":" + minutes + " IST", result);
        });

      console.log("Completed for " + page.url());
      await browser.close();
      
    })();
}


function sleep (time) {
  return new Promise((resolve) => setTimeout(resolve, time));
}

var url_blr_hyd = 'https://www.redbus.in/bus-tickets/bangalore-to-hyderabad?fromCityName=Bangalore&fromCityId=122&toCityName=Hyderabad&toCityId=124&onward=23-Jan-2023&srcCountry=IND&destCountry=IND&opId=0&busType=Any';
var url_hyd_blr = 'https://www.redbus.in/bus-tickets/hyderabad-to-bangalore?fromCityName=Hyderabad&fromCityId=124&toCityName=Bangalore&toCityId=122&onward=23-Jan-2023&srcCountry=IND&destCountry=IND&opId=0&busType=Any';

function LoopingFunction()
{
    var date = new Date;
    var hour = date.getHours();
    var minutes = date.getMinutes();
    console.log("Starting data capture.." + hour + ":" + minutes + " IST");

    
    sleep(500).then(() => {
       Wrapper("data_hydblr.json",url_hyd_blr);
    });

    sleep(60000).then(() => {
        
         Wrapper("data_blrhyd.json",url_blr_hyd);
    });

    setTimeout(LoopingFunction, 60000 * 15);
}

LoopingFunction();