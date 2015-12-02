/*
 * Scrap data from a given facebook group 
 */

var config = require("./package.json");
var fb     = require("fb");
var async  = require("async");
var pg     = require("pg");
var _      = require("underscore");
var argv   = require('yargs')
  .usage('Scrap data from a given facebook group')
  .demand(['since','until'])
  .argv;

var groupId = config.facebook.group_id;

var queryFeed = groupId + "/feed?fields=" + config.facebook.fields_feed + "&since=" + argv.since + "&until=" + argv.until;

var confPostgres = "postgres://" + config.postgres.user + ":" + config.postgres.pswd + "@localhost/" + config.postgres.db;

function processPosts(posts,res,callback) {

    async.each(posts, function(post, next) {

        var id = post.id;
        var created_time = post.created_time;
        var updated_time = post.updated_time;
        var from = post.from;
        var to_array = post.to.data;
        var to_id_array = _.map(to_array, function(to){ return to.id; });
        var message = post.message;
        var link_url = post.link;
        var link_name = post.name;
        var link_picture = post.picture;
        var link_caption=post.caption;

        console.log(from);
        console.log(to_id_array);

        // var client = new pg.Client(confPostgres);

        // client.connect(function(err) {

        //     if (err)
        //         next(err);

        //     client.query("INSERT INTO TABLE fb_post (" 
                
        //         + post.id + "," +
        //         + created_time + "," +
        //         + post.id + "," +

        //         ")", function(err, result) {

        //         if (err)
        //             next(err);

        //         client.end();
        //     });
        // });

      next();

    }, function(err){
        
        if (err) 
            callback(err);

        callback(null,res);

    });    

}

function getFeed(query,callback) {

    // process query feed

    async.waterfall([
    
        function(next) {

            // call graph api

            fb.api(query, function (res) {
        
                if(!res || res.error) {
                    next(res.error);
                }

                next(null, res);

            });

        },

        function(res, next) {

            // process results

            processPosts(res.data,res,next);

        },
    
        function(res, next) {

            // process pagination if needed or end the call

            var pagingFeed = res.paging;

            if (pagingFeed != null) {

                console.log("=== " + pagingFeed.next);
                
                var nextQuery = groupId + pagingFeed.next.split(groupId)[1];;

                getFeed(nextQuery, next);
            
            } else {

                console.log("end");

                callback(null, 'done');
            }
        }
    
    ], function (err, res) {

        if (err)
            exitWithError(err);

        console.log("endnnn");
        
    });
}

fb.api('oauth/access_token', {

    client_id: config.facebook.app_id,
    client_secret: config.facebook.app_secret,
    grant_type: 'client_credentials'

}, function (res) {
    
    if(!res || res.error) {
        console.log(!res ? 'error occurred' : res.error);
        return;
    }
    
    var accessToken = res.access_token;

    fb.setAccessToken(accessToken);

    getFeed(queryFeed,null);
});

