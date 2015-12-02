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

function exitWithError(msg) {
  console.log(msg);
  process.exit(1);
}

function processPosts(posts,res,callback) {

    async.each(posts, function(post, next) {

        var id = post.id;
        var created_time = post.created_time;
        var updated_time = post.updated_time;
        var from = post.from;

        var to = null;
        var to_id = null;
        
        if (post.to != null) {
            to = post.to.data;
            to_id = _.map(to, function(to){ return to.id; });            
        }
        
        var message = null;
        
        if (post.message != null) {
            message = post.message.replace(/'/g, "''");
        } 
        
        var link_url = post.link;
        var link_name = post.name;

        var link_name = null;
        
        if (post.name != null) {
            link_name = post.name.replace(/'/g, "''");
        } 

        var link_picture = post.picture;
        var link_caption = post.caption;

        var link_description = null;
        
        if (post.description != null) {
            link_description = post.description.replace(/'/g, "''");
        } 
        
        var source_url = post.source;
        var type = post.type;

        var likes = null;
        var likes_id = null;

        if (post.likes != null) {
            likes = post.likes.data;
            likes_id = _.map(likes, function(like){ return like.id; });
        }

        var comments = null;
        var comments_id = null;

        if (post.comments != null) {
            comments = post.comments.data;
            comments_id = _.map(comments, function(comment){ return comment.id; });
        }

        var client = new pg.Client(confPostgres);

        var query = "INSERT INTO fb_post (id,created_time,updated_time,from_id,to_id,message,link_url,link_name,link_picture,link_caption,link_description,source_url,type,likes,comments)"
                + " SELECT '" +
                id + "','" +
                created_time + "','" +
                updated_time + "','" +
                from.id + "'," +
                "ARRAY[" + to_id + "],'" +
                message + "','" +
                link_url + "','" +
                link_name + "','" +
                link_picture + "','" +
                link_caption + "','" +
                link_description + "','" +
                source_url + "','" +
                type + "'," + 
                "ARRAY[" + likes_id + "]," +
                "ARRAY[" + comments_id + "]" +
                " WHERE NOT EXISTS ( SELECT id FROM fb_post WHERE id = '" + id + "')";

        client.connect(function(err) {

            if (err)
                next(err);

            client.query(query, function(err, result) {

                if (err) {
                    console.log(query)
                    next(err);
                }


                client.end();

                next();
            });
        });

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

