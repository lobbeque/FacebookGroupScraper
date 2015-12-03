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

var post_cpt = 0;

var queryFeed = groupId + "/feed?fields=" + config.facebook.fields_feed + "&since=" + argv.since + "&until=" + argv.until;

var confPostgres = "postgres://" + config.postgres.user + ":" + config.postgres.pswd + "@localhost/" + config.postgres.db;

function exitWithError(msg) {
  console.log(msg);
  process.exit(1);
}

function processPosts(posts,res,callback) {

    async.eachSeries(posts, function(post, next) {

        post_cpt ++;

        /*
         * some posts params must be pre-process
         */

        var to             = null;
        var to_id          = null;
        var message        = null;
        var name           = null;
        var caption        = null;
        var description    = null;
        var likes          = null;
        var likes_id       = null;
        var comments       = null;
        var comments_id    = null;
        var comments_likes = null;

        var users          = []; 

        users.push(post.from);

        /*
         * Extracting ids
         */

        if (post.to != null) {
            to = post.to.data;
            users = _.union(users,to); 
            to_id = _.map(to, function(to){ return to.id; });            
        }

        if (post.likes != null) {
            likes = post.likes.data;
            users = _.union(users,likes); 
            likes_id = _.map(likes, function(like){ return like.id; });
        }

        if (post.comments != null) {
            comments = post.comments.data;
            comments_id = _.map(comments, function(comment){ return comment.id; });
        }

        /*
         * Dealing with excaping charachters
         */
        
        if (post.message != null)
            message = post.message.replace(/'/g, "''");
        
        if (post.name != null)
            name = post.name.replace(/'/g, "''");

        if (post.caption != null)
            caption = post.caption.replace(/'/g, "''");

        if (post.description != null)
            description = post.description.replace(/'/g, "''");

        var client = new pg.Client(confPostgres);

        var queryPost = "INSERT INTO fb_post (id,created_time,updated_time,from_id,to_id,message,link_url,link_name,link_picture,link_caption,link_description,source_url,type,likes,comments)"
                + " SELECT '" +
                post.id + "','" +
                post.created_time + "','" +
                post.updated_time + "','" +
                post.from.id + "'," +
                "ARRAY[" + to_id + "],'" +
                message + "','" +
                post.link + "','" +
                name + "','" +
                post.picture + "','" +
                caption + "','" +
                description + "','" +
                post.source + "','" +
                post.type + "'," + 
                "ARRAY[" + likes_id + "]," +
                "ARRAY[" + comments_id + "]" +
                " WHERE NOT EXISTS ( SELECT id FROM fb_post WHERE id = '" + post.id + "')";

        client.connect(function(err) {

            if (err)
                next(err);

            async.waterfall([

                function(nextStep) {

                    // Add a post to db

                    client.query(queryPost, function(err, result) {

                        if (err) {
                            console.log(queryPost);
                            nextStep(err);
                        }

                        nextStep(null);
                    });
                },
                function(nextStep) {

                    // Add comments related to previous post to db

                    if (comments != null) {
                        
                        // flatten commentaries tree 

                        var cmt = [];

                        _.map(comments,function(c){

                            if (c.comments != null) {
                                cmt = _.union(cmt,c.comments.data);
                            }

                            cmt = _.union(cmt,_.omit(c,"comments"));

                            return;
                        });

                        async.each(cmt, function(comment, nextCmt) {

                            var comment_msg = null;

                            if (comment.message != null)
                                comment_msg = comment.message.replace(/'/g, "''");

                            var parent_id = null;

                            if (comment.parent != null);
                                parent_id = comment.parent.id;

                            var comment_likes = null;
                            var comment_likes_id = null;

                            if (comment.likes != null) {
                                comment_likes = comment.likes.data;
                                users = _.union(users,comment_likes);
                                comment_likes_id = _.map(comment_likes, function(like){ return like.id; });
                            }

                            users.push(comment.from);

                            var queryComment = "INSERT INTO fb_comment (id,from_id,comment_count,like_count,created_time,message,parent,likes)"
                                + " SELECT '" +
                                comment.id + "','" +
                                comment.from.id + "'," +
                                comment.comment_count + "," +
                                comment.like_count + ",'" +                
                                comment.created_time + "','" +
                                comment_msg + "','" +
                                parent_id + "'," +
                                "ARRAY[" + comment_likes_id + "]" +
                                " WHERE NOT EXISTS ( SELECT id FROM fb_comment WHERE id = '" + comment.id + "')"; 

                            client.query(queryComment, function(err, rst) {
                                
                                if(err)
                                    nextCmt(err);

                                nextCmt(null);
                            });
                        
                        }, function(err){
                            
                            if(err)
                                nextStep(err);

                            nextStep(null);
                        });

                    } else {
                        nextStep(null);                        
                    }

                },
                function(nextStep) {

                    // add all the users associated to a post such as : post.from, post.to[], post.likes[], comment.from, comment.likes[]

                    async.eachSeries(users, function(user,nextUser){

                        var queryUser = "INSERT INTO fb_user (id,name)"
                            + " SELECT '" +
                            user.id + "','" +
                            user.name.replace(/'/g, "''") + "'" +
                            " WHERE NOT EXISTS ( SELECT id FROM fb_user WHERE id = '" + user.id + "')"; 

                        client.query(queryUser, function(err, rst) {
                            
                            if(err) {
                                console.log(queryUser);
                                nextUser(err);
                            }

                            nextUser(null);
                        });

                    }, function(err){

                        if(err)
                            nextStep(err);

                        nextStep(null);

                    });

                }
            ], function (err, result) {

                if(err)
                    next(err);
                
                /*
                 * Close the connection and go to next post
                 */
                
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
                callback(null, 'done');
            }
        }
    
    ], function (err, res) {

        if (err)
            exitWithError(err);

        console.log("=> " + post_cpt + " posts added");

        console.log("=> End !");
        
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

