var r = require('rethinkdb');
var debug = require('debug')('castnow');
var connection = null;
r.connect( {host: 'localhost', port: 28015}, function(err, conn) {
    if (err) throw err;
    connection = conn;
});

var movie_table_name = 'movies';

module.exports = {
    store: function (movie_name,value) {
        var id = this.base64Encode(movie_name);
        var playtime_object = {"id":id,"last_seek_time":value};
        var table_list = r.tableList().run(connection);
        if(table_list.indexOf(movie_table_name) == -1){
            // there is no table that exists...
            r.db('test').tableCreate('movies').run(connection, function(err, result) {
                if (err) throw err;
                debug(JSON.stringify(result, null, 2));
            });
        }else{
            // update/insert the record.
            r.db('test').table(movie_table_name).get(id).
            run(connection, function(err, result) {
                if (err){
                    // this is an insert, s o insert..
                    r.table(movie_table_name).get(id).update({last_seek_time: value}).run(connection, function(err,result){
                        if(err) debug(err);
                    });

                }else{
                    // update
                    r.table(movie_table_name).insert(playtime_object).run(connection,function(err,result){
                        if(err) debug(err);
                    });
                }

            });
        }

    },
    get: function (movie_name) {
        var id = this.base64Encode(movie_name);
        r.db('test').table(movie_name).get(id).run(connection,function(err,result){
            if(err) return -1;
            return result["last_seek_time"];

        });
    }
};

var base64Encode = function(str){
    return new Buffer(str).toString('base64');
};

