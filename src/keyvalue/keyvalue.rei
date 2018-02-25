module Json: {

    type t;

    let create : path_to_db::string => t;
    let write : branch::t => id::string => key::string => json::Ezjsonm.t => Lwt.t unit;
    let read : branch::t => id::string => key::string => Lwt.t Ezjsonm.t;

};