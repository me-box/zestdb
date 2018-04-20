module Json: {
    type t;
    let create : path_to_db::string => t;
    let write : ctx::t => id::string => key::string => json::Ezjsonm.t => Lwt.t unit;
    let read : ctx::t => id::string => key::string => Lwt.t Ezjsonm.t;
    let keys : ctx::t => id::string => Lwt.t Ezjsonm.t;
    let delete : ctx::t => id::string => key::string => Lwt.t unit;
    let delete_all : ctx::t => id::string => Lwt.t unit;
};

module Text: {
    type t;
    let create : path_to_db::string => t;
    let write : ctx::t => id::string => key::string => text::string => Lwt.t unit;
    let read : ctx::t => id::string => key::string => Lwt.t string;
    let keys : ctx::t => id::string => Lwt.t Ezjsonm.t;
    let delete : ctx::t => id::string => key::string => Lwt.t unit;    
};

module Binary: {
    type t;
    let create : path_to_db::string => t;
    let write : ctx::t => id::string => key::string => binary::string => Lwt.t unit;
    let read : ctx::t => id::string => key::string => Lwt.t string;
    let keys : ctx::t => id::string => Lwt.t Ezjsonm.t;
    let delete : ctx::t => id::string => key::string => Lwt.t unit; 
};