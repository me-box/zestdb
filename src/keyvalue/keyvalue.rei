module Json: {
    type t;
    let create : path_to_db::string => t;
    let write : branch::t => id::string => key::string => json::Ezjsonm.t => Lwt.t unit;
    let read : branch::t => id::string => key::string => Lwt.t Ezjsonm.t;
};

module Text: {
    type t;
    let create : path_to_db::string => t;
    let write : branch::t => id::string => key::string => text::string => Lwt.t unit;
    let read : branch::t => id::string => key::string => Lwt.t string;
};

module Binary: {
    type t;
    let create : path_to_db::string => t;
    let write : branch::t => id::string => key::string => binary::string => Lwt.t unit;
    let read : branch::t => id::string => key::string => Lwt.t string;
};