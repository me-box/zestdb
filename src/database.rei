
module String: {
    module Kv: {
        module Store: {
            type branch = Ezirmin.FS_lww_register(Irmin.Contents.String).branch;
        };
        let create : file::string => Lwt.t Store.branch;
        let write : Lwt.t Store.branch => string => string => Lwt.t unit;
        let read : Lwt.t Store.branch => string => Lwt.t string;
    };
};

module Json: {
    module Kv: {
        module Store: {
            type branch = Ezirmin.FS_lww_register(Irmin.Contents.Json).branch;
        };
        let create : file::string => Lwt.t Store.branch;
        let write : Lwt.t Store.branch => string => Ezjsonm.t => Lwt.t unit;
        let read : Lwt.t Store.branch => string => Lwt.t Ezjsonm.t;
    };
    module Ts: {
        module Store: {
            type branch = Ezirmin.FS_log(Tc.Pair(Tc.Int)(Irmin.Contents.Json)).branch;
        };
        let create : file::string => Lwt.t Store.branch;
        let write : Lwt.t Store.branch => option int => string => Ezjsonm.t => Lwt.t unit;
        module Complex: {
            let read_latest: Lwt.t Store.branch => string => Lwt.t Ezjsonm.t;
            let read_last: Lwt.t Store.branch => string => int => Lwt.t Ezjsonm.t;
            let read_earliest: Lwt.t Store.branch => string => Lwt.t Ezjsonm.t;
            let read_first: Lwt.t Store.branch => string => int => Lwt.t Ezjsonm.t;
            let read_since: Lwt.t Store.branch => string => int => Lwt.t Ezjsonm.t;
            let read_range: Lwt.t Store.branch => string => int => int => Lwt.t Ezjsonm.t;
        };
        module Simple: {
            let read_latest: Lwt.t Store.branch => string => Lwt.t Ezjsonm.t;
            let read_last: Lwt.t Store.branch => string => int => Lwt.t Ezjsonm.t;
            let read_earliest: Lwt.t Store.branch => string => Lwt.t Ezjsonm.t;
            let read_first: Lwt.t Store.branch => string => int => Lwt.t Ezjsonm.t;
        };
    };
};
