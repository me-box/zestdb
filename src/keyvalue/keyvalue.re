open Lwt.Infix;

module Json = {

  module Store = Ezirmin.FS_lww_register (Irmin.Contents.Json);

  type t = Lwt.t (Store.branch);

  let json_empty = Ezjsonm.dict [];

  let create path_to_db::path => {
    Store.init root::(path ^ "Json_kv") bare::true () >>= Store.master;
  };

  let write branch::branch id::id key::k json::v => {
    branch >>= fun branch' =>
      Store.write message::(id ^ k) branch' path::[id, k] v
  };

  let read branch::branch id::id key::k => {
    branch >>= fun branch' =>
      Store.read branch' path::[id, k] >>=
        fun data => switch data {
          | Some json => Lwt.return json;
          | None => Lwt.return json_empty;
        };  
  };

};

