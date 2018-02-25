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
      Store.write message::(id ^ "|" ^ k) branch' path::[id, k] v
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

module Text = {
  
    module Store = Ezirmin.FS_lww_register (Irmin.Contents.String);
  
    type t = Lwt.t (Store.branch);
  
    let text_empty = "";
  
    let create path_to_db::path => {
      Store.init root::(path ^ "Text_kv") bare::true () >>= Store.master;
    };
  
    let write branch::branch id::id key::k text::v => {
      branch >>= fun branch' =>
        Store.write message::(id ^ "|" ^ k) branch' path::[id, k] v
    };
  
    let read branch::branch id::id key::k => {
      branch >>= fun branch' =>
        Store.read branch' path::[id, k] >>=
          fun data => switch data {
            | Some json => Lwt.return json;
            | None => Lwt.return text_empty;
          };  
    };
  
  };

  module Binary = {
    
      module Store = Ezirmin.FS_lww_register (Irmin.Contents.String);
    
      type t = Lwt.t (Store.branch);
    
      let binary_empty = "";
    
      let create path_to_db::path => {
        Store.init root::(path ^ "Binary_kv") bare::true () >>= Store.master;
      };
    
      let write branch::branch id::id key::k binary::v => {
        branch >>= fun branch' =>
          Store.write message::(id ^ "|" ^ k) branch' path::[id, k] v
      };
    
      let read branch::branch id::id key::k => {
        branch >>= fun branch' =>
          Store.read branch' path::[id, k] >>=
            fun data => switch data {
              | Some json => Lwt.return json;
              | None => Lwt.return binary_empty;
            };  
      };
    
    };