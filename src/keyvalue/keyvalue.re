open Lwt.Infix;

module Json = {

  module Store = Ezirmin.FS_lww_register (Irmin.Contents.Json);

  type t = {
    branch: Lwt.t (Store.branch),    
    keys: Lwt.t Keys.t
  };

  let json_empty = Ezjsonm.dict [];

  let create path_to_db::path => {
    { 
      branch: Store.init root::(path ^ "Json_kv_store") bare::true () >>= Store.master,      
      keys: Keys.create file::(path ^ "Json_kv_keys_store")
    };
  };

  let write ctx::ctx id::id key::k json::v => {
    ctx.branch >>= fun branch' =>
      Store.write message::(id ^ "|" ^ k) branch' path::[id, k] v >>=
        fun () => Keys.update ctx.keys id k;
  };

  let read ctx::ctx id::id key::k => {
    ctx.branch >>= fun branch' =>
      Store.read branch' path::[id, k] >>=
        fun data => switch data {
          | Some json => Lwt.return json;
          | None => Lwt.return json_empty;
        };  
  };

  let keys ctx::ctx id::id => {
    Keys.get ctx.keys id;
  };

};

module Text = {
  
    module Store = Ezirmin.FS_lww_register (Irmin.Contents.String);
  
    type t = Lwt.t (Store.branch);
  
    let text_empty = "";
  
    let create path_to_db::path => {
      Store.init root::(path ^ "Text_kv_store") bare::true () >>= Store.master;
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
        Store.init root::(path ^ "Binary_kv_store") bare::true () >>= Store.master;
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