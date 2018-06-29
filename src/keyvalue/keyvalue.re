open Lwt.Infix;

module Json = {

  module Store = Ezirmin.FS_lww_register (Irmin.Contents.Json);

  type t = {
    branch: Lwt.t (Store.branch),    
    keys: Keys.t
  };

  let json_empty = Ezjsonm.dict [];

  let create path_to_db::path => {
    { 
      branch: Store.init root::(path ^ "Json_kv_store") bare::true () >>= Store.master,      
      keys: Keys.create file::(path ^ "Json_kv_keys_store")
    };
  };

  let write ctx::ctx info::info id::id key::k json::v => {
    ctx.branch >>= fun branch' =>
      Store.write message::info branch' path::[id, k] v >>=
        fun () => Keys.update ctx.keys info id k;
  };

  let read ctx::ctx info::info id::id key::k => {
    ctx.branch >>= fun branch' =>
      Store.read branch' path::[id, k] >>=
        fun data => switch data {
          | Some json => Lwt.return json;
          | None => Lwt.return json_empty;
        };  
  };

  let keys ctx::ctx info::info id::id => {
    Keys.get ctx.keys id;
  };

  let count_of_json result => {
    open Ezjsonm;
    dict [("count", int result)] |> Lwt.return;
  };

  let count ctx::ctx info::info id::id => {
    Keys.count ctx.keys id >>= count_of_json;
  };

  let delete ctx::ctx info::info id::id key::k => {
    write ctx::ctx info::info id::id key::k json::json_empty >>=
      fun () => Keys.delete ctx.keys info id k;
  };

  let delete_all ctx::ctx info::info id::id => {
    Keys.alist ctx.keys id >>= 
      fun lis => Lwt_list.iter_s (fun k => delete ctx::ctx info::info id::id key::k) lis;
  };

};

module Text = {
  
    module Store = Ezirmin.FS_lww_register (Irmin.Contents.String);
  
    type t = {
      branch: Lwt.t (Store.branch),    
      keys: Keys.t
    };
  
    let text_empty = "";
  
    let create path_to_db::path => {
      { 
        branch: Store.init root::(path ^ "Text_kv_store") bare::true () >>= Store.master,      
        keys: Keys.create file::(path ^ "Text_kv_keys_store")
      };
    };
  
    let write ctx::ctx info::info id::id key::k text::v => {
      ctx.branch >>= fun branch' =>
        Store.write message::info branch' path::[id, k] v >>=
          fun () => Keys.update ctx.keys info id k;
    };
  
    let read ctx::ctx info::info id::id key::k => {
      ctx.branch >>= fun branch' =>
        Store.read branch' path::[id, k] >>=
          fun data => switch data {
            | Some json => Lwt.return json;
            | None => Lwt.return text_empty;
          };  
    };

    let keys ctx::ctx info::info id::id => {
      Keys.get ctx.keys id;
    };

    let count_of_json result => {
      open Ezjsonm;
      dict [("count", int result)] |> Lwt.return;
    };

    let count ctx::ctx info::info id::id => {
      Keys.count ctx.keys id >>= count_of_json;
    };

    let delete ctx::ctx info::info id::id key::k => {
      write ctx::ctx info::info id::id key::k text::text_empty >>=
        fun () => Keys.delete ctx.keys info id k;
    };

    let delete_all ctx::ctx info::info id::id => {
      Keys.alist ctx.keys id >>= 
        fun lis => Lwt_list.iter_s (fun k => delete ctx::ctx info::info id::id key::k) lis;
    };
  
  };

  module Binary = {
    
      module Store = Ezirmin.FS_lww_register (Irmin.Contents.String);
    
      type t = {
        branch: Lwt.t (Store.branch),    
        keys: Keys.t
      };
    
      let binary_empty = "";
    
      let create path_to_db::path => {
        { 
          branch: Store.init root::(path ^ "Binary_kv_store") bare::true () >>= Store.master,      
          keys: Keys.create file::(path ^ "Binary_kv_key_store")
        };
      };
    
      let write ctx::ctx info::info id::id key::k binary::v => {
        ctx.branch >>= fun branch' =>
          Store.write message::info branch' path::[id, k] v >>=
            fun () => Keys.update ctx.keys info id k;
      };
    
      let read ctx::ctx info::info id::id key::k => {
        ctx.branch >>= fun branch' =>
          Store.read branch' path::[id, k] >>=
            fun data => switch data {
              | Some json => Lwt.return json;
              | None => Lwt.return binary_empty;
            };  
      };

      let keys ctx::ctx info::info id::id => {
        Keys.get ctx.keys id;
      };

      let count_of_json result => {
        open Ezjsonm;
        dict [("count", int result)] |> Lwt.return;
      };

      let count ctx::ctx info::info id::id => {
        Keys.count ctx.keys id >>= count_of_json;
      };

      let delete ctx::ctx info::info id::id key::k => {
        write ctx::ctx info::info id::id key::k binary::binary_empty >>=
          fun () => Keys.delete ctx.keys info id k;
      };

      let delete_all ctx::ctx info::info id::id => {
        Keys.alist ctx.keys id >>= 
          fun lis => Lwt_list.iter_s (fun k => delete ctx::ctx info::info id::id key::k) lis;
      };
    
    };