open Lwt.Infix;

type t = {
  mutable notify_list: list ((string, int), list (string, Int32.t))
};

let time_now () => {
  Int32.of_float (Unix.time ());
};

let create () => {
  notify_list: []
};

let is_observed ctx path => {
  List.mem_assoc path ctx.notify_list;
};

let observed_paths_exist lis => {
  List.length lis > 0;
};

let get ctx path => {
  List.assoc path ctx.notify_list;
};

let add ctx uri_path content_format ident max_age mode => {
  open Int32;
  open Logger;
  open Printf;
  let key = (uri_path, content_format);
  let expiry = (equal max_age (of_int 0)) ? max_age : add (time_now ()) max_age;
  let value = (ident, expiry);
  if (is_observed ctx key) {
    info_f "add_to_observe" (sprintf "adding ident:%s to existing path:%s with max-age:%lu" ident uri_path max_age) >>= fun () => {
      let items = get ctx key;
      let new_items = List.cons value items;
      let filtered = List.filter (fun (key',_) => (key' != key)) ctx.notify_list;
      ctx.notify_list = (List.cons (key, new_items) filtered);
      Lwt.return_unit;
    };
  } else {
    info_f "add_to_observe" (sprintf "adding ident:%s to new path:%s with max-age:%lu" ident uri_path max_age) >>= fun () => {
      ctx.notify_list = (List.cons (key, [value]) ctx.notify_list);
      Lwt.return_unit;
    }; 
  };
};


let diff l1 l2 => List.filter (fun x => not (List.mem x l2)) l1;

let list_uuids lis => {
  open List;  
  map (fun (x,y) => hd y) lis;    
};


let handle_expire lis t => {
  open List;
  let f x =>
    switch x {
    | (k,v) => (k, filter (fun (_,t') => (t' > t) || (t' == Int32.of_int 0)) v);
    };
  filter (fun (x,y) => y != []) (map f lis);
};


let expire ctx => {
  if (observed_paths_exist ctx.notify_list) {
    let remaining = handle_expire ctx.notify_list (time_now ());
    let expired_uuids = diff (list_uuids ctx.notify_list) (list_uuids remaining);
    ctx.notify_list = remaining;
    Lwt.return expired_uuids;
  } else {
    Lwt.return [];
  };
};

let get_all ctx => {
  list_uuids ctx.notify_list;
};