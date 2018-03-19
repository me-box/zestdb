open Lwt.Infix;

type t = {
  mutable notify_list: list ((string, int), list (string, Int32.t, string))
};

let time_now () => {
  Int32.of_float (Unix.time ());
};

let create () => {
  notify_list: []
};

let has_prefix s1 s2 => {
  open String;
  length s1 <= length s2 &&
    s1 == (sub s2 0 (length s1 - 1)) ^ "*";
};


let get_keys lis => {
  open List;  
  map (fun (k,v) => k) lis;    
};

let get_values lis => {
  open List;  
  map (fun (k,v) => v) lis |> flatten;
};

let is_observed ctx key => {
  List.mem_assoc key ctx.notify_list;
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
  let value = (ident, expiry, mode);
  if (is_observed ctx key) {
    info_f "observing" (sprintf "adding ident:%s to existing path:%s with max-age:%lu and mode:%s" ident uri_path max_age mode) >>= fun () => {
      let items = get ctx key;
      let new_items = List.cons value items;
      let filtered = List.filter (fun (key',_) => (key' != key)) ctx.notify_list;
      ctx.notify_list = (List.cons (key, new_items) filtered);
      Lwt.return_unit;
    };
  } else {
    info_f "observing" (sprintf "adding ident:%s to new path:%s with max-age:%lu and mode:%s" ident uri_path max_age mode) >>= fun () => {
      ctx.notify_list = (List.cons (key, [value]) ctx.notify_list);
      Lwt.return_unit;
    }; 
  };
};


let diff l1 l2 => List.filter (fun x => not (List.mem x l2)) l1;


let handle_expire lis t => {
  open List;
  let f x =>
    switch x {
    | (k,v) => (k, filter (fun (_,t',_) => (t' > t) || (t' == Int32.of_int 0)) v);
    };
  filter (fun (x,y) => y != []) (map f lis);
};


let expire ctx => {
  if (observed_paths_exist ctx.notify_list) {
    let remaining = handle_expire ctx.notify_list (time_now ());
    let expired_uuids = diff (get_values ctx.notify_list) (get_values remaining);
    ctx.notify_list = remaining;
    Lwt.return expired_uuids;
  } else {
    Lwt.return [];
  };
};

let get_all ctx => {
  get_values ctx.notify_list;
};