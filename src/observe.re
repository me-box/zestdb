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

let update ctx l => {
  ctx.notify_list = l;
};

let unwrap ctx => {
  ctx.notify_list;
};


let has_observed options => {
  (Array.exists (fun (number,_) => number == 6) options) ? true : false;
};

let is_observed ctx path => {
  List.mem_assoc path ctx.notify_list;
};

let observed_paths_exist ctx => {
  List.length ctx.notify_list > 0;
};

let get_ident ctx path => {
  List.assoc path ctx.notify_list;
};

let add_to_observe ctx uri_path content_format ident max_age => {
  open Int32;
  open Logger;
  open Printf;
  let key = (uri_path, content_format);
  let expiry = (equal max_age (of_int 0)) ? max_age : add (time_now ()) max_age;
  let value = (ident, expiry);
  if (is_observed ctx key) {
    info_f "add_to_observe" (sprintf "adding ident:%s to existing path:%s with max-age:%lu" ident uri_path max_age) >>= fun () => {
      let items = get_ident ctx key;
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

let expire ctx t => {
  open List;
  let f x =>
    switch x {
    | (k,v) => (k, filter (fun (_,t') => (t' > t) || (t' == Int32.of_int 0)) v);
    };
  filter (fun (x,y) => y != []) (map f ctx.notify_list);
};

let diff l1 l2 => List.filter (fun x => not (List.mem x l2)) l1;

let list_uuids lis => {
  open List;  
  map (fun (x,y) => hd y) lis;    
};

