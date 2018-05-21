open Ezjsonm;
open Common;
open Lwt.Infix;


type t = { 
  cat: Ezjsonm.t,
  store: Keyvalue.Json.t
};

let create store::store => {
  { 
    cat: Ezjsonm.from_channel (open_in "base-cat.json"),
    store: store
  }
};


let ident x => x;

let get_href json => {
  find json ["href"] |> get_string;
};

let get_metadata json => {
  dict [("item-metadata", find json ["item-metadata"])] 
}; 

let has_href_and_metadata item => {
  mem item ["href"] && mem item ["item-metadata"];
};

let is_rel_val_pair item => {
  let metadata = find item ["item-metadata"];
  let l = get_list ident metadata;
  List.for_all (fun x => mem x ["rel"] && mem x ["val"]) l;
};

let string_exists string term =>
String.split_on_char ':' string |> 
  List.exists (fun x => x == term);

let has_rel_term term item => {
find item ["item-metadata"] |>
  get_list ident |>
    List.map (fun x => find x ["rel"]) |>
      List.exists (fun x => string_exists (get_string x) term);
}; 

let is_valid_item item =>
  has_href_and_metadata item &&
  is_rel_val_pair item &&
  has_rel_term "hasDescription" item && 
  has_rel_term "isContentType" item;
 
 
let update ctx::ctx info::info item::item => {
  if (is_valid_item item) {
    let k = get_href item;
    let v = get_metadata item;
    Keyvalue.Json.write ctx::ctx.store info::info id::"//cat" key::k json::v >>=
      fun () => Result.Ok |> Lwt.return;
  } else {
    Result.Error 128 |> Lwt.return;
  }
};

let make_item lis => {
  list (fun (k,v) => dict [("href", string k), ("item-metadata", (find (value v) ["item-metadata"]))]) lis;
};

let get ctx::ctx info::info => {
  open Keyvalue.Json;
  keys ctx::ctx.store info::info id::"//cat" >>=
    fun json => get_strings (value json) |>
      fun keys => Lwt_list.map_s (fun k => read ctx::ctx.store info::info id::"//cat" key::k) keys >>=
        fun values => make_item (List.combine keys values) |>
          fun new_items => Ezjsonm.update (value ctx.cat) ["items"] (Some (value new_items)) |>
            fun cat => `O (get_dict cat) |> Lwt.return;
};