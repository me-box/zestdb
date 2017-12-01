open Ezjsonm;
open Common;

let cat = ref (Ezjsonm.from_channel (open_in "base-cat.json"));

let ident x => x;

let has_href_and_metadata item =>
  mem item ["href"] && mem item ["item-metadata"];

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


let get_href json => {
  find json ["href"] |> get_string;
};  
let remove href alist => {
  let match href item => href != get_href item;
  List.filter (fun item => match href item) alist;  
};
  
let update_cat item =>
  if (is_valid_item item) {
    let current_items = find !cat ["items"];
    let href = get_href item;
    let current_list = get_list ident current_items;
    let filtered_list = remove href current_list;
    let new_list = List.cons item filtered_list;
    let new_items = list ident new_list;
    cat := update !cat ["items"] (Some new_items);
    Result.Ok;
  } else {
    Result.Error 128;
  };

let create_rel_val rel_string val_string => {
    let rel_json = ("rel", `String rel_string);
    let val_json = ("val", `String val_string);
    dict [rel_json, val_json];
};

  let update_item item rel_string val_string => {
    let item_metadata = find item ["item-metadata"];
    let metadata_list = get_list ident item_metadata;
    let rel_val = create_rel_val rel_string val_string;
    let new_metadata_list = List.append metadata_list [rel_val];
    let new_item_metadata = list ident new_metadata_list;
    let new_item = update item ["item-metadata"] (Some new_item_metadata);
    update_cat new_item;
  };

let get_cat () => `O (Ezjsonm.get_dict !cat);