open Common;

let cat = ref (Ezjsonm.from_channel (open_in "base-cat.json"));

let has_href_and_metadata item =>
  Ezjsonm.(mem item ["href"] && mem item ["item-metadata"]);

let is_rel_val_pair item => {
  open Ezjsonm;
  let metadata = find item ["item-metadata"];
  let list = get_list (fun x => x) metadata;
  List.for_all (fun x => mem x ["rel"] && mem x ["val"]) list
};

 let has_rel_term term item => {
  open Ezjsonm;
  let metadata = find item ["item-metadata"];
  let string_exists string term =>
    String.split_on_char ':' string |> 
        List.exists (fun x => x == term);
  get_list get_dict metadata |>
    List.map (fun x => List.hd x) |>
        List.map (fun (x,y) => y) |>
            List.map (fun x => get_string x) |>
                List.exists (fun x => string_exists x term)
}; 


let is_valid_item item =>
  has_href_and_metadata item &&
  is_rel_val_pair item &&
  has_rel_term "hasDescription" item && has_rel_term "isContentType" item;

let update_cat item =>
  if (is_valid_item item) {
    open Ezjsonm;
    let current_items = find !cat ["items"];
    let current_list = get_list (fun x => x) current_items;
    let new_list = List.cons item current_list;
    let new_items = list (fun x => x) new_list;
    cat := update !cat ["items"] (Some new_items);
    Result.Ok;
  } else {
    Result.Error 128;
  };

let create_rel_val rel_string val_string => {
    open Ezjsonm;
    let rel_json = ("rel", `String rel_string);
    let val_json = ("val", `String val_string);
    dict [rel_json, val_json];
};

  let update_item item rel_string val_string => {
    open Ezjsonm;
    let item_metadata = find item ["item-metadata"];
    let metadata_list = get_list (fun x => x) item_metadata;
    let rel_val = create_rel_val rel_string val_string;
    let new_metadata_list = List.append metadata_list [rel_val];
    let new_item_metadata = list (fun x => x) new_metadata_list;
    let new_item = update item ["item-metadata"] (Some new_item_metadata);
    update_cat new_item;
  };

let get_cat () => `O (Ezjsonm.get_dict !cat);