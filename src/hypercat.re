
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
    Ezjsonm.dict [("status", `Bool true)]
  } else {
    Ezjsonm.dict [("status", `Bool false)]
  };

let get_cat () => `O (Ezjsonm.get_dict !cat);