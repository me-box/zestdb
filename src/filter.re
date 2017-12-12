

/* let str_json_sample = "[{\"value\": 43}, {\"event\": \"bang\", \"value\": 44}]";
let json = Ezjsonm.from_string str_json_sample; */

let get_value term json => {
  open Ezjsonm;
  find json ["data"] |> fun x => find x [term] |> get_string;
};  

module String = {
  let equals s1 s2 => {
    open Ezjsonm;
    let match item => get_value s1 item == s2;
    List.filter (fun item => {
      try (match item) {
        | Not_found => false;
        };
      });
  };
  let contains s1 s2 => {
    let match item => {
      let re = Str.regexp_string s2;
      let str = get_value s1 item;
      try {
        ignore (Str.search_forward re str 0);
        true
      } {
      | Not_found => false
      };
    };
    List.filter (fun item => match item);
  };
};

let from_json json => {
  open Ezjsonm;
  get_list (fun x => x) (value json);
};

let to_json alist => {
  open Ezjsonm;
  list (fun x => x) alist;
};

let apply func json => from_json json |> func |> to_json;

let equals t v json => apply (String.equals t v) json;

let contains t v json => apply (String.contains t v) json;
