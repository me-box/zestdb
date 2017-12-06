

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
        | _ => false;
        };
      });
  };
};

let from_json json => {
  open Ezjsonm;
  let s = to_string json; 
  let _ = Lwt_io.printf "filter =>>>>>>>>>>>>>>>>>>>>>>>> %s\n" s;
  get_list (fun x => x) (value json);
};

let to_json alist => {
  open Ezjsonm;
  list (fun x => x) alist;
};

let apply func json => from_json json |> func |> to_json;

let equals t v json => apply (String.equals t v) json;


