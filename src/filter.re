/* let str_json_sample = "[{\"value\": 43}, {\"event\": \"bang\", \"value\": 44}]";
   let json = Ezjsonm.from_string str_json_sample; */
   let get_value = (term, json) =>
   Ezjsonm.(find(json, ["data"]) |> ((x) => find(x, [term]) |> get_string));
 
 module String = {
   let equals = (s1, s2) => {
     open Ezjsonm;
     let switch_ = (item) => get_value(s1, item) == s2;
     List.filter(
       (item) =>
         try (switch_(item)) {
         | Not_found => false
         }
     );
   };
   let contains = (s1, s2) => {
     let switch_ = (item) => {
       let re = Str.regexp_string(s2);
       let str = get_value(s1, item);
       try {
         ignore(Str.search_forward(re, str, 0));
         true;
       } {
       | Not_found => false
       };
     };
     List.filter((item) => switch_(item));
   };
 };
 
 let from_json = (json) => Ezjsonm.(get_list((x) => x, value(json)));
 
 let to_json = (alist) => Ezjsonm.(list((x) => x, alist));
 
 let apply = (func, json) => from_json(json) |> func |> to_json;
 
 let equals = (t, v, json) => apply(String.equals(t, v), json);
 
 let contains = (t, v, json) => apply(String.contains(t, v), json);