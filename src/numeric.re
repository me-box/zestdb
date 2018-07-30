/* let str_json_sample = "[{\"value\": 43}, {\"event\": \"bang\", \"value\": 44}]";
   let json = Ezjsonm.from_string str_json_sample; */
   module Oml = {
    open Oml.Util.Array;
    open Oml.Statistics.Descriptive;
    let sum = sumf;
    let max = max;
    let min = min;
    let mean = mean;
    let sd = sd;
    let median = median;
  };
  
  let from_json = (json) =>
    Ezjsonm.(
      List.rev_map(
        (x) => find(x, ["data"]) |> ((y) => get_float(find(y, ["value"]))),
        get_list((x) => x, value(json))
      )
    );
  
  let to_json = (result) => Ezjsonm.(dict([("result", `Float(result))]));
  
  let apply = (func, json) => {
    let lis = from_json(json);
    lis == [] ? Ezjsonm.dict([]) : Array.of_list(lis) |> func |> to_json;
  };
  
  let sum = (json) => apply(Oml.sum, json);
  
  let min = (json) => apply(Oml.min, json);
  
  let max = (json) => apply(Oml.max, json);
  
  let mean = (json) => apply(Oml.mean, json);
  
  let median = (json) => apply(Oml.median, json);
  
  let sd = (json) => apply(Oml.sd, json);
  
  let count = (json) => from_json(json) |> List.length |> float_of_int |> to_json;