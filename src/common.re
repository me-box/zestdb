module Result = {
    type t = Ok | Error int;
};

module Ack = {
    type t = Code int |  Payload int string | Observe string string;
};

module Response = {
    type t = Json (Lwt.t Ezjsonm.t) | Text (Lwt.t string) | Binary (Lwt.t string);
};