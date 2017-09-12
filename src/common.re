module Result = {
    type t = Ok | Error int;
};

module Ack = {
    type t = Code int |  Payload string;
};