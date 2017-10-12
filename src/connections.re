open Lwt.Infix;

let setup_logger () => {
  Lwt_log_core.default :=
    Lwt_log.channel
      template::"$(date).$(milliseconds) [$(level)] $(message)"
      close_mode::`Keep
      channel::Lwt_io.stdout
      ();
  Lwt_log_core.add_rule "*" Lwt_log_core.Error;
  Lwt_log_core.add_rule "*" Lwt_log_core.Info;
  Lwt_log_core.add_rule "*" Lwt_log_core.Debug;
};

let get_peername fd => {
  open Unix;
  open Printf;
  switch (getpeername fd) {
  | ADDR_INET addr int => sprintf "%s:%d" (string_of_inet_addr addr) int;
  | ADDR_UNIX fname => fname;
  }
};

let handle_monitor socket => {
  let lwt_socket = Lwt_zmq.Socket.of_socket socket;
  let rec loop () => {
    Lwt_zmq.Monitor.recv lwt_socket >>= {
    open ZMQ.Monitor;    
    fun event =>
      switch event {
      | Accepted address fd => Lwt_log_core.info_f "%s" (string_of_event event);
      | Disconnected address fd => Lwt_log_core.info_f "%s" (string_of_event event);
      | _ => Lwt.return_unit;
      };
    } >>= fun () => loop ();
  };
  loop ();
};

let monitor ctx socket => {
  let soc = Lwt_zmq.Socket.to_socket socket;
  let mon_soc = ZMQ.Monitor.connect ctx (ZMQ.Monitor.create soc);  
  let () = Lwt.async (fun () => handle_monitor mon_soc);
};
