let init () => {
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
  
let to_hex msg => {
  open Hex;
  String.trim (of_string msg |> hexdump_s print_chars::false);
};