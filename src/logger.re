let init = () => {
  Lwt_log_core.default :=
  Lwt_log.channel(
    ~template="$(date).$(milliseconds) [$(level)] $(message)",
    ~close_mode=`Keep,
      ~channel=Lwt_io.stdout,
      ()
    );
    Lwt_log_core.add_rule("*", Lwt_log_core.Error);
    Lwt_log_core.add_rule("*", Lwt_log_core.Info);
    Lwt_log_core.add_rule("*", Lwt_log_core.Debug);
  };

  let to_hex = (msg) => Hex.(String.trim(of_string(msg) |> hexdump_s(~print_chars=false)));

  let info_f = (s1, s2) => Lwt_log_core.info_f("%s:%s", s1, s2);
  
  let debug_f = (s1, s2) => Lwt_log_core.debug_f("%s:%s", s1, s2);
  
  let error_f = (s1, s2) => Lwt_log_core.error_f("%s:%s", s1, s2);
