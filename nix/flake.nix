{
  inputs.std.url = "github:divnix/std?ref=main";
  outputs =
    { std
    , ...
    }
      @ inputs:
    std.grow {
      inherit inputs;
      cellsFrom = ./cells;
      systems = [
        {
          build = "x86_64-unknown-linux-gnu";
          host = "x86_64-unknown-linux-gnu";
        }
      ];
      organelles = [
      (inputs.std.runnables "entrypoints")
      (inputs.std.functions "library")
      ];
    };
}
