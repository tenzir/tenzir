import ExecutionEnvironment from "@docusaurus/ExecutionEnvironment";
import { registerCopyButton } from "@rehype-pretty/transformers";

export function onRouteDidUpdate() {
  if (ExecutionEnvironment.canUseDOM) {
    registerCopyButton();
  }
}

// NOTE: This is an ugly workaround for making the copy button work on page load.
// See more at: https://stackoverflow.com/a/74736980
if (ExecutionEnvironment.canUseDOM) {
  // We also need to regiserCopyButton when the page first loads otherwise,
  // after reloading the page, these triggers will not be set until the user
  // navigates somewhere.
  window.addEventListener("load", () => {
    setTimeout(registerCopyButton, 1);
  });
}
